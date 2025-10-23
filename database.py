# database.py

import mariadb
import pandas as pd
import sys
import os
import glob
from datetime import datetime # <-- 1. IMPORTADO AQUI
from utils import carregar_config 

def conectar_db(db_config):
    """
    Tenta conectar ao banco de dados real MariaDB.
    """
    host = db_config.get("host")
    port = db_config.get("porta", 3306)
    db = db_config.get("database")
    user = db_config.get("usuario")
    pwd = db_config.get("senha")
    
    try:
        print(f"[Database] Conectando ao banco real MariaDB: {db}@{host}:{port}...")
        conexao = mariadb.connect(
            user=user,
            password=pwd,
            host=host,
            port=port,
            database=db,
            connect_timeout=10
        )
        print("[Database] Conexão real estabelecida.")
        return conexao
        
    except mariadb.Error as e:
        print(f"[Database] !!! FALHA AO CONECTAR AO BANCO REAL !!!")
        print(f"[Database] Erro: {e}")
        print("[Database] Verifique seu config.json e se o serviço MariaDB está rodando.")
        return None

def inserir_dados_produtos(conexao, caminho_arquivo_csv):
    """
    Lê o CSV, processa os códigos (interno, principal e adicionais),
    duplica os dados do produto para cada código de barras,
    e (re)cria a tabela 'bronze_plugpharma_produtos' completa.
    """
    if not conexao:
        print("[Database] Inserção falhou: conexão está nula.")
        return

    # --- Constantes da nova lógica ---
    TABELA_DB = "bronze_plugpharma_produtos"

    # Colunas base para a lógica de split
    COLUNAS_CSV_BASE = [
        "CODIGO INTERNO", 
        "CODIGO BARRAS PRINCIPAL", 
        "CODIGO BARRAS ADICIONAL"
    ]
    
    # Novas colunas de dados do produto (extra)
    COLUNAS_CSV_DADOS = [
        "DESCRIÇÃO", "APRESENTAÇÃO", "STATUS", "CODIGO FABRICANTE", "FABRICANTE", 
        "CNPJ FABRICANTE", "CODIGO TIPO PRODUTO", "TIPO PRODUTO", 
        "CODIGO GRUPO PRINCIPAL", "GRUPO PRINCIPAL", "NCM", "NCM DESCRIÇÃO", 
        "PREÇO CONTROLADO", "CODIGO MS", "PORTARIA", "FORMA APRESENTAÇÃO", 
        "CODIGO UNIDADE MEDIDA", "FRAÇÃO", "SUBSTANCIA NOME", "CONCENTRAÇÃO", 
        "FARMACOLOGICO", "DATA CADASTRO", "ULTIMA ALTERAÇÃO", "ASSOCIADO"
    ]
    
    # Nomes das colunas correspondentes no banco (para os dados)
    COLUNAS_DB_DADOS = [
        "descricao", "apresentacao", "status", "codigo_fabricante", "fabricante", 
        "cnpj_fabricante", "codigo_tipo_produto", "tipo_produto", 
        "codigo_grupo_principal", "grupo_principal", "ncm", "ncm_descricao", 
        "preco_controlado", "codigo_ms", "portaria", "forma_apresentacao", 
        "codigo_unidade_medida", "fracao", "substancia_nome", "concentracao", 
        "farmacologico", "data_cadastro", "ultima_alteracao", "associado"
    ]
    
    # Colunas base do DB
    COLUNAS_DB_BASE = [
        "codigo_interno",
        "codigo_barras",
        "codigo_barras_normalizado", 
        "codigo_principal"
    ]

    # Todas as colunas necessárias do CSV
    COLUNAS_CSV_NECESSARIAS = COLUNAS_CSV_BASE + COLUNAS_CSV_DADOS
    
    # --- 3. COLUNA ADICIONADA À LISTA ---
    # Todas as colunas do DB
    COLUNAS_DB_TODAS = COLUNAS_DB_BASE + COLUNAS_DB_DADOS + ["data_insercao"]
    
    # --- Fim das constantes ---

    # --- 2. PEGAR O TIMESTAMP ATUAL ---
    # Pega a data/hora atual UMA VEZ para todo o lote
    agora = datetime.now()

    try:
        # 1. Ler e Processar o CSV com Pandas
        print(f"[Database] Lendo arquivo: {caminho_arquivo_csv}")
        
        try:
            df = pd.read_csv(
                caminho_arquivo_csv, 
                sep=';', 
                encoding='utf-8-sig', 
                low_memory=False, 
                dtype=str # Ler tudo como string
            )
        except Exception as e:
            print(f"[Database] Erro ao ler o CSV (mesmo com utf-8-sig): {e}")
            return

        # Verificar se todas as colunas necessárias existem
        colunas_faltando = [col for col in COLUNAS_CSV_NECESSARIAS if col not in df.columns]
        if colunas_faltando:
            print(f"[Database] Erro: Colunas obrigatórias não encontradas no CSV: {colunas_faltando}")
            print(f"[Database] Colunas encontradas: {list(df.columns)}")
            return
        
        # Filtrar apenas pelas colunas necessárias e preencher NaNs
        df = df[COLUNAS_CSV_NECESSARIAS].fillna('')
        print(f"[Database] CSV lido com sucesso. {len(df)} linhas encontradas. Iniciando processamento...")

        # --- CORREÇÃO DE DATAS (Existente) ---
        print("[Database] Corrigindo formatos de data (DATA CADASTRO, ULTIMA ALTERAÇÃO)...")
        try:
            formato_data_br = '%d/%m/%Y %H:%M:%S'
            df['DATA CADASTRO'] = pd.to_datetime(df['DATA CADASTRO'], 
                                                 format=formato_data_br, 
                                                 errors='coerce')
            df['DATA CADASTRO'] = df['DATA CADASTRO'].astype(object).where(pd.notnull(df['DATA CADASTRO']), None)
            df['ULTIMA ALTERAÇÃO'] = pd.to_datetime(df['ULTIMA ALTERAÇÃO'], 
                                                    format=formato_data_br, 
                                                    errors='coerce')
            df['ULTIMA ALTERAÇÃO'] = df['ULTIMA ALTERAÇÃO'].astype(object).where(pd.notnull(df['ULTIMA ALTERAÇÃO']), None)
            print("[Database] Formatos de data corrigidos.")
        except KeyError as e:
            print(f"[Database] ERRO: Não foi possível encontrar a coluna de data {e}. Verifique as COLUNAS_CSV_DADOS.")
            raise e
        except Exception as e:
            print(f"[Database] ERRO ao corrigir formato de data: {e}")
            raise e
        # --- FIM DA CORREÇÃO DE DATAS ---

        # 2. Preparar dados para inserção
        data_to_insert = []

        for _, row in df.iterrows():
            cod_interno_raw = row["CODIGO INTERNO"].strip()
            cod_principal_raw = row["CODIGO BARRAS PRINCIPAL"].strip()
            cod_adicional_raw = row["CODIGO BARRAS ADICIONAL"].strip()

            if cod_interno_raw:
                cod_interno_trunc = cod_interno_raw[:14]
                dados_produto = tuple(row[col].strip() if isinstance(row[col], str) else row[col] for col in COLUNAS_CSV_DADOS)
                
                # 1. Adicionar o código de barras principal (Flag = 1)
                if cod_principal_raw:
                    cod_principal_norm = cod_principal_raw.zfill(14)[:14]
                    
                    # --- 4. DADO ADICIONADO À TUPLA ---
                    tupla_inserir = (cod_interno_trunc, cod_principal_raw, cod_principal_norm, 1) + dados_produto + (agora,)
                    data_to_insert.append(tupla_inserir)

                # 2. Processar e adicionar os códigos de barras adicionais (Flag = 0)
                if cod_adicional_raw:
                    codigos_adicionais_lista = cod_adicional_raw.split('+')
                    
                    for cod_ad in codigos_adicionais_lista:
                        cod_ad_limpo = cod_ad.strip()
                        if cod_ad_limpo:
                            cod_ad_norm = cod_ad_limpo.zfill(14)[:14]
                            
                            # --- 4. DADO ADICIONADO À TUPLA ---
                            tupla_inserir = (cod_interno_trunc, cod_ad_limpo, cod_ad_norm, 0) + dados_produto + (agora,)
                            data_to_insert.append(tupla_inserir)

        if not data_to_insert:
            print("[Database] Nenhum dado válido para inserir após processamento.")
            return
        
        # 3. Preparar o Banco
        cursor = conexao.cursor()

        print(f"[Database] Recriando tabela (DROP/CREATE) '{TABELA_DB}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_DB}")
        
        # --- 5. COLUNA ADICIONADA AO SQL ---
        create_query = f"""
        CREATE TABLE {TABELA_DB} (
            codigo_interno VARCHAR(14),
            codigo_barras VARCHAR(14),
            codigo_barras_normalizado VARCHAR(14),
            codigo_principal TINYINT,
            
            descricao VARCHAR(255), 
            apresentacao VARCHAR(255), 
            status VARCHAR(20), 
            codigo_fabricante INT(11), 
            fabricante TEXT, 
            cnpj_fabricante VARCHAR(20), 
            codigo_tipo_produto INT(11), 
            tipo_produto VARCHAR(255), 
            codigo_grupo_principal INT(11), 
            grupo_principal VARCHAR(255), 
            ncm VARCHAR(20), 
            ncm_descricao TEXT, 
            preco_controlado VARCHAR(10), 
            codigo_ms VARCHAR(255), 
            portaria TEXT, 
            forma_apresentacao TEXT, 
            codigo_unidade_medida INT(11), 
            fracao INT(11), 
            substancia_nome TEXT, 
            concentracao TEXT, 
            farmacologico TEXT, 
            data_cadastro DATETIME, 
            ultima_alteracao DATETIME, 
            associado VARCHAR(255),

            data_insercao DATETIME 
        )
        """
        cursor.execute(create_query)
        print("[Database] Tabela recriada com sucesso.")

        # 4. Inserir no Banco
        print(f"[Database] Preparando para inserir {len(data_to_insert)} registros...")

        # (Esta parte funciona automaticamente por ser dinâmica)
        colunas_sql = ", ".join(COLUNAS_DB_TODAS)
        placeholders_sql = ", ".join(["?"] * len(COLUNAS_DB_TODAS))
        query = f"INSERT INTO {TABELA_DB} ({colunas_sql}) VALUES ({placeholders_sql})"
        
        cursor.executemany(query, data_to_insert)
        
        conexao.commit()
        print(f"[Database] Sucesso! {len(data_to_insert)} registros inseridos.")

    except pd.errors.EmptyDataError:
        print(f"[Database] Erro: O arquivo CSV baixado está vazio: {caminho_arquivo_csv}")
    except mariadb.Error as e:
        print(f"[Database] Erro de banco de dados (DROP/CREATE/INSERT): {e}")
        try:
            conexao.rollback()
            print("[Database] Rollback realizado.")
        except mariadb.Error as rb_e:
            print(f"[Database] Erro durante o rollback: {rb_e}")
    except Exception as e:
        print(f"[Database] Erro inesperado durante o processamento de dados: {e}")
        if 'conexao' in locals():
            try:
                conexao.rollback()
                print("[Database] Rollback realizado.")
            except mariadb.Error as rb_e:
                print(f"[Database] Erro durante o rollback: {rb_e}")
    finally:
        if 'cursor' in locals():
            cursor.close()

# --- Função 'processar_csv_para_db' (Sem alterações) ---
def processar_csv_para_db():
    """
    Função principal "chamável" que executa todo o processo do banco de dados.
    Encontra o CSV mais recente e o insere no banco.
    """
    print("--- Executando 'database.py' (processar_csv_para_db) ---")
    
    config = carregar_config()
    db_cfg = config.get("dbSults")
    if not db_cfg:
        print("[DB] Erro: Configuração 'dbSults' não encontrada no config.json")
        raise Exception("Configuração 'dbSults' não encontrada no config.json")

    pasta_downloads = os.path.join(os.getcwd(), "downloads")
    if not os.path.exists(pasta_downloads):
        print(f"[DB] Erro: Pasta de downloads não encontrada em '{pasta_downloads}'")
        raise Exception(f"Pasta de downloads não encontrada em '{pasta_downloads}'")
        
    arquivos_csv = glob.glob(os.path.join(pasta_downloads, "*.csv"))
    if not arquivos_csv:
        print(f"[DB] Erro: Nenhum arquivo .csv encontrado na pasta '{pasta_downloads}'")
        raise Exception(f"Nenhum arquivo .csv encontrado na pasta '{pasta_downloads}'")
        
    arquivo_recente = max(arquivos_csv, key=os.path.getctime)
    print(f"[DB] Encontrado arquivo mais recente: {os.path.basename(arquivo_recente)}")

    conexao = None
    try:
        conexao = conectar_db(db_cfg)
        if conexao:
            print("[DB] Conexão bem-sucedida. Iniciando inserção...")
            # Chama a função principal que agora inclui a data_insercao
            inserir_dados_produtos(conexao, arquivo_recente) 
            print("[DB] Processo de inserção finalizado.")
        else:
            print("[DB] Conexão com o banco falhou. Processo abortado.")
            raise Exception("Conexão com o banco de dados falhou.")
            
    except Exception as e:
        print(f"[DB] Ocorreu um erro inesperado: {e}")
        raise e 
    finally:
        if conexao:
            conexao.close()
            print("[DB] Conexão fechada.")

# --- Bloco de Execução Independente (Sem alterações) ---
if __name__ == "__main__":
    print("--- Executando 'database.py' em modo de teste direto ---")
    try:
        processar_csv_para_db()
        print("[Teste DB] Teste finalizado com sucesso.")
    except Exception as e:
        print(f"[Teste DB] Teste falhou: {e}")
        sys.exit(1)