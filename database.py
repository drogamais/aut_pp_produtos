# database.py

import mariadb
import pandas as pd
import sys
import os
from datetime import datetime
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
    Usa uma tabela staging para carregar os dados. Se sucesso,
    substitui a tabela principal pela staging atomicamente.
    """
    if not conexao:
        print("[Database] Inserção falhou: conexão está nula.")
        return

    TABELA_PRINCIPAL = "bronze_plugpharma_produtos"
    TABELA_STAGING = "bronze_plugpharma_produtos_staging"

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
    COLUNAS_DB_TODAS = COLUNAS_DB_BASE + COLUNAS_DB_DADOS + ["data_insercao"]

    agora = datetime.now()
    cursor = None

    try:
        # 1. Ler e Processar o CSV (igual antes)
        print(f"[Database] Lendo arquivo: {caminho_arquivo_csv}")
        try:
            df = pd.read_csv(
                caminho_arquivo_csv, sep=';', encoding='utf-8-sig',
                low_memory=False, dtype=str
            )
        except Exception as e:
             print(f"[Database] Erro ao ler o CSV (mesmo com utf-8-sig): {e}")
             # Tenta ler com latin-1 como fallback
             try:
                 print("[Database] Tentando ler com encoding 'latin-1'...")
                 df = pd.read_csv(
                     caminho_arquivo_csv, sep=';', encoding='latin-1',
                     low_memory=False, dtype=str
                 )
                 print("[Database] Leitura com 'latin-1' bem-sucedida.")
             except Exception as e_latin:
                 print(f"[Database] Erro ao ler o CSV com 'latin-1': {e_latin}")
                 print("[Database] Verifique o encoding do arquivo CSV.")
                 return # Aborta se não conseguir ler


        colunas_faltando = [col for col in COLUNAS_CSV_NECESSARIAS if col not in df.columns]
        if colunas_faltando:
            print(f"[Database] Erro: Colunas obrigatórias não encontradas no CSV: {colunas_faltando}")
            return

        df = df[COLUNAS_CSV_NECESSARIAS].fillna('')
        print(f"[Database] CSV lido. {len(df)} linhas. Processando...")

        # Correção de datas (igual antes)
        print("[Database] Corrigindo formatos de data...")
        try:
             formato_data_br = '%d/%m/%Y %H:%M:%S'
             df['DATA CADASTRO'] = pd.to_datetime(df['DATA CADASTRO'], format=formato_data_br, errors='coerce')
             df['DATA CADASTRO'] = df['DATA CADASTRO'].astype(object).where(pd.notnull(df['DATA CADASTRO']), None)
             df['ULTIMA ALTERAÇÃO'] = pd.to_datetime(df['ULTIMA ALTERAÇÃO'], format=formato_data_br, errors='coerce')
             df['ULTIMA ALTERAÇÃO'] = df['ULTIMA ALTERAÇÃO'].astype(object).where(pd.notnull(df['ULTIMA ALTERAÇÃO']), None)
             print("[Database] Formatos de data corrigidos.")
        except Exception as e:
             print(f"[Database] ERRO ao corrigir formato de data: {e}")
             # Considera continuar mesmo com erro de data, inserindo NULL
             if 'DATA CADASTRO' in df.columns: df['DATA CADASTRO'] = None
             if 'ULTIMA ALTERAÇÃO' in df.columns: df['ULTIMA ALTERAÇÃO'] = None
             print("[Database] Colunas de data problemáticas foram definidas como NULL.")

        # Preparar dados para inserção (igual antes)
        data_to_insert = []
        for _, row in df.iterrows():
            cod_interno_raw = row["CODIGO INTERNO"].strip()
            cod_principal_raw = row["CODIGO BARRAS PRINCIPAL"].strip()
            cod_adicional_raw = row["CODIGO BARRAS ADICIONAL"].strip()

            if cod_interno_raw:
                cod_interno_trunc = cod_interno_raw[:14]
                dados_produto = tuple(row[col].strip() if isinstance(row[col], str) else row[col] for col in COLUNAS_CSV_DADOS)

                if cod_principal_raw:
                    cod_principal_norm = cod_principal_raw.zfill(14)[:14]
                    tupla_inserir = (cod_interno_trunc, cod_principal_raw, cod_principal_norm, 1) + dados_produto + (agora,)
                    data_to_insert.append(tupla_inserir)

                if cod_adicional_raw:
                    codigos_adicionais_lista = cod_adicional_raw.split('+')
                    for cod_ad in codigos_adicionais_lista:
                        cod_ad_limpo = cod_ad.strip()
                        if cod_ad_limpo:
                            cod_ad_norm = cod_ad_limpo.zfill(14)[:14]
                            tupla_inserir = (cod_interno_trunc, cod_ad_limpo, cod_ad_norm, 0) + dados_produto + (agora,)
                            data_to_insert.append(tupla_inserir)

        if not data_to_insert:
            print("[Database] Nenhum dado válido para inserir após processamento.")
            return

        # 2. Preparar o Banco (Staging)
        cursor = conexao.cursor()

        print(f"[Database] Criando/Limpando tabela staging '{TABELA_STAGING}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_STAGING}") # Garante que está limpa

        # Cria a tabela staging com a mesma estrutura da principal
        # (Reutiliza a query de criação, apenas muda o nome da tabela)
        create_query_base = f"""
        CREATE TABLE {{table_name}} (
            codigo_interno VARCHAR(14), codigo_barras VARCHAR(14),
            codigo_barras_normalizado VARCHAR(14), codigo_principal TINYINT,
            descricao VARCHAR(255), apresentacao VARCHAR(255), status VARCHAR(20),
            codigo_fabricante VARCHAR(50), fabricante TEXT, cnpj_fabricante VARCHAR(20),
            codigo_tipo_produto VARCHAR(50), tipo_produto VARCHAR(255),
            codigo_grupo_principal VARCHAR(50), grupo_principal VARCHAR(255),
            ncm VARCHAR(20), ncm_descricao TEXT, preco_controlado VARCHAR(10),
            codigo_ms VARCHAR(255), portaria TEXT, forma_apresentacao TEXT,
            codigo_unidade_medida VARCHAR(50), fracao VARCHAR(50), substancia_nome TEXT,
            concentracao TEXT, farmacologico TEXT, data_cadastro DATETIME,
            ultima_alteracao DATETIME, associado VARCHAR(255),
            data_insercao DATETIME,
            INDEX idx_cod_barras_norm (codigo_barras_normalizado), -- Adiciona índice
            INDEX idx_cod_interno (codigo_interno) -- Adiciona índice
        ) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci; -- Define charset/collation
        """
        create_query_staging = create_query_base.format(table_name=TABELA_STAGING)

        cursor.execute(create_query_staging)
        print("[Database] Tabela staging criada com sucesso.")

        # 3. Inserir na Tabela Staging
        print(f"[Database] Preparando para inserir {len(data_to_insert)} registros na tabela staging...")
        colunas_sql = ", ".join(COLUNAS_DB_TODAS)
        placeholders_sql = ", ".join(["?"] * len(COLUNAS_DB_TODAS))
        query = f"INSERT INTO {TABELA_STAGING} ({colunas_sql}) VALUES ({placeholders_sql})"

        cursor.executemany(query, data_to_insert)
        # NÃO FAZ COMMIT AINDA AQUI - A INSERÇÃO FAZ PARTE DA TRANSAÇÃO MAIOR

        print(f"[Database] Inserção na staging concluída ({len(data_to_insert)} registros).")

        # 4. Substituir a Tabela Principal (Transação Atômica)
        print("[Database] Iniciando transação para substituir a tabela principal...")
        # conexao.begin() # mariadb-connector usa autocommit=False por padrão, begin() não é estritamente necessário se não desabilitou

        # Nome temporário para a tabela antiga, caso precise reverter manualmente
        TABELA_ANTIGA_BACKUP = f"{TABELA_PRINCIPAL}_old_{agora.strftime('%Y%m%d%H%M%S')}"

        # Renomeia a antiga (se existir) e a staging
        sql_rename_old = f"RENAME TABLE IF EXISTS {TABELA_PRINCIPAL} TO {TABELA_ANTIGA_BACKUP};"
        sql_rename_new = f"RENAME TABLE {TABELA_STAGING} TO {TABELA_PRINCIPAL};"
        sql_drop_backup = f"DROP TABLE IF EXISTS {TABELA_ANTIGA_BACKUP};" # Limpa o backup antigo

        cursor.execute(sql_drop_backup) # Remove backup de execuções anteriores
        print(f"[Database] Backup antigo ({TABELA_ANTIGA_BACKUP}) removido (se existia).")
        cursor.execute(sql_rename_old)
        print(f"[Database] Tabela principal antiga renomeada para backup ({TABELA_ANTIGA_BACKUP}) (se existia).")
        cursor.execute(sql_rename_new)
        print(f"[Database] Tabela staging renomeada para principal ('{TABELA_PRINCIPAL}').")

        # Se tudo deu certo até aqui, commita a transação (inserção + renames)
        conexao.commit()
        print("[Database] Transação concluída e commit realizado com sucesso!")

        # Opcional: Limpar a tabela de backup antiga após sucesso
        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_ANTIGA_BACKUP}")
        print(f"[Database] Tabela de backup ({TABELA_ANTIGA_BACKUP}) removida.")
        conexao.commit()

    except pd.errors.EmptyDataError:
        print(f"[Database] Erro: O arquivo CSV está vazio: {caminho_arquivo_csv}")
    except mariadb.Error as e:
        print(f"[Database] !!! ERRO DE BANCO DE DADOS: {e} !!!")
        try:
            print("[Database] Tentando reverter (rollback)...")
            conexao.rollback()
            print("[Database] Rollback realizado.")
            # Tenta limpar a staging se ela foi criada
            if cursor:
                 print(f"[Database] Tentando limpar tabela staging '{TABELA_STAGING}'...")
                 cursor.execute(f"DROP TABLE IF EXISTS {TABELA_STAGING}")
                 conexao.commit() # Commit do drop da staging
                 print("[Database] Tabela staging removida.")
        except mariadb.Error as rb_e:
            print(f"[Database] Erro durante o rollback ou limpeza: {rb_e}")
    except Exception as e:
        print(f"[Database] !!! ERRO INESPERADO: {e} !!!")
        if conexao:
            try:
                print("[Database] Tentando reverter (rollback)...")
                conexao.rollback()
                print("[Database] Rollback realizado.")
                if cursor:
                     print(f"[Database] Tentando limpar tabela staging '{TABELA_STAGING}'...")
                     cursor.execute(f"DROP TABLE IF EXISTS {TABELA_STAGING}")
                     conexao.commit()
                     print("[Database] Tabela staging removida.")
            except mariadb.Error as rb_e:
                print(f"[Database] Erro durante o rollback ou limpeza: {rb_e}")
    finally:
        if cursor:
            cursor.close()

# --- Função 'processar_csv_para_db' (Sem alterações) ---
def processar_csv_para_db(caminho_arquivo_csv_a_processar):
    """
    Função principal "chamável" que executa o processo do banco de dados
    usando um arquivo CSV específico fornecido.
    """
    print(f"--- Executando 'database.py' (processar_csv_para_db) para o arquivo: {os.path.basename(caminho_arquivo_csv_a_processar)} ---")

    config = carregar_config()
    db_cfg = config.get("dbDrogamais") # Nome da chave no config.json
    if not db_cfg:
        print("[DB] Erro: Configuração 'dbDrogamais' não encontrada no config.json")
        raise Exception("Configuração 'dbDrogamais' não encontrada no config.json")

    # --- LÓGICA DE BUSCA REMOVIDA ---
    # A função agora confia que o 'caminho_arquivo_csv_a_processar' é válido.

    conexao = None
    try:
        conexao = conectar_db(db_cfg)
        if conexao:
            print("[DB] Conexão bem-sucedida. Iniciando inserção...")
            # Chama a função de inserção com o caminho recebido
            inserir_dados_produtos(conexao, caminho_arquivo_csv_a_processar)
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