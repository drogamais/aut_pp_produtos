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
            connect_timeout=10 # Aumentar timeout de conexão inicial, se necessário
            # Considere aumentar read_timeout e write_timeout se a rede for lenta,
            # mas o problema principal costuma ser o max_allowed_packet
            # read_timeout=60,
            # write_timeout=60
        )
        print("[Database] Conexão real estabelecida.")
        # Opcional: Verificar o max_allowed_packet da conexão atual
        # try:
        #     cursor_check = conexao.cursor()
        #     cursor_check.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
        #     result = cursor_check.fetchone()
        #     print(f"[Database] max_allowed_packet da sessão atual: {result}")
        #     cursor_check.close()
        # except mariadb.Error as e:
        #     print(f"[Database] Aviso: Não foi possível verificar max_allowed_packet ({e})")
        return conexao

    except mariadb.Error as e:
        print(f"[Database] !!! FALHA AO CONECTAR AO BANCO REAL !!!")
        print(f"[Database] Erro: {e}")
        print("[Database] Verifique seu config.json e se o serviço MariaDB está rodando.")
        return None

def inserir_dados_produtos(conexao, caminho_arquivo_csv):
    if not conexao:
        print("[Database] Inserção falhou: conexão está nula.")
        return

    TABELA_PRINCIPAL = "bronze_plugpharma_produtos"
    TABELA_STAGING = "bronze_plugpharma_produtos_staging"
    CHUNK_SIZE = 10000 

    # --- Constantes ---
    COLUNAS_CSV_BASE = ["CODIGO INTERNO", "CODIGO BARRAS PRINCIPAL", "CODIGO BARRAS ADICIONAL"]
    COLUNAS_CSV_DADOS = [
        "DESCRIÇÃO", "APRESENTAÇÃO", "STATUS", "CODIGO FABRICANTE", "FABRICANTE",
        "CNPJ FABRICANTE", "CODIGO TIPO PRODUTO", "TIPO PRODUTO",
        "CODIGO GRUPO PRINCIPAL", "GRUPO PRINCIPAL", "NCM", "NCM DESCRIÇÃO",
        "PREÇO CONTROLADO", "CODIGO MS", "PORTARIA", "FORMA APRESENTAÇÃO",
        "CODIGO UNIDADE MEDIDA", "FRAÇÃO", "SUBSTANCIA NOME", "CONCENTRAÇÃO",
        "FARMACOLOGICO", "DATA CADASTRO", "ULTIMA ALTERAÇÃO", "ASSOCIADO"
    ]
    COLUNAS_DB_DADOS = [
        "descricao", "apresentacao", "produto", "status", "codigo_fabricante", "fabricante",
        "cnpj_fabricante", "codigo_tipo_produto", "tipo_produto",
        "codigo_grupo_principal", "grupo_principal", "ncm", "ncm_descricao",
        "preco_controlado", "codigo_ms", "portaria", "forma_apresentacao",
        "codigo_unidade_medida", "fracao", "substancia_nome", "concentracao",
        "farmacologico", "data_cadastro", "ultima_alteracao", "associado"
    ]
    COLUNAS_DB_BASE = ["codigo_interno", "codigo_barras", "codigo_barras_normalizado", "codigo_principal"]
    COLUNAS_CSV_NECESSARIAS = COLUNAS_CSV_BASE + COLUNAS_CSV_DADOS
    COLUNAS_DB_TODAS = COLUNAS_DB_BASE + COLUNAS_DB_DADOS + ["data_insercao"]

    agora = datetime.now()
    cursor = None

    try:
        print(f"[Database] Lendo arquivo: {caminho_arquivo_csv}")
        try:
            df = pd.read_csv(caminho_arquivo_csv, sep=';', encoding='utf-8-sig', low_memory=False, dtype=str)
        except:
            df = pd.read_csv(caminho_arquivo_csv, sep=';', encoding='latin-1', low_memory=False, dtype=str)

        df = df[COLUNAS_CSV_NECESSARIAS].fillna('')
        
        # Correção de datas
        formato_data_br = '%d/%m/%Y %H:%M:%S'
        df['DATA CADASTRO'] = pd.to_datetime(df['DATA CADASTRO'], format=formato_data_br, errors='coerce')
        df['ULTIMA ALTERAÇÃO'] = pd.to_datetime(df['ULTIMA ALTERAÇÃO'], format=formato_data_br, errors='coerce')

        data_to_insert = []
        for _, row in df.iterrows():
            cod_interno_raw = row["CODIGO INTERNO"].strip()
            cod_principal_raw = row["CODIGO BARRAS PRINCIPAL"].strip()
            cod_adicional_raw = row["CODIGO BARRAS ADICIONAL"].strip()

            if not cod_interno_raw: continue

            cod_interno_trunc = cod_interno_raw[:14]
            descricao = (row["DESCRIÇÃO"] or "").strip()
            apresentacao = (row["APRESENTAÇÃO"] or "").strip()

            dados_base_csv = [
                row[col].strip() if isinstance(row[col], str) else row[col]
                for col in COLUNAS_CSV_DADOS
            ]

            # --- PROCESSA CÓDIGO PRINCIPAL ---
            if cod_principal_raw:
                cod_norm = cod_principal_raw.zfill(14)[:14]
                # AJUSTE: Usando cod_principal_raw em vez de cod_norm
                prod_concat = f"{cod_principal_raw} - {descricao} {apresentacao}"
                
                lista_val = dados_base_csv.copy()
                lista_val.insert(2, prod_concat) 

                tupla = (cod_interno_trunc, cod_principal_raw, cod_norm, 1) + tuple(lista_val) + (agora,)
                data_to_insert.append(tupla)

            # --- PROCESSA CÓDIGOS ADICIONAIS ---
            if cod_adicional_raw:
                for cod_ad in cod_adicional_raw.split("+"):
                    cod_ad_limpo = cod_ad.strip()
                    if not cod_ad_limpo: continue
                    
                    cod_norm_ad = cod_ad_limpo.zfill(14)[:14]
                    # AJUSTE: Usando cod_ad_limpo em vez de cod_norm_ad
                    prod_concat_ad = f"{cod_ad_limpo} - {descricao} {apresentacao}"
                    
                    lista_val_ad = dados_base_csv.copy()
                    lista_val_ad.insert(2, prod_concat_ad)

                    tupla_ad = (cod_interno_trunc, cod_ad_limpo, cod_norm_ad, 0) + tuple(lista_val_ad) + (agora,)
                    data_to_insert.append(tupla_ad)

        if not data_to_insert:
            print("[Database] Nenhum dado para inserir.")
            return

        cursor = conexao.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_STAGING}")

        create_query = f"""
        CREATE TABLE {TABELA_STAGING} (
            codigo_interno VARCHAR(14), codigo_barras VARCHAR(14),
            codigo_barras_normalizado VARCHAR(14), codigo_principal TINYINT,
            descricao VARCHAR(255), apresentacao VARCHAR(255), produto VARCHAR(255), status VARCHAR(20),
            codigo_fabricante VARCHAR(50), fabricante TEXT, cnpj_fabricante VARCHAR(20),
            codigo_tipo_produto VARCHAR(50), tipo_produto VARCHAR(255),
            codigo_grupo_principal VARCHAR(50), grupo_principal VARCHAR(255),
            ncm VARCHAR(20), ncm_descricao TEXT, preco_controlado VARCHAR(10),
            codigo_ms VARCHAR(255), portaria TEXT, forma_apresentacao TEXT,
            codigo_unidade_medida VARCHAR(50), fracao VARCHAR(50), substancia_nome TEXT,
            concentracao TEXT, farmacologico TEXT, data_cadastro DATETIME,
            ultima_alteracao DATETIME, associado TEXT,
            data_insercao DATETIME,
            INDEX idx_produto (produto),
            INDEX idx_cod_barras_norm (codigo_barras_normalizado),
            INDEX idx_cod_interno (codigo_interno)
        ) CHARSET=utf8mb4;
        """
        cursor.execute(create_query)

        colunas_sql = ", ".join(COLUNAS_DB_TODAS)
        placeholders = ", ".join(["?"] * 30)
        query = f"INSERT INTO {TABELA_STAGING} ({colunas_sql}) VALUES ({placeholders})"

        for i in range(0, len(data_to_insert), CHUNK_SIZE):
            batch = data_to_insert[i:i + CHUNK_SIZE]
            cursor.executemany(query, batch)
            print(f"[Database] Inserido lote {(i//CHUNK_SIZE)+1}")

        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_PRINCIPAL}")
        cursor.execute(f"RENAME TABLE {TABELA_STAGING} TO {TABELA_PRINCIPAL}")
        conexao.commit()
        print("[Database] Sucesso total!")

    except Exception as e:
        print(f"[Database] Erro: {e}")
        if conexao: conexao.rollback()
    finally:
        if cursor: cursor.close()


# --- Função 'processar_csv_para_db' (sem alterações) ---
def processar_csv_para_db(caminho_arquivo_csv_a_processar):
    print(f"--- Executando 'database.py' (processar_csv_para_db) para o arquivo: {os.path.basename(caminho_arquivo_csv_a_processar)} ---")
    config = carregar_config()
    db_cfg = config.get("dbDrogamais")
    if not db_cfg:
        print("[DB] Erro: Configuração 'dbDrogamais' não encontrada no config.json")
        raise Exception("Configuração 'dbDrogamais' não encontrada no config.json")

    conexao = None
    try:
        conexao = conectar_db(db_cfg)
        if conexao:
            print("[DB] Conexão bem-sucedida. Iniciando inserção...")
            inserir_dados_produtos(conexao, caminho_arquivo_csv_a_processar)
            print("[DB] Processo de inserção finalizado.")
        else:
            print("[DB] Conexão com o banco falhou. Processo abortado.")
            raise Exception("Conexão com o banco de dados falhou.")
    except Exception as e:
        print(f"[DB] Ocorreu um erro inesperado na função principal: {e}")
        # Re-levanta a exceção para que o run.py possa capturá-la e sair com erro
        raise e
    finally:
        if conexao:
            conexao.close()
            print("[DB] Conexão fechada.")


# --- Bloco de Execução Independente (ajustado para teste) ---
if __name__ == "__main__":
    print("--- Executando 'database.py' em modo de teste direto ---")
    # Para testar, coloque um arquivo CSV válido na pasta 'downloads' com o nome esperado
    pasta_downloads_teste = os.path.join(os.getcwd(), "downloads")
    hoje_str_teste = datetime.now().strftime('%Y-%m-%d')
    arquivo_teste = os.path.join(pasta_downloads_teste, f"{hoje_str_teste}_produtos.csv")

    if os.path.exists(arquivo_teste):
        try:
            processar_csv_para_db(arquivo_teste)
            print("[Teste DB] Teste finalizado com sucesso.")
        except Exception as e:
            print(f"[Teste DB] Teste falhou: {e}")
            sys.exit(1)
    else:
        print(f"[Teste DB] Arquivo de teste não encontrado: {arquivo_teste}")
        print("[Teste DB] Crie ou coloque um arquivo CSV válido neste caminho para testar.")
        sys.exit(1)