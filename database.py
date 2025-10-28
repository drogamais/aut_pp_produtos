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
    """
    Usa uma tabela staging para carregar os dados EM LOTES. Se sucesso,
    substitui a tabela principal pela staging atomicamente.
    """
    if not conexao:
        print("[Database] Inserção falhou: conexão está nula.")
        return

    TABELA_PRINCIPAL = "bronze_plugpharma_produtos"
    TABELA_STAGING = "bronze_plugpharma_produtos_staging"
    CHUNK_SIZE = 50000 # <-- Define o tamanho do lote (ajuste se necessário)

    # --- Constantes (sem alterações) ---
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
        "descricao", "apresentacao", "status", "codigo_fabricante", "fabricante",
        "cnpj_fabricante", "codigo_tipo_produto", "tipo_produto",
        "codigo_grupo_principal", "grupo_principal", "ncm", "ncm_descricao",
        "preco_controlado", "codigo_ms", "portaria", "forma_apresentacao",
        "codigo_unidade_medida", "fracao", "substancia_nome", "concentracao",
        "farmacologico", "data_cadastro", "ultima_alteracao", "associado"
    ]
    COLUNAS_DB_BASE = ["codigo_interno", "codigo_barras", "codigo_barras_normalizado", "codigo_principal"]
    COLUNAS_CSV_NECESSARIAS = COLUNAS_CSV_BASE + COLUNAS_CSV_DADOS
    COLUNAS_DB_TODAS = COLUNAS_DB_BASE + COLUNAS_DB_DADOS + ["data_insercao"]
    # --- Fim Constantes ---

    agora = datetime.now()
    cursor = None

    try:
        # 1. Ler e Processar o CSV (sem alterações significativas, apenas melhor fallback de encoding)
        print(f"[Database] Lendo arquivo: {caminho_arquivo_csv}")
        try:
            df = pd.read_csv(caminho_arquivo_csv, sep=';', encoding='utf-8-sig', low_memory=False, dtype=str)
        except UnicodeDecodeError:
            print("[Database] Falha ao ler com utf-8-sig. Tentando 'latin-1'...")
            try:
                df = pd.read_csv(caminho_arquivo_csv, sep=';', encoding='latin-1', low_memory=False, dtype=str)
                print("[Database] Leitura com 'latin-1' bem-sucedida.")
            except Exception as e_latin:
                print(f"[Database] Erro ao ler o CSV com 'latin-1': {e_latin}")
                print("[Database] Verifique o encoding do arquivo CSV.")
                return
        except Exception as e:
            print(f"[Database] Erro inesperado ao ler o CSV: {e}")
            return

        colunas_faltando = [col for col in COLUNAS_CSV_NECESSARIAS if col not in df.columns]
        if colunas_faltando:
            print(f"[Database] Erro: Colunas obrigatórias não encontradas no CSV: {colunas_faltando}")
            return

        df = df[COLUNAS_CSV_NECESSARIAS].fillna('')
        print(f"[Database] CSV lido. {len(df)} linhas. Processando...")

        # Correção de datas (sem alterações)
        print("[Database] Corrigindo formatos de data...")
        try:
             formato_data_br = '%d/%m/%Y %H:%M:%S'
             df['DATA CADASTRO'] = pd.to_datetime(df['DATA CADASTRO'], format=formato_data_br, errors='coerce')
             df['DATA CADASTRO'] = df['DATA CADASTRO'].astype(object).where(pd.notnull(df['DATA CADASTRO']), None)
             df['ULTIMA ALTERAÇÃO'] = pd.to_datetime(df['ULTIMA ALTERAÇÃO'], format=formato_data_br, errors='coerce')
             df['ULTIMA ALTERAÇÃO'] = df['ULTIMA ALTERAÇÃO'].astype(object).where(pd.notnull(df['ULTIMA ALTERAÇÃO']), None)
             print("[Database] Formatos de data corrigidos.")
        except Exception as e:
             print(f"[Database] AVISO ao corrigir formato de data: {e}")
             if 'DATA CADASTRO' in df.columns: df['DATA CADASTRO'] = None
             if 'ULTIMA ALTERAÇÃO' in df.columns: df['ULTIMA ALTERAÇÃO'] = None
             print("[Database] Colunas de data problemáticas foram definidas como NULL.")

        # Preparar dados para inserção (sem alterações)
        data_to_insert = []
        # ... (loop for _, row in df.iterrows(): ... igual ao código anterior) ...
        for _, row in df.iterrows():
            cod_interno_raw = row["CODIGO INTERNO"].strip()
            cod_principal_raw = row["CODIGO BARRAS PRINCIPAL"].strip()
            cod_adicional_raw = row["CODIGO BARRAS ADICIONAL"].strip()

            if cod_interno_raw:
                cod_interno_trunc = cod_interno_raw[:14]
                # Converte explicitamente tipos que podem ser numéricos mas definimos como VARCHAR no DB
                dados_produto = list(row[col].strip() if isinstance(row[col], str) else row[col] for col in COLUNAS_CSV_DADOS)
                # Exemplo: Se 'codigo_fabricante' pode vir como número, converte para string
                try:
                    # Encontra os índices das colunas que podem ser problemáticas
                    idx_cod_fab = COLUNAS_CSV_DADOS.index("CODIGO FABRICANTE")
                    idx_cod_tipo = COLUNAS_CSV_DADOS.index("CODIGO TIPO PRODUTO")
                    idx_cod_grupo = COLUNAS_CSV_DADOS.index("CODIGO GRUPO PRINCIPAL")
                    idx_cod_unid = COLUNAS_CSV_DADOS.index("CODIGO UNIDADE MEDIDA")
                    idx_fracao = COLUNAS_CSV_DADOS.index("FRAÇÃO")

                    if dados_produto[idx_cod_fab] is not None: dados_produto[idx_cod_fab] = str(dados_produto[idx_cod_fab])
                    if dados_produto[idx_cod_tipo] is not None: dados_produto[idx_cod_tipo] = str(dados_produto[idx_cod_tipo])
                    if dados_produto[idx_cod_grupo] is not None: dados_produto[idx_cod_grupo] = str(dados_produto[idx_cod_grupo])
                    if dados_produto[idx_cod_unid] is not None: dados_produto[idx_cod_unid] = str(dados_produto[idx_cod_unid])
                    if dados_produto[idx_fracao] is not None: dados_produto[idx_fracao] = str(dados_produto[idx_fracao])
                except ValueError:
                    # Se não encontrar o índice, ignora (coluna pode ter sido removida das constantes)
                    pass
                except Exception as e_conv:
                    print(f"[Database] Aviso: Erro ao converter dado para string na linha com CODIGO INTERNO {cod_interno_raw}: {e_conv}")

                dados_produto = tuple(dados_produto) # Converte de volta para tupla

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

        # 2. Preparar o Banco (Staging - sem alterações na estrutura)
        cursor = conexao.cursor()
        print(f"[Database] Criando/Limpando tabela staging '{TABELA_STAGING}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {TABELA_STAGING}")

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
            ultima_alteracao DATETIME, associado TEXT,
            data_insercao DATETIME,
            INDEX idx_cod_barras_norm (codigo_barras_normalizado),
            INDEX idx_cod_interno (codigo_interno)
        ) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        """
        create_query_staging = create_query_base.format(table_name=TABELA_STAGING)
        cursor.execute(create_query_staging)
        print("[Database] Tabela staging criada com sucesso.")

        print(f"[Database] Preparando para inserir {len(data_to_insert)} registros em lotes de {CHUNK_SIZE}...")
        colunas_sql = ", ".join(COLUNAS_DB_TODAS)
        placeholders_sql = ", ".join(["?"] * len(COLUNAS_DB_TODAS))
        query = f"INSERT INTO {TABELA_STAGING} ({colunas_sql}) VALUES ({placeholders_sql})"

        total_inserido = 0
        num_lotes = (len(data_to_insert) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(0, len(data_to_insert), CHUNK_SIZE):
            batch = data_to_insert[i:i + CHUNK_SIZE]
            lote_num = (i // CHUNK_SIZE) + 1
            print(f"[Database] Inserindo lote {lote_num}/{num_lotes} ({len(batch)} registros) na staging...")
            cursor.executemany(query, batch)
            total_inserido += len(batch) # ou cursor.rowcount se executemany retornar corretamente
            print(f"[Database] Lote {lote_num} inserido. Total parcial: {total_inserido}")

        # Verifica se o total inserido bate com o esperado
        if total_inserido != len(data_to_insert):
             # Isso não deveria acontecer com executemany, mas é uma checagem
             raise Exception(f"Erro na contagem de inserção! Esperado: {len(data_to_insert)}, Inserido: {total_inserido}")

        print(f"[Database] Inserção em lotes na staging concluída ({total_inserido} registros).")
        

        print("[Database] Iniciando transação para substituir a tabela principal...")
        sql_drop_principal = f"DROP TABLE IF EXISTS {TABELA_PRINCIPAL};"
        sql_rename_staging = f"RENAME TABLE {TABELA_STAGING} TO {TABELA_PRINCIPAL};"

        cursor.execute(sql_drop_principal)
        print(f"[Database] Tabela principal antiga ('{TABELA_PRINCIPAL}') removida (se existia).")
        cursor.execute(sql_rename_staging)
        print(f"[Database] Tabela staging renomeada para principal ('{TABELA_PRINCIPAL}').")
        

        # Commita a transação (inserção em lotes + renames)
        conexao.commit()
        print("[Database] Transação concluída e commit realizado com sucesso!")

    # --- Blocos except e finally (sem alterações significativas) ---
    except pd.errors.EmptyDataError:
        print(f"[Database] Erro: O arquivo CSV está vazio: {caminho_arquivo_csv}")
    except mariadb.Error as e:
        print(f"[Database] !!! ERRO DE BANCO DE DADOS: {e} !!!")
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
    except Exception as e:
        print(f"[Database] !!! ERRO INESPERADO: {e} !!!")
        # Imprime traceback para depuração
        import traceback
        traceback.print_exc()
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