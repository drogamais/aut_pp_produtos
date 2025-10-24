# run.py

import argparse
import sys
import os # <-- IMPORTADO
from datetime import datetime # <-- IMPORTADO
from aut_pp_produtos import PlugPharmaAutomator
from utils import carregar_config

try:
    # A função agora espera o caminho do arquivo
    from database import processar_csv_para_db as executar_script_database
except ImportError:
    print("[run.py] ERRO: Não foi possível importar 'processar_csv_para_db' do 'database.py'.")
    print("[run.py] Verifique se 'database.py' existe e tem a função 'processar_csv_para_db(caminho_do_arquivo)'.")
    sys.exit(1)


def executar_automacao_produtos(dev_mode=False):
    """
    Executa a automação de extração de produtos. Retorna o caminho do arquivo renomeado.
    """
    print("--- Iniciando 'aut_pp_produtos' ---")
    config = carregar_config()
    login_cfg = config.get("login")
    if not login_cfg:
        print("[run.py] Erro: Seção 'login' não encontrada no config.json")
        return None # Retorna None em caso de erro

    caminho_arquivo_final = None # Inicializa
    try:
        automator = PlugPharmaAutomator(login_cfg, dev_mode=dev_mode)
        caminho_arquivo_final = automator.executar_extracao() # Deve retornar o caminho renomeado

        if caminho_arquivo_final:
            print(f"\n[run.py] Sucesso: 'aut_pp_produtos' concluído.")
            print(f"[run.py] Arquivo salvo e renomeado em: {caminho_arquivo_final}")
            return caminho_arquivo_final # Retorna o caminho
        else:
            print("\n[run.py] Falha: 'aut_pp_produtos' não concluiu a extração ou renomeação.")
            return None # Retorna None em caso de falha

    except Exception as e:
        print(f"\n[run.py] Ocorreu um erro fatal em 'aut_pp_produtos': {e}")
        return None


# --- Bloco Principal de Execução ---
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Orquestrador de automações.")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Executa os robôs em modo visível (não-headless) para depuração."
    )
    args = parser.parse_args()

    print("--- Iniciando 'run.py' ---")

    # --- LÓGICA DE VERIFICAÇÃO DO ARQUIVO ---
    pasta_downloads = os.path.join(os.getcwd(), "downloads")
    hoje_str = datetime.now().strftime('%Y-%m-%d')
    nome_arquivo_esperado = f"{hoje_str}_produtos.csv" # Formato alterado
    caminho_arquivo_hoje = os.path.join(pasta_downloads, nome_arquivo_esperado)

    caminho_arquivo_usar = None # Variável para guardar o caminho a ser usado no DB
    sucesso_etapa_anterior = False # Flag para saber se podemos ir para o DB

    print(f"[run.py] Verificando se o arquivo de hoje já existe: {nome_arquivo_esperado}")
    if os.path.exists(caminho_arquivo_hoje):
        print(f"[run.py] Arquivo encontrado! Pulando etapa de download.")
        caminho_arquivo_usar = caminho_arquivo_hoje
        sucesso_etapa_anterior = True # Consideramos sucesso, pois temos o arquivo
    else:
        print("[run.py] Arquivo de hoje não encontrado. Iniciando processo de download...")
        caminho_arquivo_usar = executar_automacao_produtos(dev_mode=args.dev)
        if caminho_arquivo_usar:
            # Verifica se o arquivo renomeado tem o nome esperado (segurança extra)
            if os.path.basename(caminho_arquivo_usar) == nome_arquivo_esperado:
                sucesso_etapa_anterior = True
            else:
                print(f"[run.py] ERRO: O arquivo baixado foi renomeado para '{os.path.basename(caminho_arquivo_usar)}' em vez de '{nome_arquivo_esperado}'.")
                sucesso_etapa_anterior = False
                caminho_arquivo_usar = None # Não usar o arquivo com nome errado
        else:
            sucesso_etapa_anterior = False


    # --- CHAMADA CONDICIONAL PARA O DATABASE ---
    if sucesso_etapa_anterior and caminho_arquivo_usar:
        print(f"\n[run.py] Etapa anterior concluída. Iniciando 'database.py' com o arquivo: {os.path.basename(caminho_arquivo_usar)}...")
        try:
            executar_script_database(caminho_arquivo_usar)
            print("\n[run.py] 'database.py' concluído com sucesso.")
            print("[run.py] Orquestração finalizada com sucesso.")
            sys.exit(0)

        except Exception as e:
            print(f"\n[run.py] ERRO FATAL durante a execução do 'database.py': {e}")
            print("[run.py] Verifique o log e o arquivo CSV.")
            sys.exit(1)

    else:
        print("\n[run.py] Orquestração finalizada com falhas (Arquivo CSV não está disponível ou download/renomeação falhou).")
        sys.exit(1)