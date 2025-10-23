import argparse
import sys
from aut_pp_produtos import PlugPharmaAutomator # Importa a classe do outro arquivo
from utils import carregar_config

# --- 1. IMPORTAÇÃO CORRIGIDA ---
# Importa a nova função 'processar_csv_para_db' que criamos
try:
    from database import processar_csv_para_db as executar_script_database
except ImportError:
    print("[run.py] ERRO: Não foi possível importar 'processar_csv_para_db' do 'database.py'.")
    print("[run.py] Verifique se 'database.py' existe e tem a função 'processar_csv_para_db()'.")
    sys.exit(1)
# --- FIM DA CORREÇÃO ---


def executar_automacao_produtos(dev_mode=False):
    """
    Executa a automação de extração de produtos.
    """
    print("--- Iniciando 'aut_pp_produtos' ---")
    
    config = carregar_config()
    login_cfg = config.get("login")
    
    if not login_cfg:
        print("[run.py] Erro: Seção 'login' não encontrada no config.json")
        return False

    try:
        # Passa o 'dev_mode' recebido para a classe
        automator = PlugPharmaAutomator(login_cfg, dev_mode=dev_mode) 
        
        caminho_arquivo_final = automator.executar_extracao()
        
        if caminho_arquivo_final:
            print(f"\n[run.py] Sucesso: 'aut_pp_produtos' concluído.")
            print(f"[run.py] Arquivo salvo em: {caminho_arquivo_final}")
            return True # Retorna Sucesso
        else:
            print("\n[run.py] Falha: 'aut_pp_produtos' não concluiu a extração.")
            return False # Retorna Falha
            
    except Exception as e:
        print(f"\n[run.py] Ocorreu um erro fatal em 'aut_pp_produtos': {e}")
        return False

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    
    # 1. Configura o leitor de argumentos
    parser = argparse.ArgumentParser(description="Orquestrador de automações.")
    parser.add_argument(
        "--dev", 
        action="store_true", 
        help="Executa os robôs em modo visível (não-headless) para depuração."
    )
    args = parser.parse_args()

    print("--- Iniciando 'run.py' ---")
    
    # 2. Chama a automação de download
    sucesso_download = executar_automacao_produtos(dev_mode=args.dev)

    # --- 3. VERIFICA O SUCESSO E CHAMA O PRÓXIMO PASSO ---
    if sucesso_download:
        print("\n[run.py] Download concluído. Iniciando 'database.py'...")
        
        try:
            # Chama a função 'processar_csv_para_db' do 'database.py'
            executar_script_database()
            
            print("\n[run.py] 'database.py' concluído com sucesso.")
            print("[run.py] Orquestração finalizada com sucesso.")
            sys.exit(0) # Termina com código 0 (sucesso total)
            
        except Exception as e:
            print(f"\n[run.py] ERRO FATAL durante a execução do 'database.py': {e}")
            sys.exit(1) # Termina com código 1 (erro na etapa do banco)

    else:
        # Se o download falhou, nem tenta rodar o banco
        print("\n[run.py] Orquestração finalizada com falhas (Download falhou).")
        sys.exit(1) # Termina com código 1 (erro na etapa de download)