# run.py

from utils import carregar_config
from aut_pp_produtos import PlugPharmaAutomator
from database import conectar_db, inserir_dados_produtos
# import pandas as pd  # <-- NÃO PRECISA MAIS DO PANDAS AQUI

def main():
    """
    Função principal que orquestra todo o processo:
    1. Carrega config
    2. Executa automação Web para BAIXAR o CSV
    3. Chama a função de banco para LER e INSERIR os dados
    """
    print("[Sistema] --- Iniciando processo completo ---")
    
    # 1. Carregar configuração
    config = carregar_config()
    login_cfg = config.get("login")
    db_cfg = config.get("dbSults")

    # 2. Etapa Web: Extração de dados (Download)
    print("\n[Sistema] Etapa 1: Iniciando extração de dados (Download CSV)...")
    automator = PlugPharmaAutomator(login_cfg)
    # A variável agora recebe o CAMINHO do arquivo ou None
    caminho_arquivo_csv = automator.executar_extracao()

    # 3. Etapa Banco de Dados:
    if caminho_arquivo_csv:
        print(f"\n[Sistema] Etapa 2: Processamento do Banco de Dados...")
        
        # Conecta no banco
        conexao = conectar_db(db_cfg)
        
        if conexao:
            # Passa a conexão e o CAMINHO DO ARQUIVO para a função
            inserir_dados_produtos(conexao, caminho_arquivo_csv)
            
            # Fecha a conexão real
            conexao.close() 
            print("[Database] Conexão real fechada.")
        else:
            print("[Sistema] Erro: Não foi possível conectar ao banco.")
    else:
        print("\n[Sistema] Erro: Nenhum arquivo foi baixado (download falhou).")
        print("[Sistema] Processo do banco de dados não será executado.")

    print("\n[Sistema] --- Processo finalizado ---")

if __name__ == "__main__":
    main()