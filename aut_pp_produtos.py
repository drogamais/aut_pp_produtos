import time
import os
import glob
import argparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from utils import carregar_config 

class PlugPharmaAutomator:
    
    # --- 3. ALTERADO ---
    # Adicionado 'dev_mode=False'
    def __init__(self, login_config, dev_mode=False): 
        self.config = login_config
        
        # Define o caminho absoluto para a pasta 'downloads' dentro do projeto
        self.pasta_downloads = os.path.join(os.getcwd(), "downloads")
        
        # Cria a pasta 'downloads' se ela não existir
        if not os.path.exists(self.pasta_downloads):
            os.makedirs(self.pasta_downloads)
        print(f"[Sistema] Pasta de downloads definida: {self.pasta_downloads}")

        options = webdriver.ChromeOptions()
        prefs = {"download.default_directory" : self.pasta_downloads}
        options.add_experimental_option("prefs", prefs)
        
        # --- LÓGICA DO MODO DEV ---
        if dev_mode:
            print("[Sistema] MODO DEV ATIVADO. Navegador ficará visível.")
            # Não faz nada, usa as opções padrões (navegador visível)
        else:
            print("[Sistema] MODO PADRÃO (HEADLESS). Navegador ficará oculto.")
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
        # --- FIM DA LÓGICA ---
            
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=self.service, options=options)
        
        # Espera padrão (curta) para elementos da UI
        self.wait = WebDriverWait(self.driver, 20) 
        # Espera longa para processamento de servidor e downloads
        self.wait_long = WebDriverWait(self.driver, 3900) # 65 minutos
        
        if dev_mode:
            self.driver.maximize_window() # Maximiza APENAS em modo dev
        # --- FIM DA ALTERAÇÃO 3 ---

    def _limpar_pasta_downloads(self):
        """Apaga arquivos .csv e .crdownload antigos da pasta."""
        print(f"[Sistema] Limpando pasta de downloads...")
        try:
            files_csv = glob.glob(os.path.join(self.pasta_downloads, "*.csv"))
            files_cr = glob.glob(os.path.join(self.pasta_downloads, "*.crdownload"))
            
            for f in files_csv + files_cr:
                os.remove(f)
            print("[Sistema] Pasta limpa.")
        except OSError as e:
            print(f"[Sistema] Erro ao limpar pasta (arquivo pode estar em uso): {e}")
            # Se não conseguir limpar, a automação não é segura
            raise Exception(f"Não foi possível limpar a pasta de downloads: {e}")

    def _esperar_e_enviar(self, by_locator, valor):
        elemento = self.wait.until(EC.presence_of_element_located(by_locator))
        elemento.send_keys(valor)

    def _esperar_e_clicar(self, by_locator):
        elemento = self.wait.until(EC.element_to_be_clickable(by_locator))
        elemento.click()

    def fazer_login(self):
        url_login = self.config.get('url_login')
        print(f"[Web] Acessando URL de login: {url_login}")
        self.driver.get(url_login)
        print("[Web] Preenchendo 'Base'...")
        self._esperar_e_enviar((By.ID, "base"), self.config.get("base"))
        print("[Web] Preenchendo 'Usuário'...")
        self._esperar_e_enviar((By.ID, "username"), self.config.get("username"))
        print("[Web] Preenchendo 'Senha'...")
        self._esperar_e_enviar((By.ID, "password"), self.config.get("password"))
        print("[Web] Clicando 'Conectar'...")
        self._esperar_e_clicar((By.XPATH, "//button[contains(text(), 'Conectar')]"))
        print("[Web] Login realizado, aguardando carregamento da página principal...")
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[title='Menu']")))
        print("[Web] Página principal carregada.")

    def navegar_para_produtos(self):
        url_produtos = self.config.get('url_produtos')
        print(f"[Web] Navegando diretamente para a URL de produtos: {url_produtos}")
        self.driver.get(url_produtos)
        print("[Web] Aguardando página de produtos carregar...")
        locator_pagina_produtos = (By.XPATH, "//button[contains(., 'Exportar') and .//i[contains(@class, 'icon-lx-file-csv')]]")
        self.wait.until(EC.presence_of_element_located(locator_pagina_produtos))
        print("[Web] Página de produtos carregada.")

    def coletar_dados_produtos(self):
        """
        Executa a sequência de cliques para exportar o CSV.
        ANTES de clicar, limpa a pasta de downloads.
        """
        try:
            print("[Web] Procurando botão 'Exportar' (CSV)...")
            locator_btn1 = (By.XPATH, "//button[contains(., 'Exportar') and .//i[contains(@class, 'icon-lx-file-csv')]]")
            self._esperar_e_clicar(locator_btn1)

            print("[Web] Botão CSV clicado. Aguardando modal de confirmação...")
            locator_btn2 = (By.XPATH, "//button[contains(@class, 'btn-primary') and .//span[normalize-space(.)='Exportar']]")
            
            # --- PONTO CRÍTICO ---
            # Limpa a pasta ANTES do clique final que inicia o processo
            self._limpar_pasta_downloads()
            
            # Clica no botão final
            self._esperar_e_clicar(locator_btn2)

            print("[Web] Botão de confirmação clicado. Aguardando snackbar...")
            locator_msg = (By.XPATH, "//simple-snack-bar//span[contains(text(), 'Processando Arquivo de Produtos, Aguarde!')]")
            self.wait.until(EC.presence_of_element_located(locator_msg))
            
            print("[Web] Sucesso! Mensagem 'Processando Arquivo' detectada.")
            return True
        except Exception as e:
            print(f"[Web] Erro ao tentar clicar para exportar o arquivo: {e}")
            return False

    def _esperar_processamento_servidor(self):
        """
        Espera a barra de progresso (mat-progress-bar) aparecer e depois desaparecer.
        """
        try:
            locator_barra = (By.TAG_NAME, "mat-progress-bar")
            print("[Web] Aguardando barra de progresso (mat-progress-bar) aparecer...")
            # Espera até 10s para a barra aparecer
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(locator_barra))
            print("[Web] Barra apareceu. Agora aguardando desaparecer (processamento do servidor)...")
            
            # Usa a espera longa (até 65 min) para a barra DESAPARECER
            self.wait_long.until(EC.invisibility_of_element_located(locator_barra))
            print("[Web] Barra desapareceu. Servidor terminou de processar.")
            return True
        except TimeoutException:
            # Se a barra nunca apareceu (processamento instantâneo)
            print("[Web] Aviso: Barra de progresso não foi detectada (processamento rápido ou falha).")
            # Assume que foi rápido e continua, mas monitora o download
            return True
        except Exception as e:
            print(f"[Web] Erro ao esperar barra de progresso: {e}")
            return False

    def _monitorar_download_concluido(self, timeout_segundos=3900): # 65 minutos
        """
        Espera o download ser concluído monitorando a pasta.
        O download é considerado completo quando um arquivo .csv existe
        e nenhum arquivo .crdownload está presente.
        """
        print(f"[Sistema] Monitorando pasta '{self.pasta_downloads}' por até {timeout_segundos}s...")
        
        start_time = time.time()

        while True:
            # 1. Verifica o tempo total
            if time.time() - start_time > timeout_segundos:
                raise Exception(f"Timeout de {timeout_segundos}s atingido. Download não concluído.")

            # 2. Procura os arquivos
            arquivos_cr = glob.glob(os.path.join(self.pasta_downloads, "*.crdownload"))
            arquivos_csv = glob.glob(os.path.join(self.pasta_downloads, "*.csv"))

            # 3. Verifica a condição de conclusão
            # (Temos CSV) E (NÃO temos .crdownload) = Sucesso!
            if arquivos_csv and not arquivos_cr:
                print("\n[Sistema] Download concluído!")
                # Encontra o arquivo .csv mais recente
                arquivo_recente = max(arquivos_csv, key=os.path.getctime)
                print(f"[Sistema] Arquivo baixado: {arquivo_recente}")
                return arquivo_recente

            # 4. Se não terminou, informa o status e espera
            if arquivos_cr:
                # Se temos .crdownload, o download está em andamento.
                print(f"[Sistema] ...download em andamento ({os.path.basename(arquivos_cr[0])})...", end="\r")
            else:
                # Se não temos CSV nem CR, o download ainda não começou.
                print("[Sistema] ...aguardando início do download...", end="\r")

            time.sleep(2) # Espera 2 segundos antes de verificar novamente

    def fechar_navegador(self):
        print("[Web] Fechando o navegador.")
        self.driver.quit()

    def executar_extracao(self):
        """
        Orquestra a automação, monitora o download E RENOMEIA o arquivo.
        Retorna o CAMINHO do arquivo RENOMEADO, ou None se falhar.
        """
        caminho_arquivo_original = None
        caminho_arquivo_renomeado = None
        try:
            self.fazer_login()
            self.navegar_para_produtos()

            sucesso_cliques = self.coletar_dados_produtos()

            if sucesso_cliques:
                if self._esperar_processamento_servidor():
                    caminho_arquivo_original = self._monitorar_download_concluido()

                    if caminho_arquivo_original:
                        try:
                            hoje_str = datetime.now().strftime('%Y-%m-%d')
                            # --- ALTERAÇÃO AQUI ---
                            novo_nome = f"{hoje_str}_produtos.csv" # Formato alterado
                            # --- FIM DA ALTERAÇÃO ---
                            caminho_arquivo_renomeado = os.path.join(self.pasta_downloads, novo_nome)

                            if os.path.exists(caminho_arquivo_renomeado):
                                print(f"[Sistema] Removendo arquivo antigo com nome final: {novo_nome}")
                                os.remove(caminho_arquivo_renomeado)

                            print(f"[Sistema] Renomeando '{os.path.basename(caminho_arquivo_original)}' para '{novo_nome}'...")
                            os.rename(caminho_arquivo_original, caminho_arquivo_renomeado)
                            print("[Sistema] Arquivo renomeado com sucesso.")
                            return caminho_arquivo_renomeado # Retorna o novo caminho
                        except OSError as e:
                            print(f"[Sistema] !!! ERRO AO RENOMEAR O ARQUIVO: {e} !!!")
                            print(f"[Sistema] O arquivo original pode estar em: {caminho_arquivo_original}")
                            return caminho_arquivo_original

            return None

        except Exception as e:
            print(f"\n[Web] Ocorreu um erro fatal durante a automação: {e}")
            if caminho_arquivo_original and not caminho_arquivo_renomeado:
                 print(f"[Web] O arquivo pode ter sido baixado em: {caminho_arquivo_original}")
            return None

        finally:
            self.fechar_navegador()

# --- Bloco de Execução Independente (Modo de Teste) ---

# --- 2. ALTERADO ---
if __name__ == "__main__":
    # Configura o leitor de argumentos da linha de comando
    parser = argparse.ArgumentParser(description="Automação PlugPharma para extração de produtos.")
    parser.add_argument(
        "--dev", 
        action="store_true", 
        help="Executa o robô em modo visível (não-headless) para depuração."
    )
    args = parser.parse_args()

    print("--- Executando 'aut_pp_produtos.py' ---")
    config = carregar_config()
    login_cfg = config.get("login")
    
    if login_cfg:
        # Passa o argumento 'dev_mode' para a classe
        # args.dev será True se --dev for usado, ou False caso contrário
        automator = PlugPharmaAutomator(login_cfg, dev_mode=args.dev) 
        caminho_arquivo_final = automator.executar_extracao()
        
        if caminho_arquivo_final:
            print(f"\n[Teste] Download CONCLUÍDO com sucesso.")
            print(f"[Teste] Arquivo está em: {caminho_arquivo_final}")
        else:
            print("\n[Teste] A extração (download) falhou.")
    else:
        print("[Teste] Erro: Seção 'login' não encontrada no config.json")
# --- FIM DA ALTERAÇÃO 2 ---