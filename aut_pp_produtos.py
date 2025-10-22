# aut_pp_produtos.py

import time
import os
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from utils import carregar_config 

class PlugPharmaAutomator:
    
    def __init__(self, login_config):
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
        
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=self.service, options=options)
        
        # Espera padrão (curta) para elementos da UI
        self.wait = WebDriverWait(self.driver, 20) 
        # Espera longa para processamento de servidor e downloads
        self.wait_long = WebDriverWait(self.driver, 300) # 5 minutos
        
        self.driver.maximize_window()

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
            
            # Usa a espera longa (até 5 min) para a barra DESAPARECER
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

    def _monitorar_download_concluido(self, timeout_segundos=300):
        """
        Espera um .crdownload aparecer e depois desaparecer.
        Retorna o caminho completo do arquivo .csv baixado.
        """
        print(f"[Sistema] Monitorando pasta '{self.pasta_downloads}'...")
        
        # 1. Esperar o arquivo .crdownload aparecer (download iniciar)
        print("[Sistema] ...esperando download iniciar (procurando .crdownload)...")
        timeout_inicio = 30 # 30 segundos para o download sequer começar
        start_time_inicio = time.time()
        arquivo_cr = None
        
        while not arquivo_cr:
            lista_cr = glob.glob(os.path.join(self.pasta_downloads, "*.crdownload"))
            if lista_cr:
                arquivo_cr = lista_cr[0] # Pega o primeiro .crdownload que aparecer
                break
            if time.time() - start_time_inicio > timeout_inicio:
                raise Exception("Timeout: Download não iniciado (nenhum .crdownload apareceu).")
            time.sleep(0.5)

        print(f"[Sistema] Download iniciado: {os.path.basename(arquivo_cr)}")

        # 2. Esperar o arquivo .crdownload desaparecer (download concluir)
        print("[Sistema] ...download em andamento (esperando .crdownload desaparecer)...")
        start_time_download = time.time()
        
        while os.path.exists(arquivo_cr):
            if time.time() - start_time_download > timeout_segundos:
                raise Exception(f"Timeout: Download não concluído (.crdownload não desapareceu a tempo).")
            time.sleep(1)

        print("[Sistema] Download concluído!")

        # 3. Encontrar o arquivo .csv final (que NÃO é .crdownload)
        # O nome final pode ser diferente do .crdownload (ex: "produtos (1).csv")
        arquivos_csv = glob.glob(os.path.join(self.pasta_downloads, "*.csv"))
        if not arquivos_csv:
            raise Exception("Erro: Download concluído, mas nenhum arquivo .csv foi encontrado.")
            
        arquivo_recente = max(arquivos_csv, key=os.path.getctime)
        print(f"[Sistema] Arquivo baixado: {arquivo_recente}")
        return arquivo_recente

    def fechar_navegador(self):
        print("[Web] Fechando o navegador.")
        self.driver.quit()

    def executar_extracao(self):
        """
        Orquestra a automação E o monitoramento do download.
        Retorna o CAMINHO do arquivo baixado, ou None se falhar.
        """
        try:
            self.fazer_login()
            self.navegar_para_produtos()
            
            # Clica para exportar (e limpa a pasta)
            sucesso_cliques = self.coletar_dados_produtos()
            
            if sucesso_cliques:
                # Espera a barra <mat-progress-bar> sumir
                if self._esperar_processamento_servidor():
                    # Espera o arquivo .crdownload sumir
                    caminho_arquivo = self._monitorar_download_concluido()
                    return caminho_arquivo
            
            return None # Retorna None se qualquer etapa falhar

        except Exception as e:
            print(f"\n[Web] Ocorreu um erro fatal durante a automação: {e}")
            return None 
        
        finally:
            self.fechar_navegador()

# --- Bloco de Execução Independente (Modo de Teste) ---
if __name__ == "__main__":
    print("--- Executando 'aut_pp_produtos.py' em modo de teste ---")
    config = carregar_config()
    login_cfg = config.get("login")
    
    if login_cfg:
        automator = PlugPharmaAutomator(login_cfg)
        caminho_arquivo_final = automator.executar_extracao()
        
        if caminho_arquivo_final:
            print(f"\n[Teste] Download CONCLUÍDO com sucesso.")
            print(f"[Teste] Arquivo está em: {caminho_arquivo_final}")
        else:
            print("\n[Teste] A extração (download) falhou.")
    else:
        print("[Teste] Erro: Seção 'login' não encontrada no config.json")