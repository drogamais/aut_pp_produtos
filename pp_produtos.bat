@echo off
TITLE Automacao PlugPharma Produtos

ECHO [SISTEMA] Iniciando...
ECHO [SISTEMA] Navegando para o diretorio do script...
REM Garante que o script rode a partir da pasta onde ele esta
cd /d %~dp0

ECHO [SISTEMA] Ativando o Ambiente Virtual (venv)...
REM Tenta ativar o venv. Se nao encontrar, avisa e sai.
IF NOT EXIST "venv\Scripts\activate.bat" (
    ECHO [ERRO] Ambiente virtual 'venv' nao encontrado.
    ECHO [ERRO] Execute o passo a passo do README.md para criar o venv.
    PAUSE
    EXIT /B 1
)
CALL venv\Scripts\activate

ECHO [SISTEMA] Venv ativado. Iniciando o run.py (LOG em execucao.log)...
ECHO [SISTEMA] Aguarde, o console ficara em silencio durante a execucao...
ECHO -----------------------------------------------------

REM --- INICIO DA ALTERACAO ---
REM Executa o Python, salvando TUDO (saida e erro) no 'execucao.log'.
REM O '>' sobrescreve o arquivo. O '2>&1' redireciona erros para a saida.
python run.py %* > execucao.log 2>&1
REM --- FIM DA ALTERACAO ---

ECHO -----------------------------------------------------
ECHO [SISTEMA] Processo finalizado. Exibindo log (execucao.log):
ECHO.

REM Exibe o conteudo do log recem-criado no console
TYPE execucao.log

ECHO.
ECHO -----------------------------------------------------
ECHO [SISTEMA] Log completo salvo em execucao.log