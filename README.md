# Automação PlugPharma - Extração de Produtos

Este projeto é um robô (RPA) desenvolvido em Python que automatiza a extração de um relatório de produtos da plataforma PlugPharma e, em seguida, processa e insere esses dados em um banco de dados MariaDB.

## O que o robô faz?

O script `run.py` orquestra duas tarefas principais:

1.  **Extração (aut_pp_produtos.py):**
    * Acessa a plataforma PlugPharma usando Selenium (em modo *headless* ou visível para debug).
    * Navega até a página de exportação de produtos.
    * Solicita o relatório em formato `.csv`.
    * Aguarda pacientemente o processamento do servidor, que pode levar **até 1 hora**.
    * Monitora a pasta `downloads/` até que o arquivo seja baixado com sucesso.

2.  **Carga (database.py):**
    * Lê o arquivo `.csv` mais recente da pasta `downloads/`.
    * Processa os dados usando Pandas (limpa, normaliza códigos de barras e trata datas).
    * Adiciona um timestamp `data_insercao` para rastreabilidade.
    * Conecta-se ao banco de dados MariaDB (definido no `config.json`).
    * Recria (DROP/CREATE) a tabela `bronze_plugpharma_produtos` e insere todos os dados de uma vez.

## Pré-requisitos

* Python 3.8 ou superior
* Google Chrome instalado (o `webdriver-manager` irá gerenciá-lo)
* Acesso de rede ao banco de dados MariaDB de destino

## Passo a Passo para Instalação

Siga estes passos para configurar e rodar o projeto pela primeira vez.

### 1. Crie o Ambiente Virtual (venv)

Na pasta raiz do projeto, crie um ambiente virtual chamado `venv`:

```bash
python -m venv venv
```

### 2. Ative o Ambiente

Você deve ativar o ambiente toda vez que for rodar o projeto.

No Windows (PowerShell/CMD):
```bash
.\venv\Scripts\activate
```

### 3. Instale as Dependências

Com o (venv) ativo, instale todas as bibliotecas listadas no requirements.txt:
```bash
pip install -r requirements.txt
```

## Como Executar o Robô

A execução é controlada principalmente pelo arquivo aut_pp_produtos.bat. Ele foi criado para gerenciar automaticamente a ativação do ambiente virtual (venv) e salvar um log de tudo o que acontece no arquivo execucao.log.

Existem duas formas de usá-lo:

### Execução Normal (Modo Oculto)
```bash
.\pp_produtos.bat
```

### Modo de Debug (Modo Visível)
```bash
.\pp_produtos.bat --dev
```