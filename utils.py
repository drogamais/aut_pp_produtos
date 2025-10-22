# utils.py

import json
import sys

def carregar_config(arquivo="config.json"):
    """
    Carrega as configurações do arquivo JSON.
    """
    try:
        with open(arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Sistema] Erro: Arquivo de configuração '{arquivo}' não encontrado.")
        sys.exit(1) # Termina o script se a config não existe
    except json.JSONDecodeError:
        print(f"[Sistema] Erro: O arquivo '{arquivo}' não é um JSON válido.")
        sys.exit(1)