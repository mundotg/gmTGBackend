# app/config/dotenv.py
import os
from dotenv import load_dotenv
from typing import Optional

# Carrega variáveis do arquivo .env
load_dotenv()

def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Retorna o valor de uma variável de ambiente.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - default (str, opcional): Valor padrão caso a variável não exista.

    Retorna:
    - str | None: Valor da variável ou o padrão informado.
    """
    # print(f"Obtendo variável de ambiente: {name} com valor padrão: {default}")
    # print(f"Valor obtido: {os.getenv(name)}")
    return os.getenv(name, default)
