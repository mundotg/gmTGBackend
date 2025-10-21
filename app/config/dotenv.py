import os
from dotenv import load_dotenv
from typing import Optional, Union

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
    return os.getenv(name, default)

def get_env_bool(name: str, default: bool = False) -> bool:
    """
    Retorna o valor de uma variável de ambiente como booleano.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - default (bool): Valor padrão caso a variável não exista.

    Retorna:
    - bool: Valor da variável convertido para booleano.
    """
    value = get_env(name)
    if value is None:
        return default
    
    true_values = ['true', '1', 'yes', 'on', 't', 'y']
    return value.lower() in true_values

def get_env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    """
    Retorna o valor de uma variável de ambiente como inteiro.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - default (int, opcional): Valor padrão caso a variável não exista ou seja inválida.

    Retorna:
    - int | None: Valor da variável convertido para inteiro.
    """
    value = get_env(name)
    if value is None:
        return default
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def get_env_float(name: str, default: Optional[float] = None) -> Optional[float]:
    """
    Retorna o valor de uma variável de ambiente como float.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - default (float, opcional): Valor padrão caso a variável não exista ou seja inválida.

    Retorna:
    - float | None: Valor da variável convertido para float.
    """
    value = get_env(name)
    if value is None:
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_env_list(name: str, separator: str = ',', default: Optional[list] = None) -> list:
    """
    Retorna o valor de uma variável de ambiente como lista.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - separator (str): Separador para dividir a string.
    - default (list, opcional): Valor padrão caso a variável não exista.

    Retorna:
    - list: Lista de valores.
    """
    value = get_env(name)
    if value is None:
        return default or []
    
    return [item.strip() for item in value.split(separator) if item.strip()]

def get_env_list_cors(name: str, default: Optional[list] = None, separator: str = ",") -> list:
    """
    Retorna uma lista de origens CORS a partir da variável de ambiente.

    Aceita tanto:
    - BACKEND_CORS_ORIGINS=http://localhost:3000,https://app.com
    - BACKEND_CORS_ORIGINS=["http://localhost:3000", "https://app.com"]
    """
    import json
    value = get_env(name)

    if not value:
        return default or []

    # Se já for lista
    if isinstance(value, list):
        return value

    # Se vier como JSON string
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass

    # Separar por vírgula
    return [item.strip() for item in value.split(separator) if item.strip()]


def set_env(name: str, value: str) -> None:
    """
    Define uma variável de ambiente temporariamente.

    Parâmetros:
    - name (str): Nome da variável de ambiente.
    - value (str): Valor a ser definido.
    """
    os.environ[name] = value

def require_env(name: str) -> str:
    """
    Retorna o valor de uma variável de ambiente obrigatória.

    Parâmetros:
    - name (str): Nome da variável de ambiente.

    Retorna:
    - str: Valor da variável.

    Levanta:
    - ValueError: Se a variável não estiver definida.
    """
    value = get_env(name)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória '{name}' não definida")
    return value

# Funções de conveniência para configurações específicas
def get_app_config() -> dict:
    """Retorna configurações da aplicação."""
    return {
        'name': get_env('APP_NAME', 'GeradorDadosInteligente'),
        'env': get_env('APP_ENV', 'development'),
        'debug': get_env_bool('APP_DEBUG', True),
        'locale': get_env('APP_LOCALE', 'pt_PT')
    }

def get_cache_config() -> dict:
    """Retorna configurações de cache."""
    return {
        'enabled': get_env_bool('CACHE_ENABLED', True),
        'ttl': get_env_int('CACHE_TTL_SECONDS', 3600),
        'max_size_mb': get_env_int('CACHE_MAX_SIZE_MB', 100),
        'cleanup_interval': get_env_int('CACHE_CLEANUP_INTERVAL', 3600)
    }

def get_database_config() -> dict:
    """Retorna configurações do banco de dados."""
    return {
        'host': get_env('DB_HOST', 'localhost'),
        'port': get_env_int('DB_PORT', 5432),
        'name': get_env('DB_NAME', 'meu_banco'),
        'user': get_env('DB_USER', 'usuario'),
        'password': get_env('DB_PASSWORD', 'senha_segura'),
        'schema': get_env('DB_SCHEMA', 'public')
    }

def get_log_config() -> dict:
    """Retorna configurações de log."""
    return {
        'level': get_env('LOG_LEVEL', 'INFO'),
        'file': get_env('LOG_FILE', 'app.log'),
        'max_size_mb': get_env_int('LOG_MAX_SIZE_MB', 10),
        'backup_count': get_env_int('LOG_BACKUP_COUNT', 5)
    }

def get_generator_config() -> dict:
    """Retorna configurações do gerador de dados."""
    return {
        'default_locale': get_env('GENERATOR_DEFAULT_LOCALE', 'pt_PT'),
        'max_retries': get_env_int('GENERATOR_MAX_RETRIES', 3),
        'unique_retries': get_env_int('GENERATOR_UNIQUE_RETRIES', 100),
        'null_probability': get_env_float('GENERATOR_NULL_PROBABILITY', 0.05)
    }

def get_faker_config() -> dict:
    """Retorna configurações do Faker."""
    return {
        'locale': get_env('FAKER_LOCALE', 'pt_PT'),
        'seed': get_env_int('FAKER_SEED'),
        'providers': get_env_list('FAKER_PROVIDERS')
    }

# Validação básica ao carregar o módulo
def _validate_required_config():
    """Valida configurações obrigatórias."""
    required_vars = ['APP_NAME', 'SECRET_KEY']
    
    for var in required_vars:
        if get_env(var) is None:
            print(f"AVISO: Variável de ambiente '{var}' não está definida")

# Executa validação ao importar
_validate_required_config()
