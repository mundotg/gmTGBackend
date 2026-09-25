"""
Construção da URL de ligação ao Redis.

Estas linhas tinham dois defeitos que só apareceriam no pior momento:

1. `host = (get_env("REDIS_HOST", "localhost"),)` — a vírgula final dentro de
   parênteses cria um TUPLO. A URL de reserva ficava
   `redis://('localhost',):(6379,)/(0,)`. Nunca ligaria a nada; só não se via
   porque `app_cache_REDIS_URL` está definida e ganha.
2. A password era lida do ambiente e nunca chegava ao cliente. Um Redis
   protegido respondia NOAUTH com a configuração aparentemente correta.
"""

import base64
import os

import pytest

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.config import redis as modulo_redis  # noqa: E402


@pytest.fixture
def env(monkeypatch):
    """Controla o que `_build_url` vê, sem depender do .env do projeto."""

    def aplicar(**valores):
        def falso_get_env(chave, default=None):
            return valores.get(chave, default)

        monkeypatch.setattr(modulo_redis, "get_env", falso_get_env)

        # As constantes são lidas no import; recalcula-se para o teste.
        monkeypatch.setattr(modulo_redis, "REDIS_HOST", valores.get("REDIS_HOST", "localhost"))
        monkeypatch.setattr(modulo_redis, "REDIS_PORT", int(valores.get("REDIS_PORT", 6379)))
        monkeypatch.setattr(modulo_redis, "REDIS_DB", int(valores.get("REDIS_DB", 0)))
        monkeypatch.setattr(modulo_redis, "REDIS_PASSWORD", valores.get("REDIS_PASSWORD") or None)
        monkeypatch.setattr(modulo_redis, "REDIS_USERNAME", valores.get("REDIS_USERNAME") or None)
        return modulo_redis._build_url()

    return aplicar


def test_sem_password_url_simples(env):
    assert env(REDIS_HOST="redis", REDIS_PORT="6379", REDIS_DB="0") == "redis://redis:6379/0"


def test_a_url_nunca_contem_tuplos(env):
    """O defeito original: `('localhost',)` em vez de `localhost`."""
    url = env(REDIS_HOST="redis", REDIS_PORT="6379", REDIS_DB="0")

    assert "(" not in url and ")" not in url
    assert "," not in url


def test_password_entra_na_url(env):
    url = env(REDIS_HOST="redis", REDIS_PASSWORD="segredo")

    assert "segredo" in url
    assert url == "redis://:segredo@redis:6379/0"


def test_utilizador_e_password(env):
    url = env(REDIS_HOST="redis", REDIS_USERNAME="default", REDIS_PASSWORD="segredo")
    assert url == "redis://default:segredo@redis:6379/0"


def test_password_com_simbolos_e_escapada(env):
    """
    Uma password com `@`, `:` ou `/` partia a URL e o cliente ligava-se ao
    sítio errado — ou a lado nenhum.
    """
    url = env(REDIS_HOST="redis", REDIS_PASSWORD="p@ss:w/rd")

    # Os símbolos aparecem codificados, não em cru.
    assert "p@ss:w/rd" not in url
    assert "%40" in url and "%3A" in url and "%2F" in url
    # E o host continua a ser identificável depois do separador de credenciais.
    assert url.endswith("@redis:6379/0")


def test_url_completa_do_ambiente_ganha(env):
    """Quem já tem a ligação inteira numa variável (Redis Cloud) não é ignorado."""
    url = env(
        app_cache_REDIS_URL="rediss://default:abc@nuvem.exemplo:16999",
        REDIS_HOST="ignorado",
        REDIS_PASSWORD="ignorada",
    )
    assert url == "rediss://default:abc@nuvem.exemplo:16999"


def test_base_de_dados_diferente_de_zero(env):
    assert env(REDIS_HOST="redis", REDIS_DB="3").endswith("/3")
