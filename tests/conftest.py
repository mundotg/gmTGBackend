"""
Configuração partilhada dos testes.

As variáveis de ambiente têm de ser definidas ANTES de qualquer import de
`app.*`: vários módulos leem configuração no momento do import (e alguns
recusam-se a carregar sem SECRET_KEY / ENCRYPTION_KEY).
"""

import base64
import os

os.environ.setdefault("ENV", "development")
os.environ.setdefault("SECRET_KEY", "chave-de-teste-nao-usar-em-producao")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_suite.db")
os.environ.setdefault(
    "ENCRYPTION_KEY",
    base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode(),
)

import pytest  # noqa: E402


@pytest.fixture
def chave_mestra_nova(monkeypatch):
    """Troca a ENCRYPTION_KEY e limpa a cache, simulando outro ambiente."""
    from app.services import crypto_utils

    def _trocar():
        monkeypatch.setattr(crypto_utils, "_master_key_cache", None)
        monkeypatch.setenv("ENCRYPTION_KEY", crypto_utils.generate_master_key())

    return _trocar
