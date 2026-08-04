"""Testes do rate limiter do login (backend em memória)."""

import time

import pytest
from fastapi import HTTPException

from app.ultils import rate_limit


@pytest.fixture(autouse=True)
def backend_em_memoria(monkeypatch):
    """Força o fallback em memória e isola o estado entre testes."""
    monkeypatch.setattr(rate_limit, "_redis_checked", True)
    monkeypatch.setattr(rate_limit, "_redis_client", None)
    rate_limit._memory_hits.clear()
    yield
    rate_limit._memory_hits.clear()


def test_permite_ate_ao_limite():
    for _ in range(5):
        rate_limit.check_rate_limit("k1", limit=5, window=300)


def test_bloqueia_acima_do_limite():
    for _ in range(5):
        rate_limit.check_rate_limit("k2", limit=5, window=300)

    with pytest.raises(HTTPException) as exc:
        rate_limit.check_rate_limit("k2", limit=5, window=300)

    assert exc.value.status_code == 429
    # O cliente precisa de saber quando pode tentar de novo.
    assert int(exc.value.headers["Retry-After"]) > 0


def test_chaves_sao_independentes():
    for _ in range(5):
        rate_limit.check_rate_limit("ip:1.1.1.1", limit=5, window=300)

    # Outro IP não pode herdar o bloqueio do primeiro.
    rate_limit.check_rate_limit("ip:2.2.2.2", limit=5, window=300)


def test_janela_liberta_apos_expirar():
    rate_limit.check_rate_limit("k3", limit=1, window=1)

    with pytest.raises(HTTPException):
        rate_limit.check_rate_limit("k3", limit=1, window=1)

    time.sleep(1.1)

    rate_limit.check_rate_limit("k3", limit=1, window=1)


def test_falha_em_modo_aberto(monkeypatch):
    # Se o contador rebentar, o login legítimo tem de passar — negar o
    # serviço a toda a gente seria pior do que perder a proteção.
    def rebenta(*args, **kwargs):
        raise RuntimeError("backend em baixo")

    monkeypatch.setattr(rate_limit, "_hit_memory", rebenta)

    rate_limit.check_rate_limit("k4", limit=1, window=300)
