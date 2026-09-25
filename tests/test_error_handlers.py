"""
Testes dos handlers globais de erro.

Usam uma app mínima em vez da app real: o objetivo é exercitar os handlers,
não arrancar a stack completa (que carrega OCR, cache e ligações a BDs).
"""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError

from app.middleware import RequestContextMiddleware, register_exception_handlers


class Corpo(BaseModel):
    n: int


@pytest.fixture
def client():
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    register_exception_handlers(app)

    @app.get("/ok")
    def ok():
        return {"v": 1}

    @app.get("/boom")
    def boom():
        raise RuntimeError("detalhe interno com senha=abc123")

    @app.get("/db")
    def db():
        raise OperationalError("SELECT * FROM users", {}, Exception("recusado"))

    @app.get("/lento")
    def lento():
        raise HTTPException(429, "devagar", headers={"Retry-After": "42"})

    @app.post("/val")
    def val(corpo: Corpo):
        return corpo

    return TestClient(app, raise_server_exceptions=False)


def test_resposta_ok_tem_request_id(client):
    resposta = client.get("/ok")

    assert resposta.status_code == 200
    assert resposta.headers["X-Request-ID"]


def test_erro_nao_tratado_devolve_500_com_request_id(client):
    resposta = client.get("/boom")
    corpo = resposta.json()

    assert resposta.status_code == 500
    assert corpo["type"] == "internal_error"
    # O ID tem de estar presente e coincidir com o header: é o que permite
    # ligar o relato do utilizador à linha de log.
    assert corpo["request_id"] not in ("", "-", None)
    assert corpo["request_id"] == resposta.headers["X-Request-ID"]


def test_erro_de_bd_nao_expoe_a_query(client):
    resposta = client.get("/db")

    assert resposta.status_code == 503
    assert resposta.json()["type"] == "database_error"
    assert "users" not in resposta.text
    assert "SELECT" not in resposta.text


def test_headers_da_excecao_sao_preservados(client):
    # Sem isto, o Retry-After do rate limiter perder-se-ia no handler.
    resposta = client.get("/lento")

    assert resposta.status_code == 429
    assert resposta.headers["Retry-After"] == "42"


def test_erro_de_validacao_e_uniformizado(client):
    resposta = client.post("/val", json={"n": "nao-e-inteiro"})
    corpo = resposta.json()

    assert resposta.status_code == 422
    assert corpo["type"] == "validation_error"
    assert corpo["errors"]


def test_404_segue_o_mesmo_formato(client):
    corpo = client.get("/inexistente").json()

    assert corpo["type"] == "http_error"
    assert "request_id" in corpo
