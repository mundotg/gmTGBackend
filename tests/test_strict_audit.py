"""
Testes para o Modo de Auditoria Rigorosa (StrictAuditMiddleware).

Garante que:
1. O corpo de pedidos POST, PUT, PATCH e DELETE é higienizado (senhas e tokens substituídos por '***').
2. Só é executado quando a definição 'strict_audit' estiver ativada no sistema.
3. Os endpoints a jusante conseguem ler o corpo do pedido normalmente (stream não é quebrado).
4. Métodos GET e rotas de monitorização não geram auditoria de corpo.
"""

import json
from unittest.mock import patch, MagicMock
import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from fastapi import FastAPI, Body

from app.middleware.strict_audit import (
    StrictAuditMiddleware,
    _e_sensivel,
    _redigir,
    _redigir_form_urlencoded,
    _redigir_corpo_bytes,
    REDIGIDO,
    MÉTODOS_AUDITADOS,
)


def test_metodos_auditados():
    assert "POST" in MÉTODOS_AUDITADOS
    assert "PUT" in MÉTODOS_AUDITADOS
    assert "PATCH" in MÉTODOS_AUDITADOS
    assert "DELETE" in MÉTODOS_AUDITADOS
    assert "GET" not in MÉTODOS_AUDITADOS
    assert "OPTIONS" not in MÉTODOS_AUDITADOS


@pytest.mark.parametrize(
    "chave",
    [
        "password",
        "senha",
        "nova_senha",
        "confirmar_senha",
        "token",
        "access_token",
        "refresh_token",
        "secret",
        "client_secret",
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "private_key",
        "auth",
        "pin",
    ],
)
def test_identificacao_campos_sensiveis(chave):
    assert _e_sensivel(chave) is True
    assert _e_sensivel(chave.upper()) is True


@pytest.mark.parametrize(
    "chave",
    ["nome", "apelido", "email", "empresa_id", "cargo", "status", "descricao", "telefone"],
)
def test_campos_normais_nao_sensiveis(chave):
    assert _e_sensivel(chave) is False


def test_redacao_dados_aninhados_e_listas():
    payload = {
        "nome": "Sebastião",
        "email": "user@example.com",
        "senha": "super_secret_password_123",
        "token": "jwt.eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        "credenciais": {
            "api_key": "live_key_xyz987",
            "servidor": "db.prod.local",
            "private_key": "-----BEGIN RSA PRIVATE KEY-----",
        },
        "membros": [
            {"id": 1, "nome": "Membro 1", "access_token": "token_1"},
            {"id": 2, "nome": "Membro 2", "password": "pass_2"},
        ],
    }

    redigido = _redigir(payload)

    # Campos normais preservados
    assert redigido["nome"] == "Sebastião"
    assert redigido["email"] == "user@example.com"
    assert redigido["credenciais"]["servidor"] == "db.prod.local"
    assert redigido["membros"][0]["nome"] == "Membro 1"

    # Segredos substituídos
    assert redigido["senha"] == REDIGIDO
    assert redigido["token"] == REDIGIDO
    assert redigido["credenciais"]["api_key"] == REDIGIDO
    assert redigido["credenciais"]["private_key"] == REDIGIDO
    assert redigido["membros"][0]["access_token"] == REDIGIDO
    assert redigido["membros"][1]["password"] == REDIGIDO

    # Nenhum segredo original sobrevive na string JSON
    dump = json.dumps(redigido)
    for segredo in ("super_secret_password_123", "live_key_xyz987", "token_1", "pass_2"):
        assert segredo not in dump


def test_redacao_form_urlencoded():
    texto = "nome=Admin&senha=minhaSenha123&client_secret=secretKey&cargo=Gestor"
    redigido = _redigir_form_urlencoded(texto)
    assert "nome=Admin" in redigido
    assert "cargo=Gestor" in redigido
    assert "senha=%2A%2A%2A" in redigido or "senha=***" in redigido
    assert "minhaSenha123" not in redigido
    assert "secretKey" not in redigido


def test_redigir_corpo_bytes_json_e_limites():
    corpo = json.dumps({"user": "carlos", "password": "123"}).encode("utf-8")
    res = _redigir_corpo_bytes(corpo, "application/json")
    dados = json.loads(res)
    assert dados["user"] == "carlos"
    assert dados["password"] == REDIGIDO

    # Corpo vazio
    assert _redigir_corpo_bytes(b"", "application/json") == "<corpo vazio>"

    # Corpo excedendo MAX_CORPO
    grande = b"x" * 5000
    res_grande = _redigir_corpo_bytes(grande, "text/plain")
    assert "não gravado por exceder o limite" in res_grande


# ══════════════════════════════════════════════════════════════════════
# TESTES DE INTEGRAÇÃO COM FASTAPI TESTCLIENT
# ══════════════════════════════════════════════════════════════════════

def test_middleware_com_auditoria_desativada():
    """Quando strict_audit=False, não audita e o handler funciona normalmente."""
    app = FastAPI()
    app.add_middleware(StrictAuditMiddleware)

    @app.post("/itens")
    async def criar_item(dados: dict = Body(...)):
        return {"recebido": dados}

    client = TestClient(app)

    with patch("app.middleware.strict_audit.is_strict_audit_enabled", return_value=False):
        with patch("app.middleware.strict_audit.log_message") as mock_log:
            resp = client.post("/itens", json={"nome": "Produto", "password": "secreta"})
            assert resp.status_code == 200
            assert resp.json()["recebido"]["password"] == "secreta"
            # Não deve ter chamado log_message de auditoria
            assert not any(
                call.kwargs.get("source") == "strict_audit" for call in mock_log.call_args_list
            )


def test_middleware_com_auditoria_ativada_registra_e_redige():
    """Quando strict_audit=True, audita o pedido com segredos redigidos e entrega o corpo real ao handler."""
    app = FastAPI()
    app.add_middleware(StrictAuditMiddleware)

    @app.post("/login")
    async def login(dados: dict = Body(...)):
        # O handler precisa receber os dados REAIS
        return {"autenticado": dados["password"] == "segredo_real"}

    @app.put("/utilizadores/{id}")
    async def atualizar(id: int, dados: dict = Body(...)):
        return {"id": id, "atualizado": True}

    @app.get("/itens")
    async def listar():
        return {"items": []}

    client = TestClient(app)

    with patch("app.middleware.strict_audit.is_strict_audit_enabled", return_value=True):
        with patch("app.middleware.strict_audit.log_message") as mock_log:
            # 1. Teste POST
            resp = client.post(
                "/login",
                json={"email": "teste@mustainf.com", "password": "segredo_real", "token": "abc"},
                headers={"X-Request-ID": "req-1234"},
            )
            assert resp.status_code == 200
            assert resp.json()["autenticado"] is True

            # Verifica se foi gravado na auditoria
            audit_calls = [
                call for call in mock_log.call_args_list
                if call.kwargs.get("source") == "strict_audit"
            ]
            assert len(audit_calls) >= 1
            msg = audit_calls[0].args[0]
            assert "[auditoria] POST /login" in msg
            assert "segredo_real" not in msg
            assert "abc" not in msg
            assert REDIGIDO in msg
            assert "teste@mustainf.com" in msg

            # 2. Teste GET (não deve auditar)
            mock_log.reset_mock()
            resp_get = client.get("/itens")
            assert resp_get.status_code == 200
            assert not any(
                call.kwargs.get("source") == "strict_audit" for call in mock_log.call_args_list
            )

            # 3. Teste PUT
            mock_log.reset_mock()
            resp_put = client.put(
                "/utilizadores/10",
                json={"nome": "João", "token": "token_secreto_777"},
            )
            assert resp_put.status_code == 200
            audit_calls_put = [
                call for call in mock_log.call_args_list
                if call.kwargs.get("source") == "strict_audit"
            ]
            assert len(audit_calls_put) >= 1
            msg_put = audit_calls_put[0].args[0]
            assert "[auditoria] PUT /utilizadores/10" in msg_put
            assert "token_secreto_777" not in msg_put
            assert REDIGIDO in msg_put
