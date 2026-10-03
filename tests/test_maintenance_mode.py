"""
Testes unitários e de integração para o Modo Manutenção (MaintenanceModeMiddleware).

Requisitos:
- Crítico: Recusa pedidos de quem não é administrador, com 503.
- O login e os endpoints de health continuam abertos.
- Só atua se a funcionalidade 'maintenance_mode' for ativada nas definições do sistema.
"""

import base64
import os
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.testclient import TestClient

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.middleware.maintenance_mode import (
    MaintenanceModeMiddleware,
    ROTAS_SEMPRE_ABERTAS,
    _rota_aberta,
    is_maintenance_mode_enabled,
)
from app.services import system_settings_service as definicoes


# ══════════════════════════ Testes de Rotas Abertas ══════════════════════════

@pytest.mark.parametrize(
    "caminho",
    [
        "/auth/login",
        "/auth/refresh",
        "/auth/logout",
        "/health",
        "/health/ready",
        "/health/live",
        "/system/",
        "/system/status",
        "/users/rbac/me",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/favicon.ico",
    ],
)
def test_rotas_sempre_abertas_permitem_acesso_essencial(caminho):
    """Endpoints de sobrevivência e manutenção continuam sempre abertos."""
    assert _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS) is True


@pytest.mark.parametrize(
    "caminho",
    [
        "/api/users",
        "/projects/list",
        "/tasks/create",
        "/sql-editor/execute",
        "/database/table/info",
        "/conn/connections/",
        "/exe/update_row",
    ],
)
def test_rotas_de_negocio_sao_sujeitas_a_manutencao(caminho):
    """Endpoints de operação e dados devem ser protegidos em manutenção."""
    assert _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS) is False


# ══════════════════════════ Teste is_maintenance_mode_enabled ══════════════════════════

def test_is_maintenance_mode_enabled_com_mock():
    with patch("app.services.system_settings_service.obter", return_value=True):
        assert is_maintenance_mode_enabled() is True

    with patch("app.services.system_settings_service.obter", return_value=False):
        assert is_maintenance_mode_enabled() is False

    with patch("app.services.system_settings_service.obter", side_effect=Exception("DB indisponível")):
        # Em caso de erro na BD, não bloqueia o sistema (retorna False)
        assert is_maintenance_mode_enabled() is False


# ══════════════════════════ Teste do Middleware com TestClient ══════════════════════════

@pytest.fixture
def test_app():
    """Cria uma mini aplicação FastAPI com MaintenanceModeMiddleware e rotas de teste."""
    app = FastAPI()
    app.add_middleware(MaintenanceModeMiddleware)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/auth/login")
    def login():
        return {"token": "dummy-token"}

    @app.get("/system/status")
    def system_status():
        return {"maintenance": False}

    @app.get("/users/rbac/me")
    def rbac_me():
        return {"id": 1, "role": "admin"}

    @app.get("/api/dados-protegidos")
    def dados_protegidos():
        return {"dados": [1, 2, 3]}

    @app.post("/api/alterar-estado")
    def alterar_estado():
        return {"sucesso": True}

    return app


def test_modo_manutencao_desligado_permite_tudo(test_app):
    """Quando o modo manutenção está desligado, todos os pedidos passam normalmente."""
    client = TestClient(test_app)

    with patch("app.middleware.maintenance_mode.is_maintenance_mode_enabled", return_value=False):
        resp_health = client.get("/health")
        assert resp_health.status_code == 200

        resp_dados = client.get("/api/dados-protegidos")
        assert resp_dados.status_code == 200
        assert resp_dados.json() == {"dados": [1, 2, 3]}

        resp_post = client.post("/api/alterar-estado")
        assert resp_post.status_code == 200


def test_modo_manutencao_ligado_rotas_abertas_continuam_200(test_app):
    """Mesmo com manutenção ligada, login, health e system continuam abertos."""
    client = TestClient(test_app)

    with patch("app.middleware.maintenance_mode.is_maintenance_mode_enabled", return_value=True):
        resp_health = client.get("/health")
        assert resp_health.status_code == 200

        resp_login = client.post("/auth/login")
        assert resp_login.status_code == 200

        resp_system = client.get("/system/status")
        assert resp_system.status_code == 200

        resp_me = client.get("/users/rbac/me")
        assert resp_me.status_code == 200


def test_modo_manutencao_ligado_bloqueia_nao_admin_com_503(test_app):
    """Sem token ou com utilizador não-admin, pedidos a rotas protegidas recebem 503."""
    client = TestClient(test_app)

    with patch("app.middleware.maintenance_mode.is_maintenance_mode_enabled", return_value=True):
        # 1. Sem token nenhum
        resp = client.get("/api/dados-protegidos")
        assert resp.status_code == 503
        dados = resp.json()
        assert dados["maintenance"] is True
        assert "Sistema em manutenção" in dados["detail"]
        assert resp.headers.get("retry-after") == "120"

        # 2. Com token de utilizador normal (não admin)
        with patch.object(MaintenanceModeMiddleware, "_e_admin", return_value=False):
            resp2 = client.get("/api/dados-protegidos", headers={"Authorization": "Bearer token-usuario-normal"})
            assert resp2.status_code == 503
            assert resp2.json()["maintenance"] is True


def test_modo_manutencao_ligado_permite_admin(test_app):
    """Administradores (super admin) conseguem aceder a rotas protegidas durante a manutenção."""
    client = TestClient(test_app)

    with patch("app.middleware.maintenance_mode.is_maintenance_mode_enabled", return_value=True):
        with patch.object(MaintenanceModeMiddleware, "_e_admin", return_value=True):
            resp = client.get("/api/dados-protegidos", headers={"Authorization": "Bearer token-admin"})
            assert resp.status_code == 200
            assert resp.json() == {"dados": [1, 2, 3]}

            resp_post = client.post("/api/alterar-estado", headers={"Authorization": "Bearer token-admin"})
            assert resp_post.status_code == 200


import asyncio


def test_e_admin_verificacoes():
    """Valida a extração de token via header e cookie e verificação de superadmin."""
    async def _run():
        middleware = MaintenanceModeMiddleware(FastAPI())

        req_sem_token = MagicMock()
        req_sem_token.headers = {}
        req_sem_token.cookies = {}
        assert await middleware._e_admin(req_sem_token) is False

        # Token inválido no header
        req_token_invalido = MagicMock()
        req_token_invalido.headers = {"Authorization": "Bearer token_invalido"}
        req_token_invalido.cookies = {}
        with patch("app.auth.decode_token", return_value=None):
            assert await middleware._e_admin(req_token_invalido) is False

        # Utilizador superadmin ativo
        req_admin = MagicMock()
        req_admin.headers = {"Authorization": "Bearer token_valido"}
        req_admin.cookies = {}

        mock_user = MagicMock()
        mock_user.is_active = True

        mock_db = MagicMock()
        mock_db.query.return_value.options.return_value.filter.return_value.first.return_value = mock_user

        with patch("app.auth.decode_token", return_value={"sub": "10"}):
            with patch("app.middleware.maintenance_mode.SessionLocal", return_value=mock_db):
                with patch("app.ultils.permissions.is_superadmin", return_value=True):
                    assert await middleware._e_admin(req_admin) is True

                # Se não for superadmin, recusa
                with patch("app.ultils.permissions.is_superadmin", return_value=False):
                    assert await middleware._e_admin(req_admin) is False

                # Se for inativo, recusa
                mock_user.is_active = False
                with patch("app.ultils.permissions.is_superadmin", return_value=True):
                    assert await middleware._e_admin(req_admin) is False

        # Autenticação por Cookie
        req_cookie = MagicMock()
        req_cookie.headers = {}
        req_cookie.cookies = {"access_token": "token_do_cookie"}
        mock_user.is_active = True

        with patch("app.auth.decode_token", return_value={"user_id": 10}):
            with patch("app.middleware.maintenance_mode.SessionLocal", return_value=mock_db):
                with patch("app.ultils.permissions.is_superadmin", return_value=True):
                    assert await middleware._e_admin(req_cookie) is True

    asyncio.run(_run())
