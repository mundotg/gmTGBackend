"""
Regressão dos três bugs do suporte a MongoDB.

Observado em produção:

    ERROR - Teste de conexão falhou (MongoDB): Authentication failed.,
            full error: {'code': 18, 'codeName': 'AuthenticationFailed'}
    WARNING - Erro ao limpar engine do usuário 1:
              'Database' object is not callable
"""

from unittest.mock import patch

import pytest
from pymongo import MongoClient

from app.ultils.conect_database import DatabaseManager, close_engine


@pytest.fixture
def uri_gerada():
    """Captura a URI passada ao MongoClient sem abrir ligação real."""
    capturado = {}

    def _fake_client(uri, **kwargs):
        capturado["uri"] = uri
        return MongoClient(uri, connect=False)

    def _gerar(config):
        with patch("app.ultils.conect_database.MongoClient", _fake_client):
            DatabaseManager.get_engine("MongoDB", config)
        return capturado["uri"]

    return _gerar


BASE = {
    "host": "localhost",
    "port": 27017,
    "database": "notificacoes",
}


class TestUriDoMongo:
    def test_credenciais_com_caracteres_reservados_sao_escapadas(self, uri_gerada):
        # Uma password com "@" partia a URI: o pymongo lia o texto a seguir
        # ao primeiro "@" como host. O sintoma era "Authentication failed",
        # que aponta para o lado errado do problema.
        uri = uri_gerada({**BASE, "user": "admin$user", "password": "p@ss:w/rd#123"})

        assert "p%40ss%3Aw%2Frd%23123" in uri
        assert "admin%24user" in uri
        # O único "@" restante é o separador credenciais/host.
        assert uri.count("@") == 1

    def test_auth_source_vem_do_campo_service(self, uri_gerada):
        # Utilizadores criados na própria base (não em "admin") falhavam
        # sempre com code 18, porque authSource estava fixo em "admin".
        uri = uri_gerada(
            {**BASE, "user": "u", "password": "p", "service": "notificacoes"}
        )

        assert "authSource=notificacoes" in uri

    def test_auth_source_mantem_admin_por_omissao(self, uri_gerada):
        # Não quebra quem já dependia do comportamento anterior.
        uri = uri_gerada({**BASE, "user": "u", "password": "p"})

        assert "authSource=admin" in uri

    def test_sem_credenciais_nao_inclui_auth_source(self, uri_gerada):
        uri = uri_gerada({**BASE, "user": "", "password": ""})

        assert uri == "mongodb://localhost:27017/notificacoes"

    def test_user_sem_password_nao_rebenta(self, uri_gerada):
        # Caminho em que auth_source poderia ficar por definir.
        uri = uri_gerada({**BASE, "user": "u", "password": ""})

        assert uri == "mongodb://localhost:27017/notificacoes"


class TestFechoDeConexao:
    def test_dispose_num_mongoclient_rebenta(self):
        # Documenta a armadilha: o pymongo interpreta ".dispose" como o nome
        # de uma base de dados e devolve um objeto Database em vez de falhar
        # com AttributeError. Só o "()" final rebenta.
        client = MongoClient("mongodb://localhost:27017", connect=False)

        with pytest.raises(TypeError, match="not callable"):
            client.dispose()

    def test_close_engine_fecha_mongoclient(self):
        client = MongoClient("mongodb://localhost:27017", connect=False)

        close_engine(client)  # não deve levantar

    def test_close_engine_fecha_engine_sqlalchemy(self):
        from sqlalchemy import create_engine

        close_engine(create_engine("sqlite:///:memory:"))

    def test_close_engine_ignora_none(self):
        close_engine(None)
