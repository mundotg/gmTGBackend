"""
Regressão: introspeção de MongoDB.

Sintomas reportados:

    GET /consu/structures → {"success": true, "data": []}
    GET /consu/sync       → todos os contadores a 0,
                            server_version "Desconhecida"

Duas causas independentes:

1. `sync_connection_statistics` passava o ID da conexão onde
   `collect_statistics` espera o TIPO do banco. O ramo MongoDB nunca era
   escolhido e um MongoClient acabava no caminho SQL.

2. Toda a introspeção assumia SQLAlchemy: `inspect(engine)` e
   `engine.dialect` rebentam sobre um MongoClient, e o except devolvia
   lista vazia.
"""

import pytest
from pymongo import MongoClient
from sqlalchemy import create_engine

from app.services import database_inspector
from app.services.database_inspector import (
    _list_mongo_collections,
    collect_statistics,
    is_mongo,
    safe_inspect,
)


class ColecaoFalsa:
    def __init__(self, indices=1):
        self._indices = indices

    def index_information(self):
        return {f"idx_{i}": {} for i in range(self._indices)}

    def count_documents(self, _filtro):
        return 42


class BaseFalsa:
    """Imita pymongo.database.Database no que a introspeção usa."""

    name = "notificacoes"

    def __init__(self, colecoes=None):
        self._colecoes = colecoes or [
            {"name": "mensagens", "type": "collection"},
            {"name": "utilizadores", "type": "collection"},
            {"name": "resumo_diario", "type": "view"},
        ]

    def list_collections(self):
        return iter(self._colecoes)

    def __getitem__(self, _nome):
        return ColecaoFalsa(indices=2)


@pytest.fixture
def mongo(monkeypatch):
    """MongoClient real (sem ligar) com a base substituída por uma falsa."""
    client = MongoClient("mongodb://localhost:27017/notificacoes", connect=False)

    monkeypatch.setattr(
        database_inspector, "get_mongo_database", lambda _engine: BaseFalsa()
    )
    monkeypatch.setattr(
        MongoClient, "server_info", lambda self: {"version": "7.0.5"}
    )

    return client


class TestDeteccao:
    def test_distingue_mongo_de_sqlalchemy(self):
        assert is_mongo(MongoClient("mongodb://h:1/d", connect=False))
        assert not is_mongo(create_engine("sqlite:///:memory:"))

    def test_safe_inspect_devolve_none_para_mongo(self):
        # inspect() levantaria exceção e o chamador devolvia [].
        assert safe_inspect(MongoClient("mongodb://h:1/d", connect=False)) is None

    def test_safe_inspect_continua_a_funcionar_para_sql(self):
        assert safe_inspect(create_engine("sqlite:///:memory:")) is not None


class TestListagemDeColecoes:
    def test_coleccoes_e_views_com_a_forma_do_caminho_sql(self, mongo):
        items = _list_mongo_collections(mongo)

        # (nome, schema, tipo) — a base faz de schema
        assert ("mensagens", "notificacoes", "table") in items
        assert ("resumo_diario", "notificacoes", "view") in items
        assert len(items) == 3

    def test_lista_vazia_quando_nao_ha_base(self, monkeypatch):
        monkeypatch.setattr(
            database_inspector, "get_mongo_database", lambda _e: None
        )

        assert _list_mongo_collections(object()) == []

    def test_list_tables_and_views_encaminha_para_mongo(self, mongo):
        # O ponto que antes rebentava: `engine.dialect` não existe no Mongo.
        items = database_inspector._list_tables_and_views(mongo, None)

        assert len(items) == 3


class TestEstatisticas:
    def test_tipo_correto_produz_contagens_reais(self, mongo):
        stats = collect_statistics(mongo, "MongoDB")

        assert stats["server_version"] == "7.0.5"
        assert stats["table_count"] == 2  # views não contam como tabelas
        assert stats["view_count"] == 1
        assert stats["tables_connected"] == 2
        assert stats["index_count"] == 4  # 2 coleções × 2 índices

    def test_tipo_errado_reproduz_o_bug_original(self, mongo):
        # Era isto que acontecia: passava-se str(id_conn), ex. "1".
        stats = collect_statistics(mongo, "1")

        assert stats["table_count"] == 0
        assert stats["server_version"] == "Desconhecida"

    def test_maiusculas_do_tipo_sao_irrelevantes(self, mongo):
        assert collect_statistics(mongo, "mongodb")["table_count"] == 2
