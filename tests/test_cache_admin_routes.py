"""
Testes unitários e de integração para a Gestão de Cache (/system/cache).

Cobre:
- Pesquisa e listagem paginada de chaves de cache (Redis e RAM)
- Inspecionar e obter detalhe de uma chave
- Editar valor e TTL de uma chave
- Eliminar uma chave específica
- Eliminar chaves em lote
- Limpar todo o cache do sistema (/system/cache/clear-all)
- Estatísticas do cache (/system/cache/stats)
"""

import base64
import os
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.config.cache_manager import MEMORY_CACHE
from app.routes.cache_admin_routes import router
from app.ultils.permissions import get_current_user, require_permission


@pytest.fixture
def client():
    app = FastAPI()
    mock_admin = MagicMock()
    mock_admin.id = 1
    mock_admin.email = "admin@mustainf.com"
    mock_admin.permissions = {"settings:system", "admin:*"}
    mock_admin.is_active = True

    # Override tanto da dependência direta de router quanto de get_current_user
    for dep in router.dependencies:
        app.dependency_overrides[dep.dependency] = lambda: mock_admin
    app.dependency_overrides[get_current_user] = lambda: mock_admin

    app.include_router(router)
    return TestClient(app)


@pytest.fixture(autouse=True)
def limpar_memoria():
    MEMORY_CACHE.clear()
    yield
    MEMORY_CACHE.clear()


# ══════════════════════════ Testes de Estatísticas ══════════════════════════

def test_obter_estatisticas(client):
    MEMORY_CACHE["cache:teste:1"] = {"ts": time.time(), "val": {"hello": "world"}, "ttl": 60}
    res = client.get("/system/cache/stats")
    assert res.status_code == 200
    dados = res.json()
    assert "total_keys" in dados
    assert "memory_keys" in dados
    assert dados["memory_keys"] >= 1


# ══════════════════════════ Testes de Listagem e Pesquisa ══════════════════════════

def test_listar_e_pesquisar_chaves(client):
    # Insere chaves na memória
    MEMORY_CACHE["cache:usuario:10"] = {"ts": time.time(), "val": {"nome": "Maria"}, "ttl": 120}
    MEMORY_CACHE["cache:empresa:20"] = {"ts": time.time(), "val": ["Empresa A", "Empresa B"], "ttl": 300}
    MEMORY_CACHE["cache:config:geral"] = {"ts": time.time(), "val": "ativo", "ttl": 600}

    # 1. Listagem completa por tipo memória
    res = client.get("/system/cache/keys?tipo=memory")
    assert res.status_code == 200
    dados = res.json()
    assert dados["total"] == 3
    chaves = [i["key"] for i in dados["items"]]
    assert "cache:usuario:10" in chaves
    assert "cache:empresa:20" in chaves

    # 2. Pesquisa por filtro (substring específica)
    res_busca = client.get("/system/cache/keys?search=cache:empresa:20")
    assert res_busca.status_code == 200
    dados_busca = res_busca.json()
    assert dados_busca["total"] == 1
    assert dados_busca["items"][0]["key"] == "cache:empresa:20"
    assert dados_busca["items"][0]["tipo"] == "list"

    # 3. Pesquisa sem resultados
    res_vazia = client.get("/system/cache/keys?search=inexistente")
    assert res_vazia.status_code == 200
    assert res_vazia.json()["total"] == 0


# ══════════════════════════ Testes de Detalhe e 404 ══════════════════════════

def test_detalhe_chave_existente_e_inexistente(client):
    MEMORY_CACHE["cache:detalhe:teste"] = {
        "ts": time.time(),
        "val": {"chave": "valor_importante", "num": 42},
        "ttl": 180,
    }

    # 1. Chave existente
    res = client.get("/system/cache/keys/cache:detalhe:teste")
    assert res.status_code == 200
    dados = res.json()
    assert dados["key"] == "cache:detalhe:teste"
    assert dados["source"] == "memory"
    assert dados["value"]["chave"] == "valor_importante"
    assert dados["value"]["num"] == 42
    assert dados["ttl"] is not None

    # 2. Chave inexistente retorna 404
    res_404 = client.get("/system/cache/keys/chave:que:nao:existe")
    assert res_404.status_code == 404


# ══════════════════════════ Testes de Edição (PUT) ══════════════════════════

def test_editar_chave_de_cache(client):
    MEMORY_CACHE["cache:config:preco"] = {"ts": time.time(), "val": 100, "ttl": 60}

    # Edita o valor de 100 para 250 e altera o TTL para 500
    payload = {"value": 250, "ttl": 500}
    res_put = client.put("/system/cache/keys/cache:config:preco", json=payload)
    assert res_put.status_code == 200
    assert res_put.json()["success"] is True

    # Valida alteração
    assert MEMORY_CACHE["cache:config:preco"]["val"] == 250
    assert MEMORY_CACHE["cache:config:preco"]["ttl"] == 500

    # Edita com objeto estruturado JSON
    payload_obj = {"value": {"desconto": 15, "moeda": "EUR"}, "ttl": 300}
    res_put2 = client.put("/system/cache/keys/cache:config:preco", json=payload_obj)
    assert res_put2.status_code == 200
    assert MEMORY_CACHE["cache:config:preco"]["val"]["desconto"] == 15


# ══════════════════════════ Testes de Eliminação (DELETE) ══════════════════════════

def test_eliminar_chave_individual(client):
    MEMORY_CACHE["cache:para:eliminar"] = {"ts": time.time(), "val": "temporario", "ttl": 60}
    assert "cache:para:eliminar" in MEMORY_CACHE

    res = client.delete("/system/cache/keys/cache:para:eliminar")
    assert res.status_code == 200
    assert res.json()["success"] is True
    assert "cache:para:eliminar" not in MEMORY_CACHE


def test_eliminar_chaves_em_lote(client):
    MEMORY_CACHE["k1"] = {"ts": time.time(), "val": 1}
    MEMORY_CACHE["k2"] = {"ts": time.time(), "val": 2}
    MEMORY_CACHE["k3"] = {"ts": time.time(), "val": 3}

    res = client.post("/system/cache/keys/delete-bulk", json={"keys": ["k1", "k2"]})
    assert res.status_code == 200
    assert res.json()["removed"] == 2
    assert "k1" not in MEMORY_CACHE
    assert "k2" not in MEMORY_CACHE
    assert "k3" in MEMORY_CACHE


def test_limpar_todo_o_cache(client):
    MEMORY_CACHE["k1"] = {"ts": time.time(), "val": 1}
    MEMORY_CACHE["k2"] = {"ts": time.time(), "val": 2}

    with patch("app.routes.cache_admin_routes.clear_all_cache", return_value=5):
        res = client.post("/system/cache/clear-all")
        assert res.status_code == 200
        dados = res.json()
        assert dados["removidos_memoria"] == 2
        assert dados["removidos_redis"] == 5
        assert len(MEMORY_CACHE) == 0


# ══════════════════════════ Testes de Exploração por Utilizador ══════════════════════════

from types import SimpleNamespace

from app.database import get_db
from app.routes.cache_admin_routes import _extrair_user_id_de_chave_ou_dado


@pytest.fixture
def client_com_db(client):
    """Cliente com uma sessão de BD falsa que conhece os utilizadores 10 e 20."""
    utilizadores = [
        SimpleNamespace(id=10, nome="Maria", email="maria@mustainf.com"),
        SimpleNamespace(id=20, nome="João", email="joao@mustainf.com"),
    ]
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = utilizadores
    db.query.return_value.filter.return_value.first.return_value = utilizadores[0]
    client.app.dependency_overrides[get_db] = lambda: db
    return client


def _semear_caches_de_utilizadores():
    agora = time.time()
    # Entradas do decorador: o dono vem no campo user_id (chave é um hash)
    MEMORY_CACHE["abc123"] = {"ts": agora, "val": {"x": 1}, "ttl": 60, "user_id": 10, "function": "listar"}
    MEMORY_CACHE["def456"] = {"ts": agora, "val": {"x": 2}, "ttl": 60, "user_id": 20, "function": "listar"}
    # Chave legada com o id no nome
    MEMORY_CACHE["cachepolicy:local:user:10"] = {"ts": agora, "val": True, "ttl": 60}
    # Entrada global (sem dono)
    MEMORY_CACHE["cache:config:geral"] = {"ts": agora, "val": "ativo", "ttl": 60}


def test_extrair_user_id_ignora_falsos_positivos():
    assert _extrair_user_id_de_chave_ou_dado("cache:menu:5") is None
    assert _extrair_user_id_de_chave_ou_dado("cachepolicy:gen:user:7") == 7
    assert _extrair_user_id_de_chave_ou_dado("sess:user_id:42") == 42
    assert _extrair_user_id_de_chave_ou_dado("x", {"value": 1, "function": "f", "user_id": 3}) == 3


def test_listar_mostra_dono_de_cada_chave(client_com_db):
    _semear_caches_de_utilizadores()
    with patch("app.routes.cache_admin_routes.redis_client", None):
        res = client_com_db.get("/system/cache/keys?tipo=memory&limit=50")
    assert res.status_code == 200
    por_chave = {i["key"]: i for i in res.json()["items"]}

    assert por_chave["abc123"]["user_id"] == 10
    assert por_chave["abc123"]["user_nome"] == "Maria"
    assert por_chave["def456"]["user_id"] == 20
    assert por_chave["cachepolicy:local:user:10"]["user_id"] == 10
    assert por_chave["cache:config:geral"]["user_id"] is None


def test_filtrar_chaves_por_utilizador(client_com_db):
    _semear_caches_de_utilizadores()
    with patch("app.routes.cache_admin_routes.redis_client", None):
        res = client_com_db.get("/system/cache/keys?tipo=memory&user_id=10")
    assert res.status_code == 200
    dados = res.json()
    chaves = {i["key"] for i in dados["items"]}
    assert dados["total"] == 2
    assert chaves == {"abc123", "cachepolicy:local:user:10"}


def test_endpoint_chaves_do_utilizador(client_com_db):
    _semear_caches_de_utilizadores()
    with patch("app.routes.cache_admin_routes.redis_client", None):
        res = client_com_db.get("/system/cache/users/20/keys?tipo=memory")
    assert res.status_code == 200
    assert {i["key"] for i in res.json()["items"]} == {"def456"}


def test_detalhe_inclui_dono(client_com_db):
    _semear_caches_de_utilizadores()
    with patch("app.routes.cache_admin_routes.redis_client", None):
        res = client_com_db.get("/system/cache/keys/abc123")
    assert res.status_code == 200
    dados = res.json()
    assert dados["user_id"] == 10
    assert dados["user_nome"] == "Maria"
