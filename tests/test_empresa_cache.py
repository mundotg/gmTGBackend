import pytest
from unittest.mock import MagicMock
from app.schemas.users_schemas import EmpresaSchema
from app.routes.empresa_routes import get_empresas_paginadas_cached, invalidate_empresas_cache


from app.models import user_model


def test_empresa_schema_exposes_both_nome_and_company():
    e1 = EmpresaSchema.model_validate({"id": 1, "company": "Empresa Alfa"})
    assert e1.nome == "Empresa Alfa"
    assert e1.company == "Empresa Alfa"
    dump = e1.model_dump()
    assert dump["nome"] == "Empresa Alfa"
    assert dump["company"] == "Empresa Alfa"

    e2 = EmpresaSchema.model_validate({"id": 2, "nome": "Empresa Beta"})
    assert e2.nome == "Empresa Beta"
    assert e2.company == "Empresa Beta"


def test_get_empresas_paginadas_cached_and_invalidated():
    chamadas = {"count": 0}

    class FakeEmpresa:
        id = 1
        nome = "Test Corp"
        tamanho = "1-10"
        nif = "123456789"
        endereco = "Rua Teste"
        is_active = True
        criado_em = None

    class FakeEmpresaQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def offset(self, *args, **kwargs):
            return self

        def limit(self, *args, **kwargs):
            return self

        def count(self):
            chamadas["count"] += 1
            return 1

        def all(self):
            return [FakeEmpresa()]

    class FakeCountQuery:
        def filter(self, *args, **kwargs):
            return self

        def group_by(self, *args, **kwargs):
            return self

        def all(self):
            return [(1, 3)]

    def query_router(*args):
        if len(args) == 1 and args[0] is user_model.Empresa:
            return FakeEmpresaQuery()
        return FakeCountQuery()

    mock_db = MagicMock()
    mock_db.query.side_effect = query_router

    # Limpeza prévia para garantir estado limpo independente de testes anteriores
    invalidate_empresas_cache()

    # 1. Primeira chamada (miss)
    res1 = get_empresas_paginadas_cached(
        user_id=1,
        is_superadmin=True,
        actor_empresa_id=None,
        can_write=True,
        busca=None,
        status_filtro="todas",
        page=1,
        page_size=10,
        db=mock_db,
    )
    assert res1["total"] == 1
    assert res1["items"][0]["nome"] == "Test Corp"
    assert res1["items"][0]["company"] == "Test Corp"
    assert chamadas["count"] == 1

    # 2. Segunda chamada com os mesmos parâmetros (hit - não deve incrementar count)
    res2 = get_empresas_paginadas_cached(
        user_id=1,
        is_superadmin=True,
        actor_empresa_id=None,
        can_write=True,
        busca=None,
        status_filtro="todas",
        page=1,
        page_size=10,
        db=mock_db,
    )
    assert res2["total"] == 1
    assert chamadas["count"] == 1

    # 3. Invalidação do cache
    invalidate_empresas_cache()

    # 4. Terceira chamada (miss após invalidação)
    res3 = get_empresas_paginadas_cached(
        user_id=1,
        is_superadmin=True,
        actor_empresa_id=None,
        can_write=True,
        busca=None,
        status_filtro="todas",
        page=1,
        page_size=10,
        db=mock_db,
    )
    assert res3["total"] == 1
    assert chamadas["count"] == 2
