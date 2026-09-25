"""
Inventário de autorização das rotas.

O RBAC deste projeto estava bem construído e mal aplicado: `require_permission`
existia desde sempre mas só protegia 4 dos 28 routers, e nenhum deles era um
router de dados. Este ficheiro fixa o resultado da correção, para que uma rota
nova de dados não volte a nascer aberta sem alguém reparar.

Lê o `app` já montado em vez do código-fonte: o que interessa é o que o FastAPI
resolve em tempo de execução, incluindo as dependências herdadas do router.
"""

import pytest
from fastapi.routing import APIRoute

from app.main import app


def permissoes_da_rota(route: APIRoute) -> set[str]:
    """
    Permissões exigidas por uma rota, vindas do router ou do próprio endpoint.

    `require_permission` devolve um closure; os nomes exigidos ficam guardados
    numa célula desse closure, que é de onde se leem.
    """
    encontradas: set[str] = set()
    dependant = getattr(route, "dependant", None)

    for dep in dependant.dependencies if dependant else []:
        alvo = getattr(dep, "call", None)
        if "require_permission" not in getattr(alvo, "__qualname__", ""):
            continue
        for celula in getattr(alvo, "__closure__", None) or ():
            if isinstance(celula.cell_contents, tuple):
                encontradas |= set(celula.cell_contents)

    return encontradas


def rota(caminho: str, metodo: str) -> APIRoute:
    for r in app.routes:
        if isinstance(r, APIRoute) and r.path == caminho and metodo in r.methods:
            return r
    raise AssertionError(f"rota {metodo} {caminho} não existe")


# ══════════════════════ o que tem de estar protegido ══════════════════════
# (caminho, método, permissão que tem de ser exigida)
ROTAS_DE_ESCRITA = [
    # DDL sobre a base do cliente
    ("/database/field/", "POST", "schema:manage"),
    ("/database/field/{original_column_name}", "PUT", "schema:manage"),
    ("/database/field/{table_name}/{column_name}", "DELETE", "schema:manage"),
    ("/database/table/", "POST", "schema:manage"),
    ("/database/table/{original_table_name}", "PUT", "schema:manage"),
    ("/database/table/{table_name}", "DELETE", "schema:manage"),
    ("/database/table/bulk", "DELETE", "schema:manage"),
    # alteração de linhas
    ("/exe/update_row", "POST", "data:write"),
    ("/exe/insert_row", "POST", "data:write"),
    ("/exe/auto-create", "POST", "data:write"),
    # eliminação
    ("/delete/records", "DELETE", "data:delete"),
    ("/delete/delete_all", "DELETE", "data:delete"),
    # movimentação e reposição de dados
    ("/transfer/stream", "GET", "data:transfer"),
    ("/database/restore/{connection_id}/stream", "GET", "backup:restore"),
    ("/database/restore/{connection_id}/upload", "POST", "backup:restore"),
    ("/database/jobs/restore", "POST", "backup:restore"),
    ("/database/backup/{connection_id}/stream", "GET", "backup:execute"),
]

ROTAS_DE_LEITURA = [
    ("/sql-editor/execute", "POST", "query:execute"),
    ("/exe/execute_query", "POST", "query:execute"),
    ("/history/", "GET", "query:read_history"),
    ("/history/recent-queries", "GET", "query:read_history"),
    ("/analytics/db/", "GET", "analytics:db:view"),
    # O paginador genérico expõe user, Role, RefreshToken e DBConnection.
    ("/geral/paginate", "GET", "entity:list"),
]


@pytest.mark.parametrize("caminho,metodo,permissao", ROTAS_DE_ESCRITA)
def test_rotas_que_alteram_dados_exigem_permissao(caminho, metodo, permissao):
    assert permissao in permissoes_da_rota(rota(caminho, metodo))


@pytest.mark.parametrize("caminho,metodo,permissao", ROTAS_DE_LEITURA)
def test_rotas_de_leitura_exigem_permissao(caminho, metodo, permissao):
    assert permissao in permissoes_da_rota(rota(caminho, metodo))


def test_escrita_nao_se_contenta_com_query_execute():
    """
    `query:execute` é dado a TODOS os papéis, incluindo o "user" básico.

    Uma rota que altera dados protegida apenas por ele não filtra ninguém — foi
    por isso que se acrescentaram `data:write`, `data:delete` e `schema:manage`.
    """
    for caminho, metodo, _ in ROTAS_DE_ESCRITA:
        exigidas = permissoes_da_rota(rota(caminho, metodo))
        assert exigidas, f"{metodo} {caminho} não exige permissão nenhuma"
        assert exigidas != {"query:execute"}, (
            f"{metodo} {caminho} está protegida só por query:execute, "
            "que todos os papéis têm"
        )


# ══════════════════════ o que tem de continuar aberto ══════════════════════
ROTAS_PUBLICAS = [
    ("/auth/login", "POST"),
    ("/auth/register", "POST"),
    ("/auth/refresh", "POST"),
    ("/health", "GET"),
]


@pytest.mark.parametrize("caminho,metodo", ROTAS_PUBLICAS)
def test_rotas_de_entrada_continuam_sem_permissao(caminho, metodo):
    """Exigir permissão no login tornaria impossível autenticar."""
    assert permissoes_da_rota(rota(caminho, metodo)) == set()


# ══════════════════════ coerência com o seed ══════════════════════
def test_permissoes_exigidas_existem_no_seed():
    """
    Uma rota que exige uma permissão que ninguém tem é uma rota morta.

    Este teste apanha o erro de escrita no nome — que de outra forma só daria
    sinal em produção, como um 403 que ninguém consegue explicar.
    """
    from app.seed_new import ROLES_PERMISSIONS

    semeadas = {
        p for papel in ROLES_PERMISSIONS.values() for p in papel["permissions"]
    }

    exigidas: set[str] = set()
    for r in app.routes:
        if isinstance(r, APIRoute):
            exigidas |= permissoes_da_rota(r)

    orfas = exigidas - semeadas
    assert not orfas, f"permissões exigidas mas nunca atribuídas: {sorted(orfas)}"


def test_papel_basico_nao_pode_alterar_dados():
    """O papel "user" continua a ler; deixou de poder apagar uma tabela."""
    from app.seed_new import ROLES_PERMISSIONS

    basico = set(ROLES_PERMISSIONS["user"]["permissions"])

    assert "query:execute" in basico  # continua a consultar
    assert "data:write" not in basico
    assert "data:delete" not in basico
    assert "schema:manage" not in basico
    assert "data:transfer" not in basico
