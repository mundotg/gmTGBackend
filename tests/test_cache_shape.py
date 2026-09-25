"""
Regressão: forma dos dados devolvidos por funções com @cache_result.

Bug observado em produção:

    GET /conn/connections/ → 200 OK   (primeiro pedido)
    GET /conn/connections/ → 500      (pedidos seguintes)
    AttributeError: 'str' object has no attribute 'id'

Causa: `cache_result` devolve o objeto original no miss, mas o resultado
convertido por `_to_cacheable` nos hits. Uma função que devolva objetos
ORM entrega duas formas diferentes; desempacotar `for conn, last_used in
results` sobre a forma convertida itera as CHAVES do dict, pelo que `conn`
passa a ser uma string.

A defesa é as funções em cache devolverem sempre dados simples.
"""

from datetime import datetime

from app.config.cache_manager import _to_cacheable


class RowFalsa:
    """Imita a Row (DBConnection, last_used) devolvida pelo SQLAlchemy."""

    def __init__(self):
        self._mapping = {"DBConnection": object(), "last_used": datetime(2026, 1, 1)}


def test_forma_orm_diverge_entre_miss_e_hit():
    # Documenta a armadilha: é isto que não pode acontecer.
    original = {"results": [RowFalsa()]}
    convertido = _to_cacheable(original)

    conn, _last_used = convertido["results"][0]

    assert isinstance(conn, str)
    assert not hasattr(conn, "id")


def test_dicts_normalizados_sobrevivem_a_conversao():
    # A forma que as funções em cache devem devolver: idêntica antes e
    # depois de passar pelo cache.
    normalizado = {
        "page": 1,
        "limit": 10,
        "total": 1,
        "results": [
            {
                "id": 1,
                "name": "producao",
                "host": "v2.abcdef",
                "database": "vendas",
                "type": "PostgreSQL",
                "status": "available",
                # ISO string, não datetime: _to_cacheable converteria o
                # datetime e as formas voltariam a divergir.
                "last_used": "2026-01-01T00:00:00",
            }
        ],
    }

    assert _to_cacheable(normalizado) == normalizado


def test_datetime_cru_faria_divergir():
    # Justifica o .isoformat() nas funções em cache.
    com_datetime = {"last_used": datetime(2026, 1, 1)}

    assert _to_cacheable(com_datetime) != com_datetime
