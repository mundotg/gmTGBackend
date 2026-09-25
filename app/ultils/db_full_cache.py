"""Cache do diagrama de schema servido por `/conn/db_full/{conn_id}`.

A chave é por **conexão**, não por utilizador: o conteúdo é igual para todos os
que lhe têm acesso (o controlo de acesso é feito antes de ler a cache). Com a
chave antiga `dbfull:{user}:{conn}` era impossível invalidar a entrada dos
outros utilizadores quando o schema mudava.

Vive num módulo próprio porque é usado pelas rotas de conexão *e* pelas de DDL —
importar uma da outra criaria um ciclo.
"""

from app.config.redis import delete_cache

# O schema muda pouco, e as alterações feitas pela app invalidam a entrada.
DB_FULL_TTL = 600


def db_full_cache_key(conn_id) -> str:
    return f"dbfull:{conn_id}"


def invalidate_db_full_cache(conn_id) -> None:
    """Apaga o diagrama em cache de uma conexão (após DDL, por exemplo)."""
    if conn_id is None:
        return
    delete_cache(db_full_cache_key(conn_id))
