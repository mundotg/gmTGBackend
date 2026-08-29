"""
Testes do nível de acesso a conexões (read / write / manage).

Cobrem as duas metades da correção:

1. `requires_write` — decide se um lote de SQL do editor precisa de nível de
   escrita. É o que impede que exigir `write` no editor passe a bloquear os
   SELECTs de quem tem partilha de leitura.
2. A resolução e imposição do nível em si, contra uma base SQLite real, com
   dono, convidado de leitura, convidado de escrita e estranho.
"""

import base64
import os

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ.setdefault("ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode())

from app.database import Base  # noqa: E402
from app.models.connection_models import (  # noqa: E402
    ActiveConnection,
    DBConnection,
    DBConnectionShare,
)
from app.models.user_model import Permission, Role, User  # noqa: E402
from app.routes.sql_editor_routes import requires_write  # noqa: E402
from app.schemas.connetion_schema import ConnectionAccessLevel  # noqa: E402
from app.ultils.connection_access import (  # noqa: E402
    assert_user_connection_level,
    resolve_access_level,
)

READ = ConnectionAccessLevel.read
WRITE = ConnectionAccessLevel.write
MANAGE = ConnectionAccessLevel.manage


# ══════════════════════════ classificação do SQL ══════════════════════════
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM clientes",
        "select id, nome from clientes where ativo = 1",
        "  \n  SELECT 1  ",
        "WITH recentes AS (SELECT * FROM vendas) SELECT * FROM recentes",
        "SHOW TABLES",
        "EXPLAIN SELECT * FROM clientes",
        "SELECT * FROM clientes; SELECT * FROM vendas",
        # A palavra perigosa está dentro de um literal, não é um comando.
        "SELECT * FROM tarefas WHERE estado = 'DELETE'",
        "SELECT * FROM logs WHERE msg = 'DROP TABLE clientes'",
        # Comentários não contam como comandos.
        "SELECT * FROM clientes -- DELETE FROM clientes",
        "/* UPDATE clientes SET x=1 */ SELECT 1",
        # Nome de coluna que contém uma palavra-chave como prefixo.
        "SELECT update_at, create_date FROM clientes",
    ],
)
def test_leitura_nao_exige_escrita(sql):
    assert requires_write(sql) is False


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM clientes",
        "delete from clientes where id = 1",
        "UPDATE clientes SET nome = 'x'",
        "INSERT INTO clientes (nome) VALUES ('x')",
        "DROP TABLE clientes",
        "ALTER TABLE clientes ADD COLUMN idade INT",
        "TRUNCATE TABLE clientes",
        "CREATE TABLE novo (id INT)",
        "GRANT SELECT ON clientes TO leitor",
        # Escrita escondida no meio de um lote que começa por leitura.
        "SELECT * FROM clientes; DELETE FROM clientes",
        # CTE que escreve — o verbo inicial é WITH, mas o efeito é apagar.
        "WITH removidos AS (DELETE FROM vendas RETURNING *) SELECT * FROM removidos",
        # SELECT ... INTO cria tabela nova.
        "SELECT * INTO copia_clientes FROM clientes",
    ],
)
def test_escrita_exige_nivel_write(sql):
    assert requires_write(sql) is True


def test_explain_analyze_disfarcado_conta_como_escrita():
    """
    `/explain` cola `EXPLAIN` ao texto do utilizador: escrever `ANALYZE DELETE
    FROM x` produz `EXPLAIN ANALYZE DELETE FROM x`, que em PostgreSQL executa
    mesmo o DELETE.

    Qualquer `ANALYZE` conta como escrita, incluindo `ANALYZE SELECT`: a rota já
    põe o `EXPLAIN` por sua conta, portanto quem escreve `ANALYZE` à mão está a
    tentar exatamente esta injeção. Recusar por excesso é o lado certo do erro.
    """
    assert requires_write("ANALYZE DELETE FROM clientes") is True
    assert requires_write("ANALYZE SELECT * FROM clientes") is True

    # O uso normal — a rota recebe só a query e prefixa o EXPLAIN — continua a ler.
    assert requires_write("SELECT * FROM clientes") is False


# ══════════════════════════ resolução do nível ══════════════════════════
@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessao = sessionmaker(bind=engine)()
    try:
        yield sessao
    finally:
        sessao.close()


def _criar_user(db, email, *, plan_id=1, permissoes=()):
    role = None
    if permissoes:
        role = Role(name=f"papel-{email}")
        role.permissions = [Permission(name=p) for p in permissoes]
        db.add(role)
        db.flush()

    user = User(
        nome=email,
        email=email,
        hashed_password="x",
        is_active=True,
        plan_id=plan_id,
        role_id=role.id if role else None,
    )
    db.add(user)
    db.flush()
    return user


@pytest.fixture
def cenario(db):
    """Dono, convidado de leitura, convidado de escrita, estranho e super admin."""
    from app.models.clouds_models import Plan

    db.add(Plan(id=1, name="teste", max_storage_mb=10, max_requests_per_day=10))
    db.flush()

    dono = _criar_user(db, "dono@x.pt")
    leitor = _criar_user(db, "leitor@x.pt")
    escritor = _criar_user(db, "escritor@x.pt")
    estranho = _criar_user(db, "estranho@x.pt")
    admin = _criar_user(db, "admin@x.pt", permissoes=("admin:*",))

    conn = DBConnection(
        user_id=dono.id,
        name="producao",
        type="PostgreSQL",
        host="h",
        port=5432,
        username="u",
        password="p",
        database_name="d",
    )
    db.add(conn)
    db.flush()

    db.add(DBConnectionShare(connection_id=conn.id, user_id=leitor.id, access_level="read"))
    db.add(DBConnectionShare(connection_id=conn.id, user_id=escritor.id, access_level="write"))
    db.commit()

    return {
        "conn": conn,
        "dono": dono,
        "leitor": leitor,
        "escritor": escritor,
        "estranho": estranho,
        "admin": admin,
    }


def test_dono_e_admin_valem_manage(db, cenario):
    assert resolve_access_level(db, cenario["conn"], cenario["dono"]) is MANAGE
    assert resolve_access_level(db, cenario["conn"], cenario["admin"]) is MANAGE


def test_nivel_vem_da_partilha(db, cenario):
    assert resolve_access_level(db, cenario["conn"], cenario["leitor"]) is READ
    assert resolve_access_level(db, cenario["conn"], cenario["escritor"]) is WRITE


def test_estranho_nao_tem_nivel(db, cenario):
    assert resolve_access_level(db, cenario["conn"], cenario["estranho"]) is None


def test_leitor_pode_ler_mas_nao_escrever(db, cenario):
    conn, leitor = cenario["conn"], cenario["leitor"]

    # Ler é o que a partilha de leitura promete: tem de passar.
    assert assert_user_connection_level(db, conn, leitor.id, READ) is READ

    # Escrever é a falha que isto vem fechar.
    with pytest.raises(HTTPException) as erro:
        assert_user_connection_level(db, conn, leitor.id, WRITE)
    assert erro.value.status_code == 403


def test_escritor_escreve_mas_nao_gere(db, cenario):
    conn, escritor = cenario["conn"], cenario["escritor"]

    assert assert_user_connection_level(db, conn, escritor.id, WRITE) is WRITE

    with pytest.raises(HTTPException) as erro:
        assert_user_connection_level(db, conn, escritor.id, MANAGE)
    assert erro.value.status_code == 403


def test_estranho_nao_passa_nem_a_leitura(db, cenario):
    with pytest.raises(HTTPException) as erro:
        assert_user_connection_level(db, cenario["conn"], cenario["estranho"].id, READ)
    assert erro.value.status_code == 403


# ══════════════════════════ conexão ativa por utilizador ══════════════════════════
def test_dois_utilizadores_ligados_a_mesma_conexao(db, cenario):
    """
    O estado de ligação é de quem se liga, não da conexão.

    Com a chave antiga (só `connection_id`) a segunda ligação sobrepunha-se à
    primeira e o dono era silenciosamente desligado.
    """
    from app.cruds.connection_cruds import set_active_connection

    conn, dono, escritor = cenario["conn"], cenario["dono"], cenario["escritor"]

    set_active_connection(db, dono.id, conn.id)
    set_active_connection(db, escritor.id, conn.id)

    ativos = (
        db.query(ActiveConnection)
        .filter(ActiveConnection.connection_id == conn.id)
        .all()
    )
    assert {(a.user_id, a.status) for a in ativos} == {(dono.id, True), (escritor.id, True)}


def test_desligar_um_nao_desliga_o_outro(db, cenario):
    from app.cruds.connection_cruds import (
        desactivate_all_connections,
        set_active_connection,
    )

    conn, dono, escritor = cenario["conn"], cenario["dono"], cenario["escritor"]
    set_active_connection(db, dono.id, conn.id)
    set_active_connection(db, escritor.id, conn.id)

    desactivate_all_connections(db, dono.id)

    estados = dict(
        db.query(ActiveConnection.user_id, ActiveConnection.status)
        .filter(ActiveConnection.connection_id == conn.id)
        .all()
    )
    assert estados[dono.id] is False
    assert estados[escritor.id] is True


def test_convidado_ve_a_conexao_partilhada_como_atual(db, cenario):
    """É isto que fazia a partilha não servir para nada: sem esta ligação, a
    conexão de outra pessoa nunca chegava a ser a conexão atual do convidado."""
    from app.cruds.connection_cruds import set_active_connection
    from app.ultils.ativar_session_bd import get_connection_current

    conn, escritor = cenario["conn"], cenario["escritor"]
    set_active_connection(db, escritor.id, conn.id)

    atual, activated_at = get_connection_current(db, escritor.id)
    assert atual is not None and atual.id == conn.id
    assert activated_at is not None


def test_estranho_continua_sem_conexao_atual(db, cenario):
    """Ligar-se é bloqueado antes disto, mas o estado também não aparece sozinho."""
    from app.ultils.ativar_session_bd import get_connection_current

    atual, _ = get_connection_current(db, cenario["estranho"].id)
    assert atual is None


def test_conta_desativada_nao_executa(db, cenario):
    """O token continua válido depois de desativar a conta — o nível não."""
    dono = cenario["dono"]
    dono.is_active = False
    db.commit()

    with pytest.raises(HTTPException) as erro:
        assert_user_connection_level(db, cenario["conn"], dono.id, READ)
    assert erro.value.status_code == 403
