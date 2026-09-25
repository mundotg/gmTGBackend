"""
Máquina de estados da `Operation`.

O valor desta entidade está inteiro na palavra "estrita": se houver um atalho de
`draft` para `executed`, ou se quem pede puder aprovar-se a si próprio, a
governança volta a ser uma frase de apresentação. Estes testes tentam
precisamente esses atalhos.
"""

import base64
import os
from datetime import timedelta

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.database import Base  # noqa: E402
from app.models.clouds_models import Plan  # noqa: E402
from app.models.connection_models import DBConnection  # noqa: E402
from app.models.operation_models import (  # noqa: E402
    ESTADOS_TERMINAIS,
    TRANSICOES,
    Operation,
    OperationKind,
    OperationStatus,
)
from app.models.task_models import Project  # noqa: E402
from app.models.user_model import User  # noqa: E402
from app.services import operation_service as svc  # noqa: E402

S = OperationStatus


# ══════════════════════════ a máquina, sem base de dados ══════════════════════════
def test_todos_os_estados_estao_na_tabela_de_transicoes():
    """Um estado esquecido em `TRANSICOES` seria um beco sem saída silencioso."""
    assert set(TRANSICOES) == set(OperationStatus)


def test_transicoes_apontam_sempre_para_estados_reais():
    for origem, destinos in TRANSICOES.items():
        for destino in destinos:
            assert isinstance(destino, OperationStatus), (origem, destino)


def test_estados_finais_sao_os_esperados():
    assert ESTADOS_TERMINAIS == {
        S.rejected,
        S.rolled_back,
        S.cancelled,
        S.expired,
    }


def test_nao_ha_caminho_directo_de_rascunho_para_executada():
    """O atalho que tornaria a governança decorativa."""
    assert S.executed not in TRANSICOES[S.draft]
    assert S.executing not in TRANSICOES[S.draft]
    assert S.approved not in TRANSICOES[S.draft]


def test_executar_exige_passar_por_aprovacao():
    """Só se chega a `executing` vindo de `approved`."""
    origens = {o for o, destinos in TRANSICOES.items() if S.executing in destinos}
    assert origens == {S.approved}


def test_estado_executado_so_se_alcanca_a_partir_de_execucao():
    origens = {o for o, destinos in TRANSICOES.items() if S.executed in destinos}
    assert origens == {S.executing}


# ══════════════════════════ com base de dados ══════════════════════════
@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessao = sessionmaker(bind=engine)()
    try:
        yield sessao
    finally:
        sessao.close()


@pytest.fixture
def cenario(db):
    db.add(Plan(id=1, name="teste", max_storage_mb=10, max_requests_per_day=10))
    db.flush()

    autor = User(nome="Dev", email="dev@x.pt", hashed_password="x", plan_id=1)
    revisor = User(nome="Lead", email="lead@x.pt", hashed_password="x", plan_id=1)
    db.add_all([autor, revisor])
    db.flush()

    conn = DBConnection(
        user_id=autor.id, name="producao", type="PostgreSQL", host="h",
        port=5432, username="u", password="p", database_name="d",
    )
    projeto = Project(name="pipeline de migração", owner_id=autor.id)
    db.add_all([conn, projeto])
    db.commit()

    return {"autor": autor, "revisor": revisor, "conn": conn, "projeto": projeto}


def nova_operacao(db, cenario, **kw):
    return svc.criar_operacao(
        db,
        requerente=cenario["autor"],
        project_id=cenario["projeto"].id,
        connection_id=cenario["conn"].id,
        title="apagar clientes inativos",
        kind=OperationKind.dml,
        payload="DELETE FROM clientes WHERE ativo = false",
        **kw,
    )


def test_nasce_em_rascunho_e_ligada_ao_contexto(db, cenario):
    op = nova_operacao(db, cenario)

    assert OperationStatus(op.status) is S.draft
    assert op.project_id == cenario["projeto"].id      # o pipeline
    assert op.connection_id == cenario["conn"].id      # o alvo
    assert op.requested_by_id == cenario["autor"].id
    assert op.uuid and len(op.uuid) == 36
    assert op.esta_executavel is False


def test_caminho_feliz_completo(db, cenario):
    op = nova_operacao(db, cenario)

    svc.submeter(db, op, cenario["autor"])
    assert OperationStatus(op.status) is S.pending_review

    svc.aprovar(db, op, cenario["revisor"])
    assert OperationStatus(op.status) is S.approved
    assert op.reviewed_by_id == cenario["revisor"].id
    assert op.expires_at is not None
    assert op.esta_executavel is True

    svc.iniciar_execucao(db, op, cenario["revisor"])
    assert OperationStatus(op.status) is S.executing

    svc.concluir_execucao(db, op, rows_affected=42, duration_ms=130)
    assert OperationStatus(op.status) is S.executed
    assert op.rows_affected == 42
    assert op.esta_terminada is False   # ainda pode ser revertida


def test_rascunho_nao_salta_para_execucao(db, cenario):
    op = nova_operacao(db, cenario)

    with pytest.raises(HTTPException) as erro:
        svc.transitar(db, op, S.executing)
    assert erro.value.status_code == 409
    assert "Transição inválida" in erro.value.detail
    assert OperationStatus(op.status) is S.draft


def test_operacao_por_rever_nao_e_executavel(db, cenario):
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])

    with pytest.raises(HTTPException) as erro:
        svc.assert_executavel(db, op)
    assert erro.value.status_code == 403


def test_quem_pede_nao_pode_aprovar(db, cenario):
    """Separação de funções: é o primeiro controlo que um auditor procura."""
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])

    with pytest.raises(HTTPException) as erro:
        svc.aprovar(db, op, cenario["autor"])
    assert erro.value.status_code == 403

    assert OperationStatus(op.status) is S.pending_review


def test_rejeitada_e_final(db, cenario):
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])
    svc.rejeitar(db, op, cenario["revisor"], comentario="faltam índices")

    assert op.esta_terminada is True
    assert op.review_comment == "faltam índices"

    for destino in OperationStatus:
        if destino is S.rejected:
            continue
        with pytest.raises(HTTPException):
            svc.transitar(db, op, destino)


def test_aprovacao_expirada_nao_executa(db, cenario):
    """Aprovar na terça não deve autorizar a execução um mês depois."""
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])
    svc.aprovar(db, op, cenario["revisor"], validade=timedelta(seconds=-1))

    assert op.esta_expirada is True
    assert op.esta_executavel is False

    with pytest.raises(HTTPException) as erro:
        svc.assert_executavel(db, op)
    assert erro.value.status_code == 403
    # e o estado passa a refletir isso, em vez de ficar "aprovada" para sempre
    assert OperationStatus(op.status) is S.expired


def test_falha_permite_reverter(db, cenario):
    """Falhar a meio é justamente quando reverter costuma ser preciso."""
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])
    svc.aprovar(db, op, cenario["revisor"])
    svc.iniciar_execucao(db, op, cenario["revisor"])
    svc.falhar_execucao(db, op, erro="deadlock detected")

    assert OperationStatus(op.status) is S.failed
    assert op.error == "deadlock detected"

    svc.marcar_revertida(db, op, cenario["revisor"])
    assert OperationStatus(op.status) is S.rolled_back
    assert op.esta_terminada is True


def test_cancelar_deixa_de_ser_possivel_depois_de_comecar(db, cenario):
    op = nova_operacao(db, cenario)
    svc.submeter(db, op, cenario["autor"])
    svc.aprovar(db, op, cenario["revisor"])
    svc.iniciar_execucao(db, op, cenario["revisor"])

    with pytest.raises(HTTPException):
        svc.cancelar(db, op, cenario["autor"])


def test_reversibilidade_vem_do_rollback_declarado(db, cenario):
    sem = nova_operacao(db, cenario)
    assert sem.is_reversible is False

    com = nova_operacao(
        db, cenario, rollback_payload="INSERT INTO clientes SELECT * FROM clientes_bkp"
    )
    assert com.is_reversible is True


def test_base_de_dados_recusa_estado_inventado(db, cenario):
    """O CHECK constraint protege contra um UPDATE feito à mão."""
    from sqlalchemy.exc import IntegrityError, StatementError

    op = nova_operacao(db, cenario)
    with pytest.raises((IntegrityError, StatementError, LookupError, ValueError)):
        db.execute(
            Operation.__table__.update()
            .where(Operation.__table__.c.id == op.id)
            .values(status="tudo_bem_pode_correr")
        )
        db.commit()
