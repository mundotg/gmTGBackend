"""
🧭 Ciclo de vida de uma `Operation`.

Todas as mudanças de estado passam por aqui. O modelo sabe QUE transições são
legais (`TRANSICOES`); este módulo sabe QUANDO e POR QUEM podem ser feitas, e é
o sítio onde a auditoria e a aprovação multi-etapas se vão pendurar.

Regra que orienta o resto: nada altera `operation.status` diretamente. Se
aparecer um `op.status = ...` fora deste ficheiro, a máquina de estados deixou
de valer, porque foi contornada.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import HTTPException, status as http
from sqlalchemy.orm import Session

from app.models.operation_models import (
    ESTADO_EXECUTAVEL,
    Operation,
    OperationKind,
    OperationRisk,
    OperationStatus,
)
from app.models.user_model import User
from app.ultils.logger import log_message

#: Quanto tempo uma aprovação continua válida, se ninguém indicar outro prazo.
VALIDADE_APROVACAO = timedelta(hours=24)


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _erro(mensagem: str, codigo: int = http.HTTP_409_CONFLICT) -> HTTPException:
    return HTTPException(status_code=codigo, detail=mensagem)


# ══════════════════════════ transição ══════════════════════════
def transitar(
    db: Session,
    op: Operation,
    destino: OperationStatus,
    *,
    ator: Optional[User] = None,
    comentario: Optional[str] = None,
    commit: bool = True,
) -> Operation:
    """
    Move a operação para `destino`, recusando qualquer salto ilegal.

    É deliberadamente a única porta: as funções abaixo (submeter, aprovar,
    executar…) chamam esta, em vez de escreverem o estado à mão.
    """
    atual = OperationStatus(op.status)

    if not op.pode_transitar_para(destino):
        permitidos = sorted(e.value for e in op.destinos_possiveis())
        raise _erro(
            f"Transição inválida: '{atual.value}' → '{destino.value}'. "
            + (
                f"A partir de '{atual.value}' só é possível ir para: {', '.join(permitidos)}."
                if permitidos
                else f"'{atual.value}' é um estado final."
            )
        )

    op.status = destino
    if comentario:
        op.review_comment = comentario

    log_message(
        f"[operation] {op.uuid} {atual.value} → {destino.value}"
        + (f" por user_id={ator.id}" if ator else ""),
        "info",
    )

    db.add(op)
    if commit:
        db.commit()
        db.refresh(op)
    return op


# ══════════════════════════ criação ══════════════════════════
def criar_operacao(
    db: Session,
    *,
    requerente: User,
    project_id: int,
    connection_id: int,
    title: str,
    kind: OperationKind,
    payload: str,
    description: Optional[str] = None,
    task_id: Optional[int] = None,
    engine_dialect: Optional[str] = None,
    impact_plan: Optional[Dict[str, Any]] = None,
    risk_level: OperationRisk = OperationRisk.medium,
    rollback_payload: Optional[str] = None,
    commit: bool = True,
) -> Operation:
    """
    Cria a operação em `draft`.

    Nasce sempre em rascunho, mesmo quando quem pede tem poder para aprovar:
    submeter e decidir são passos distintos, e é essa distinção que dá o
    registo de "quem pediu" separado de "quem autorizou".
    """
    op = Operation(
        project_id=project_id,
        connection_id=connection_id,
        task_id=task_id,
        empresa_id=requerente.empresa_id,
        title=title,
        description=description,
        kind=kind,
        engine_dialect=engine_dialect,
        payload=payload,
        impact_plan=impact_plan,
        risk_level=risk_level,
        rollback_payload=rollback_payload,
        is_reversible=bool(rollback_payload),
        status=OperationStatus.draft,
        requested_by_id=requerente.id,
    )
    db.add(op)
    if commit:
        db.commit()
        db.refresh(op)
    return op


# ══════════════════════════ passos do ciclo ══════════════════════════
def submeter(db: Session, op: Operation, ator: User, **kw) -> Operation:
    """Rascunho → em revisão."""
    op.submitted_at = _agora()
    return transitar(db, op, OperationStatus.pending_review, ator=ator, **kw)


def aprovar(
    db: Session,
    op: Operation,
    ator: User,
    *,
    comentario: Optional[str] = None,
    validade: timedelta = VALIDADE_APROVACAO,
    **kw,
) -> Operation:
    """
    Em revisão → aprovada, com prazo.

    Quem pediu não pode aprovar. É a separação de funções que um auditor
    procura primeiro, e sem ela o fluxo de aprovação é teatro: bastava a mesma
    pessoa carregar em dois botões seguidos.
    """
    if op.requested_by_id is not None and op.requested_by_id == ator.id:
        raise _erro(
            "Quem pede uma operação não a pode aprovar. "
            "Peça a revisão a outra pessoa.",
            http.HTTP_403_FORBIDDEN,
        )

    op.reviewed_by_id = ator.id
    op.decided_at = _agora()
    op.expires_at = _agora() + validade
    return transitar(db, op, OperationStatus.approved, ator=ator, comentario=comentario, **kw)


def rejeitar(
    db: Session, op: Operation, ator: User, *, comentario: Optional[str] = None, **kw
) -> Operation:
    """Em revisão → rejeitada (estado final)."""
    op.reviewed_by_id = ator.id
    op.decided_at = _agora()
    return transitar(db, op, OperationStatus.rejected, ator=ator, comentario=comentario, **kw)


def iniciar_execucao(db: Session, op: Operation, ator: User, **kw) -> Operation:
    """
    Aprovada → a executar.

    Marca o prazo esgotado em vez de deixar correr, para que uma aprovação
    antiga não sirva de autorização eterna.
    """
    if op.esta_expirada:
        transitar(db, op, OperationStatus.expired, ator=ator, **kw)
        raise _erro(
            "A aprovação desta operação expirou. É preciso submetê-la de novo."
        )

    op.executed_by_id = ator.id
    op.started_at = _agora()
    return transitar(db, op, OperationStatus.executing, ator=ator, **kw)


def concluir_execucao(
    db: Session,
    op: Operation,
    *,
    rows_affected: Optional[int] = None,
    duration_ms: Optional[int] = None,
    **kw,
) -> Operation:
    """A executar → executada."""
    op.finished_at = _agora()
    op.rows_affected = rows_affected
    op.duration_ms = duration_ms
    return transitar(db, op, OperationStatus.executed, **kw)


def falhar_execucao(
    db: Session, op: Operation, *, erro: str, duration_ms: Optional[int] = None, **kw
) -> Operation:
    """A executar → falhada, guardando o motivo."""
    op.finished_at = _agora()
    op.duration_ms = duration_ms
    op.error = erro
    return transitar(db, op, OperationStatus.failed, **kw)


def marcar_revertida(db: Session, op: Operation, ator: User, **kw) -> Operation:
    """Executada ou falhada → revertida."""
    op.rolled_back_at = _agora()
    return transitar(db, op, OperationStatus.rolled_back, ator=ator, **kw)


def cancelar(
    db: Session, op: Operation, ator: User, *, comentario: Optional[str] = None, **kw
) -> Operation:
    """Cancela enquanto ainda não começou a correr."""
    return transitar(db, op, OperationStatus.cancelled, ator=ator, comentario=comentario, **kw)


# ══════════════════════════ o portão ══════════════════════════
def assert_executavel(db: Session, op: Operation) -> Operation:
    """
    Levanta se a operação não pode correr AGORA.

    Este é o ponto de que a Fase 2 vai depender: o gateway de execução chama
    isto antes de tocar no motor do cliente, e é o que transforma a promessa
    "nada corre fora de um pipeline ativo" numa condição verificada.
    """
    estado = OperationStatus(op.status)

    if estado is not ESTADO_EXECUTAVEL:
        raise _erro(
            f"Operação em '{estado.value}': só se executa o que está "
            f"'{ESTADO_EXECUTAVEL.value}'.",
            http.HTTP_403_FORBIDDEN,
        )

    if op.esta_expirada:
        transitar(db, op, OperationStatus.expired)
        raise _erro(
            "A aprovação desta operação expirou. É preciso submetê-la de novo.",
            http.HTTP_403_FORBIDDEN,
        )

    return op
