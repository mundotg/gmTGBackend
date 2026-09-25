"""
🧭 Operation — a unidade de governança do MustaInf.

Uma `Operation` é o pedido de alteração: liga **o que se vai fazer** (o payload)
a **onde** (`connection_id`), a **em que contexto** (`project_id`, o pipeline) e
a **quem pediu**. É a entidade que faltava para a promessa do produto — "nenhuma
operação crítica é executada fora de um pipeline ativo" — deixar de ser uma
frase e passar a ser uma condição verificável.

O ciclo de vida é estrito e vive em `TRANSICOES`: um estado só muda para os
estados que essa tabela permite. A validade dos valores é garantida também na
base de dados, por CHECK constraint (`native_enum=False`), para que um UPDATE à
mão não consiga inventar um estado.

O que esta tabela ainda NÃO faz, de propósito:

- Não aprova por etapas. Guarda uma única decisão (`reviewed_by_id`,
  `decided_at`, `review_comment`). A aprovação multi-etapas do plano Enterprise
  precisa de uma tabela `Approval` à parte, que se liga a esta por `operation_id`.
- Não é a trilha de auditoria. Guarda o resultado da execução, não o histórico
  imutável de tudo o que aconteceu; isso é uma tabela append-only separada.
- Não impõe isolamento por empresa. `empresa_id` é preenchido a partir de quem
  pede, para a operação ficar atribuída desde o primeiro dia, mas ainda não há
  filtro de tenant a apoiar-se nele.
"""

from __future__ import annotations

import enum
import uuid as uuid_lib
from datetime import datetime, timezone
from typing import Dict, FrozenSet

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.database import Base
from app.models.task_models import TimestampMixin


class OperationKind(str, enum.Enum):
    """O que a operação vai fazer. Decide quem a pode aprovar e como se reverte."""

    dml = "dml"  # INSERT / UPDATE / DELETE
    ddl = "ddl"  # CREATE / ALTER / DROP
    script = "script"  # lote de SQL livre, vindo do editor
    backup = "backup"
    restore = "restore"
    transfer = "transfer"  # cópia entre bases


class OperationRisk(str, enum.Enum):
    low = "low"
    medium = "medium"
    high = "high"


class OperationStatus(str, enum.Enum):
    """
    Estados possíveis. O caminho feliz é
    draft → pending_review → approved → executing → executed.
    """

    draft = "draft"
    pending_review = "pending_review"
    approved = "approved"
    rejected = "rejected"
    executing = "executing"
    executed = "executed"
    failed = "failed"
    rolled_back = "rolled_back"
    cancelled = "cancelled"
    expired = "expired"


# ── máquina de estados ────────────────────────────────────────────────────
#
# Ler como "de → para onde pode ir". Um estado ausente daqui, ou com conjunto
# vazio, é terminal. Manter isto como dado, e não como uma cascata de `if`,
# é o que permite a um teste percorrer a máquina inteira e provar que não há
# atalho de `draft` para `executed`.
TRANSICOES: Dict[OperationStatus, FrozenSet[OperationStatus]] = {
    OperationStatus.draft: frozenset(
        {OperationStatus.pending_review, OperationStatus.cancelled}
    ),
    OperationStatus.pending_review: frozenset(
        {
            OperationStatus.approved,
            OperationStatus.rejected,
            OperationStatus.cancelled,
        }
    ),
    OperationStatus.approved: frozenset(
        {
            OperationStatus.executing,
            # A aprovação tem prazo: autorizar na terça não deve autorizar a
            # execução um mês depois.
            OperationStatus.expired,
            OperationStatus.cancelled,
        }
    ),
    OperationStatus.executing: frozenset(
        {OperationStatus.executed, OperationStatus.failed}
    ),
    # Reverter é possível depois de correr — e também depois de falhar a meio,
    # que é precisamente quando costuma ser preciso.
    OperationStatus.executed: frozenset({OperationStatus.rolled_back}),
    OperationStatus.failed: frozenset({OperationStatus.rolled_back}),
    # Terminais.
    OperationStatus.rejected: frozenset(),
    OperationStatus.rolled_back: frozenset(),
    OperationStatus.cancelled: frozenset(),
    OperationStatus.expired: frozenset(),
}

#: Estados a partir dos quais nada mais acontece.
ESTADOS_TERMINAIS: FrozenSet[OperationStatus] = frozenset(
    estado for estado, destinos in TRANSICOES.items() if not destinos
)

#: O único estado em que a execução é permitida.
ESTADO_EXECUTAVEL: OperationStatus = OperationStatus.approved


def _enum_col(enum_cls, **kwargs):
    """
    Coluna de enum guardada como texto, com CHECK constraint.

    `native_enum=False` evita os tipos ENUM nativos do PostgreSQL, que exigem
    uma migração própria a cada valor novo e não existem em SQLite (usado nos
    testes).

    `create_constraint=True` não é opcional aqui: desde o SQLAlchemy 1.4 é
    `False` por omissão, e sem ele isto seria um VARCHAR comum — a validação
    existiria só em Python e um UPDATE à mão podia pôr a operação num estado
    inventado.
    """
    return Column(
        Enum(
            enum_cls,
            native_enum=False,
            create_constraint=True,
            length=20,
            values_callable=lambda e: [membro.value for membro in e],
        ),
        **kwargs,
    )


class Operation(Base, TimestampMixin):
    __tablename__ = "operations"

    id = Column(Integer, primary_key=True, index=True)

    # Identificador público. O `id` sequencial revela quantas operações existem
    # e é fácil de adivinhar; o que sai para fora é este.
    uuid = Column(
        String(36),
        default=lambda: str(uuid_lib.uuid4()),
        unique=True,
        nullable=False,
        index=True,
    )

    # ── contexto: é isto que torna a operação "governada" ──────────────────
    # O projeto é o pipeline. Sem ele a operação não tem contexto e não deve
    # existir, por isso é NOT NULL — é o coração da promessa do produto.
    project_id = Column(
        Integer,
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Opcional: nem toda a operação nasce de uma tarefa do quadro.
    task_id = Column(Integer, ForeignKey("tasks.id", ondelete="SET NULL"), nullable=True)

    # Âncora de tenant, preenchida a partir de quem pede. Ainda não há filtro
    # por empresa; existe para a operação ficar atribuída desde já, em vez de
    # ser preciso adivinhar retroativamente quando o multi-tenant chegar.
    empresa_id = Column(
        Integer, ForeignKey("empresas.id", ondelete="SET NULL"), nullable=True, index=True
    )

    # ── o que se vai fazer ─────────────────────────────────────────────────
    title = Column(String(255), nullable=False)
    description = Column(Text)
    kind = _enum_col(OperationKind, nullable=False)

    # Dialecto no momento do pedido. Guardado e não derivado da conexão porque
    # a conexão pode ser alterada depois, e o payload foi escrito para este.
    engine_dialect = Column(String(30))

    # SQL, comando Mongo ou especificação estruturada (transferência, restauro).
    payload = Column(Text, nullable=False)

    # O que o aprovador precisa de saber antes de decidir: tabelas afetadas,
    # linhas estimadas, se é reversível. Sem isto a aprovação é um carimbo às
    # cegas e a governança é decorativa.
    impact_plan = Column(JSON)
    risk_level = _enum_col(OperationRisk, nullable=False, default=OperationRisk.medium)

    # Comando inverso, quando existe. `is_reversible=False` obriga quem aprova
    # a assumir que não há volta atrás.
    rollback_payload = Column(Text)
    is_reversible = Column(Boolean, default=False, nullable=False)

    # ── ciclo de vida ──────────────────────────────────────────────────────
    status = _enum_col(
        OperationStatus, nullable=False, default=OperationStatus.draft, index=True
    )

    requested_by_id = Column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    submitted_at = Column(DateTime)

    reviewed_by_id = Column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    decided_at = Column(DateTime)
    review_comment = Column(String(500))

    # Prazo da aprovação. Passado isto, a operação transita para `expired` e
    # tem de voltar a ser aprovada.
    expires_at = Column(DateTime)

    # ── resultado da execução ──────────────────────────────────────────────
    executed_by_id = Column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    duration_ms = Column(Integer)
    rows_affected = Column(Integer)
    error = Column(Text)
    rolled_back_at = Column(DateTime)

    # ── relações ───────────────────────────────────────────────────────────
    project = relationship("Project", backref="operations")
    connection = relationship("DBConnection", backref="operations")
    task = relationship("Task")
    empresa = relationship("Empresa")

    requested_by = relationship("User", foreign_keys=[requested_by_id])
    reviewed_by = relationship("User", foreign_keys=[reviewed_by_id])
    executed_by = relationship("User", foreign_keys=[executed_by_id])

    # ── comportamento ──────────────────────────────────────────────────────
    def destinos_possiveis(self) -> FrozenSet[OperationStatus]:
        """Para onde esta operação pode ir a partir do estado atual."""
        return TRANSICOES.get(OperationStatus(self.status), frozenset())

    def pode_transitar_para(self, destino: OperationStatus) -> bool:
        return destino in self.destinos_possiveis()

    @property
    def esta_terminada(self) -> bool:
        return OperationStatus(self.status) in ESTADOS_TERMINAIS

    @property
    def esta_executavel(self) -> bool:
        """
        True só se a operação pode correr AGORA.

        Aprovada mas fora do prazo não é executável — e é por isso que a
        verificação de prazo vive aqui, e não apenas em quem faz a transição.
        """
        if OperationStatus(self.status) is not ESTADO_EXECUTAVEL:
            return False
        return not self.esta_expirada

    @property
    def esta_expirada(self) -> bool:
        if not self.expires_at:
            return False
        prazo = self.expires_at
        # As datas gravadas podem vir sem fuso (SQLite) ou com ele (PostgreSQL);
        # comparar os dois tipos diretamente levanta TypeError.
        if prazo.tzinfo is None:
            return prazo < datetime.utcnow()
        return prazo < datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return (
            f"<Operation(uuid={self.uuid}, kind={self.kind}, "
            f"status={self.status}, project={self.project_id}, "
            f"connection={self.connection_id})>"
        )
