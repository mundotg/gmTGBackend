from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base


class DBConnection(Base):
    __tablename__ = "db_connections"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # PostgreSQL, MySQL, etc.
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)  # armazenar criptografado!
    database_name = Column(String, nullable=False)
    # Connection string completa, cifrada em repouso. Quando está preenchida é
    # ela que liga (modo "por URL"); host/port/database_name continuam a ser
    # guardados, derivados da URL, porque são NOT NULL e a listagem mostra-os.
    url = Column(String, nullable=True)
    sslmode = Column(String, default="disable")
    service = Column(String, nullable=True)
    trustServerCertificate = Column(String, nullable=True)
    status = Column(String, default="available")
    is_encrypted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # 🔹 Relacionamentos ORM
    owner = relationship("User", back_populates="db_connections")
    queries = relationship("QueryHistory", back_populates="connection", cascade="all, delete-orphan", passive_deletes=True)
    statistics = relationship("DBStatistics", back_populates="connection", uselist=False, passive_deletes=True)
    logs = relationship("ConnectionLog", back_populates="connection", passive_deletes=True)
    structures = relationship("DBStructure", back_populates="connection", cascade="all, delete-orphan", lazy="selectin")
    row_count_cache = relationship("TableRowCountCache", back_populates="connection", cascade="all, delete-orphan")
    health_checks = relationship("DBHealthCheck", back_populates="connection", cascade="all, delete-orphan", passive_deletes=True)

    # 🧩 Novo relacionamento com projetos
    projects = relationship("Project", back_populates="db_connection", cascade="all, delete-orphan")

    # 🤝 Partilhas: quem mais, além do dono, pode usar esta conexão
    shares = relationship(
        "DBConnectionShare",
        back_populates="connection",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DBConnectionShare.connection_id]",
    )

    # 🔑 Roles de conexão específicas desta base de dados
    roles = relationship(
        "ConnectionRole",
        back_populates="connection",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # 🏢 Relação N:N com empresas vinculadas
    empresas = relationship(
        "Empresa",
        secondary="empresa_connections",
        back_populates="connections",
        lazy="selectin",
    )

    def __repr__(self):
        return f"<DBConnection(id={self.id}, name='{self.name}', type='{self.type}')>"


# =============================
# 🏢🔗🔌 Empresa - Connection Association (N:N)
# =============================
class EmpresaConnection(Base):
    """
    Tabela de associação N:N entre Empresas e Conexões de Banco de Dados.
    Permite que uma ou mais empresas partilhem/acedam à mesma conexão e vice-versa.
    """

    __tablename__ = "empresa_connections"

    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
        index=True,
    )
    connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
        index=True,
    )
    access_level = Column(String(20), nullable=False, default="read")
    role_id = Column(
        Integer,
        ForeignKey("connection_roles.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=True,
    )

    empresa = relationship(
        "Empresa", foreign_keys=[empresa_id], overlaps="connections,empresas"
    )
    connection = relationship(
        "DBConnection", foreign_keys=[connection_id], overlaps="connections,empresas"
    )
    role = relationship("ConnectionRole", foreign_keys=[role_id])


empresa_connections = EmpresaConnection.__table__


# =============================
# 🛡️ Connection Role - Permission Association Table
# =============================
connection_roles_permissions = Table(
    "connection_roles_permissions",
    Base.metadata,
    Column(
        "connection_role_id",
        Integer,
        ForeignKey("connection_roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "permission_id",
        Integer,
        ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


# =============================
# 🔑 Connection Role (Role por Conexão)
# =============================
class ConnectionRole(Base):
    """
    Função/Role criada e isolada no âmbito de uma conexão específica.
    Define com granularidade o que cada utilizador partilhado pode fazer
    nesta conexão de dados (consultas, DDL, DML, exportação, etc.).
    Inclui regras granulares de tabelas, campos e tipos de query permitidos.
    """

    __tablename__ = "connection_roles"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String(50), nullable=False)
    description = Column(String(200), nullable=True)
    is_default = Column(Boolean, default=False)

    # 🎯 Regras Avançadas de Segurança e Granularidade
    allowed_tables = Column(JSON, default=list, nullable=True)
    blocked_tables = Column(JSON, default=list, nullable=True)
    allowed_columns = Column(JSON, default=dict, nullable=True)
    blocked_columns = Column(JSON, default=dict, nullable=True)
    allowed_query_types = Column(JSON, default=list, nullable=True)
    max_rows = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("connection_id", "name", name="uq_connection_role_name"),
    )

    connection = relationship("DBConnection", back_populates="roles")
    permissions = relationship(
        "Permission",
        secondary=connection_roles_permissions,
        lazy="selectin",
    )

    def __repr__(self):
        return f"<ConnectionRole(id={self.id}, connection_id={self.connection_id}, name='{self.name}')>"


class DBConnectionShare(Base):
    """
    🤝 Acesso concedido pelo dono de uma conexão a outro utilizador.

    Níveis (`access_level`), do mais fraco para o mais forte:
      - read   → ver a conexão e consultar dados
      - write  → o anterior + alterar dados (insert/update/delete)
      - manage → o anterior + partilhar a conexão com outros

    Além disso, pode estar vinculado a uma `ConnectionRole` customizada desta
    conexão (`role_id`), definindo exatamente as permissões granulares,
    e ter regras avançadas próprias de tabelas, campos e tipos de consulta.
    """

    __tablename__ = "db_connection_shares"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    access_level = Column(String(20), nullable=False, default="read")

    # Role granular específica desta conexão
    role_id = Column(
        Integer,
        ForeignKey("connection_roles.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # 🎯 Regras Avançadas Personalizadas do Membro (sobrepõem ou estendem a role)
    allowed_tables = Column(JSON, default=list, nullable=True)
    blocked_tables = Column(JSON, default=list, nullable=True)
    allowed_columns = Column(JSON, default=dict, nullable=True)
    blocked_columns = Column(JSON, default=dict, nullable=True)
    allowed_query_types = Column(JSON, default=list, nullable=True)
    max_rows = Column(Integer, nullable=True)

    # Quem concedeu o acesso (para auditoria). SET NULL: se essa conta for
    # apagada, a partilha mantém-se válida.
    granted_by_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("connection_id", "user_id", name="uq_connection_share"),
    )

    connection = relationship(
        "DBConnection", back_populates="shares", foreign_keys=[connection_id]
    )
    user = relationship("User", foreign_keys=[user_id])
    granted_by = relationship("User", foreign_keys=[granted_by_id])
    role = relationship("ConnectionRole", foreign_keys=[role_id])

    def __repr__(self):
        return (
            f"<DBConnectionShare(connection_id={self.connection_id}, "
            f"user_id={self.user_id}, level='{self.access_level}', role_id={self.role_id})>"
        )


class ActiveConnection(Base):
    """
    🔌 Qual é, para cada utilizador, a conexão atualmente ligada.

    A chave inclui `user_id` de propósito. Enquanto a chave era só
    `connection_id`, "estar ativa" era uma propriedade da conexão e não de quem
    a usa: dois utilizadores com acesso à mesma conexão partilhada escreviam na
    mesma linha, e ligar de um lado desligava o outro sem aviso. Só o dono
    conseguia trabalhar, porque as consultas filtravam por `DBConnection.user_id`.

    Com a chave composta, cada utilizador tem o seu próprio estado de ligação
    sobre a mesma conexão. Quem pode ligar-se a quê é decidido à parte, por
    `app.ultils.connection_access` — aqui só se regista o que está ligado.
    """

    __tablename__ = "active_connection"

    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), primary_key=True)
    activated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(Boolean, default=False, nullable=False)
    last_checked = Column(DateTime, nullable=True)

    connection = relationship("DBConnection", backref="active_status")


class ConnectionLog(Base):
    __tablename__ = "connection_logs"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String, nullable=True)
    details = Column(JSON, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    status = Column(String, default="success")

    connection = relationship("DBConnection", back_populates="logs")
    
class DBHealthCheck(Base):
    __tablename__ = "db_health_checks"

    id = Column(Integer, primary_key=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"))
    checked_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    latency_ms = Column(Integer)
    reachable = Column(Boolean, default=False)

    connection = relationship("DBConnection", back_populates="health_checks")

