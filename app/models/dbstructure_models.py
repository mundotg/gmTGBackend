from sqlalchemy import (
    Column, Index, Integer, String, Boolean, ForeignKey, DateTime, Text,
    UniqueConstraint, func
)
from sqlalchemy.orm import relationship, validates
from app.database import Base


class DBStructure(Base):
    __tablename__ = "db_structures"
    __table_args__ = (
        UniqueConstraint("db_connection_id", "table_name", "schema_name", name="uq_structure_connection_table"),
        Index("ix_db_structures_connection", "db_connection_id"),
    )

    id = Column(Integer, primary_key=True, index=True)
    db_connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String, nullable=False)
    schema_name = Column(String, nullable=True)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Boolean, default=False)

    # Relações
    fields = relationship(
        "DBField",back_populates="structure",
        cascade="all, delete-orphan", lazy="selectin"
    )
    connection = relationship("DBConnection", back_populates="structures")

    @property
    def full_table_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name

    @validates("table_name")
    def validate_table_name(self, key, value):
        if not value or not value.strip():
            raise ValueError("O nome da tabela não pode estar vazio.")
        return value.strip()

    def __repr__(self):
        return f"<DBStructure(id={self.id}, table='{self.table_name}', schema='{self.schema_name}')>"



class DBField(Base):
    """
    Representa uma coluna (campo) pertencente a uma tabela de banco de dados.
    Contém metadados como tipo, restrições e relacionamentos (FK, PK, UNIQUE).
    """
    __tablename__ = "db_fields"
    __table_args__ = (
        UniqueConstraint("structure_id", "name", name="uq_field_structure_name"),
    )

    id = Column(Integer, primary_key=True, index=True)

    # FK para a estrutura/tabela que contém este campo
    structure_id = Column(
        Integer,
        ForeignKey("db_structures.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True
    )

    name = Column(String(255), nullable=False)
    type = Column(String(100), nullable=False)
    is_nullable = Column(Boolean, default=True)
    is_primary_key = Column(Boolean, default=False)
    is_unique = Column(Boolean, default=False)
    is_auto_increment = Column(Boolean, default=False)

    # Informações de FK (referência a outra tabela/field)is_foreign_key
    
    is_foreign_key = Column(Boolean, default=False)
    referenced_table = Column(String(255), nullable=True)   # Nome da tabela referenciada
    referenced_field = Column(String(255), nullable=True)   # Nome do campo referenciado (por texto)
    referenced_field_id = Column(
        Integer,
        ForeignKey("db_fields.id", ondelete="SET NULL", onupdate="CASCADE"),
        nullable=True,
        index=True
    )
    fk_on_delete = Column(String, nullable=True, default="NO ACTION")  # CASCADE, SET NULL, RESTRICT, NO ACTION...
    fk_on_update = Column(String, nullable=True, default="NO ACTION")

    default_value = Column(String(255), nullable=True)
    comment = Column(Text, nullable=True)
    length = Column(Integer, nullable=True)
    precision = Column(Integer, nullable=True)
    scale = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relação com a estrutura (tabela)
    structure = relationship(
        "DBStructure",
        back_populates="fields",
        passive_deletes=True,
        lazy="selectin"
    )

    # Relação auto-referenciada: este campo referencia outro DBField (referenced_field_id)
    referenced_field_obj = relationship(
        "DBField",
        remote_side=[id],
        back_populates="referenced_by_fields",
        uselist=False,
        foreign_keys=[referenced_field_id],
        passive_deletes=True,
        lazy="selectin"
    )

    # Lista de campos que referenciam este field
    referenced_by_fields = relationship(
        "DBField",
        back_populates="referenced_field_obj",
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    # Valores ENUM associados (se houver)
    enum_values = relationship(
        "DBEnumField",
        back_populates="field",
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    @validates("name")
    def validate_name(self, key, value):
        if not value or not value.strip():
            raise ValueError("O nome do campo não pode estar vazio.")
        return value.strip()

    def __repr__(self):
        return (
            f"<DBField(id={self.id}, name='{self.name}', type='{self.type}', "
            f"FK={self.is_foreign_key}, ref_table='{self.referenced_table}', ref_field='{self.referenced_field}')>"
        )


class DBEnumField(Base):
    """
    Representa os valores possíveis de um campo ENUM.
    Cada valor é associado a um campo (coluna) de uma tabela específica.
    """
    __tablename__ = "db_enum_fields"
    __table_args__ = (
        Index("ix_enum_field_field_id", "field_id"),
    )

    field_id = Column(
        Integer,
        ForeignKey("db_fields.id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
        nullable=False,
        index=True
    )
    value = Column(String(255), primary_key=True, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Relacionamento inverso com DBField
    field = relationship("DBField", back_populates="enum_values", lazy="selectin")

    def __repr__(self):
        return f"<DBEnumField(field_id={self.field_id}, value='{self.value}', active={self.is_active})>"
