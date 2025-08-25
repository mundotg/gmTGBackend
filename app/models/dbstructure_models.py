from datetime import datetime, timezone
from sqlalchemy import (
    Column, Index, Integer, String, Boolean, ForeignKey, DateTime, Text,
    UniqueConstraint
)
from sqlalchemy.orm import relationship
from app.database import Base


class DBStructure(Base):
    __tablename__ = "db_structures"
    __table_args__ = (
        UniqueConstraint("db_connection_id", "table_name", "schema_name", name="uq_structure_connection_table"),
    )

    id = Column(Integer, primary_key=True, index=True)
    db_connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String, nullable=False)
    schema_name = Column(String, nullable=True)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    is_deleted = Column(Boolean, default=False)

    fields = relationship("DBField", back_populates="structure", cascade="all, delete-orphan", lazy="selectin")
    connection = relationship("DBConnection", back_populates="structures")

    @property
    def full_table_name(self):
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name


class DBField(Base):
    __tablename__ = "db_fields"

    id = Column(Integer, primary_key=True, index=True)
    structure_id = Column(Integer, ForeignKey("db_structures.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    is_nullable = Column(Boolean, default=True)
    is_primary_key = Column(Boolean, default=False)
    is_unique = Column(Boolean, default=False)
    is_auto_increment = Column(Boolean, default=False)
    is_ForeignKey = Column(Boolean, default=False)
    referenced_table = Column(String, nullable=True)  # ← Adicionado aqui
    field_references =  Column(String, nullable=True)  # Nome do campo que referencia
    default_value = Column(String, nullable=True)
    comment = Column(Text, nullable=True)
    length = Column(Integer, nullable=True)
    precision = Column(Integer, nullable=True)
    scale = Column(Integer, nullable=True)

    structure = relationship("DBStructure", back_populates="fields")
    enum_values = relationship("DBEnum_field", back_populates="field", cascade="all, delete-orphan")



class DBEnum_field(Base):
    """
    Representa os valores possíveis de um campo ENUM.
    Cada valor é associado a um campo (coluna) de uma tabela específica.
    """
    __tablename__ = "db_enum_fields"
    __table_args__ = (
        Index("ix_enum_field_field_id", "field_id"),  # Index para acelerar buscas por campo
    )

    field_id = Column(Integer, ForeignKey("db_fields.id", ondelete="CASCADE"), primary_key=True)
    valor = Column(String(255), primary_key=True)  # limite opcional para garantir compatibilidade com bancos

    # Metadados
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)  # permite manter histórico mesmo quando valores são desativados

    # Relacionamento com DBField
    field = relationship("DBField", back_populates="enum_values")
