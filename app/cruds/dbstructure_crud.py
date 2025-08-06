from typing import List, Optional
from sqlalchemy.orm import Session
from app.models.dbstructure_models import DBField, DBStructure, DBEnum_field
from app.schemas.dbstructure_schema import (
    DBFieldCreate,
    DBStructureCreate,
)
from app.ultils.logger import log_message

# ------------------------------------------------------------------------------
# DBStructure CRUD
# ------------------------------------------------------------------------------

def create_db_structure(db: Session, structure_in: DBStructureCreate) -> DBStructure:
    structure = DBStructure(
        db_connection_id=structure_in.db_connection_id,
        table_name=structure_in.table_name,
        schema_name=structure_in.schema_name,
        description=structure_in.description,
    )
    db.add(structure)
    db.commit()
    db.refresh(structure)
    log_message(
        f"✅ Estrutura '{structure.table_name}' criada (Conexão ID: {structure.db_connection_id})", "success"
    )
    return structure


def update_db_structure(db: Session, estrutura: DBStructure) -> DBStructure:
    try:
        db.add(estrutura)
        db.commit()
        db.refresh(estrutura)
        log_message(f"✅ Estrutura atualizada: {estrutura.table_name}", "info")
        return estrutura
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar estrutura '{estrutura.table_name}': {e}", "error")
        raise


def get_db_structures(db: Session, connection_id: int) -> List[DBStructure]:
    return db.query(DBStructure).filter(DBStructure.db_connection_id == connection_id).all()


def get_db_structures_by_conn_id_and_table(
    db: Session, db_connection_id: int, table_name: str
) -> Optional[DBStructure]:
    return db.query(DBStructure).filter(
        DBStructure.db_connection_id == db_connection_id,
        DBStructure.table_name == table_name
    ).first()


def get_structure_by_id(db: Session, structure_id: int) -> Optional[DBStructure]:
    return db.query(DBStructure).filter(DBStructure.id == structure_id).first()


def get_structure_by_id_and_name(db: Session, connection_id: int, table_name: str) -> Optional[DBStructure]:
    return db.query(DBStructure).filter(
        DBStructure.db_connection_id == connection_id,
        DBStructure.table_name == table_name
    ).first()


def delete_structure(db: Session, structure_id: int) -> bool:
    structure = get_structure_by_id(db, structure_id)
    if structure:
        db.delete(structure)
        db.commit()
        log_message(f"⚠️ Estrutura com ID {structure_id} deletada.", "warning")
        return True
    log_message(f"❌ Estrutura não encontrada para exclusão: ID {structure_id}", "error")
    return False

def delete_structure_by_name(
    db: Session,
    table_name: str,
    db_connection_id: int
) -> bool:

    deleted_count = db.query(DBStructure).filter_by(
        table_name=table_name,
        db_connection_id=db_connection_id
    ).delete(synchronize_session=False)

    if deleted_count > 0:
        db.commit()
        log_message(f"⚠️ Estrutura '{table_name}' d(conn_id={db_connection_id}) deletada.", "warning")
        return True

    log_message(
        f"❌ Estrutura '{table_name}'  (conn_id={db_connection_id}) não encontrada para exclusão.",
        "error"
    )
    return False

# ------------------------------------------------------------------------------
# DBField CRUD
# ------------------------------------------------------------------------------

def create_db_field(db: Session, field_in: DBFieldCreate, structure_id: int) -> DBField:
    # Verifica se o campo já existe na estrutura (evita duplicação)
    existing_field = db.query(DBField).filter_by(
        structure_id=structure_id,
        name=field_in.name
    ).all()

    if existing_field:
        log_message(f"⚠️ Campo '{field_in.name}' já existe para estrutura ID {structure_id}. Ignorando criação.", "warning")
        print(f"[DUPLICADO] Campo '{field_in.name}' já existe para estrutura {structure_id}")
        return existing_field

    # Cria novo campo
    field = DBField(
        structure_id=structure_id,
        name=field_in.name,
        type=field_in.type,
        is_nullable=field_in.is_nullable,
        default_value=field_in.default_value,
        is_primary_key=field_in.is_primary_key,
        is_ForeignKey=field_in.is_ForeignKey,
        is_unique=field_in.is_unique,
        referenced_table=field_in.referenced_table,
        is_auto_increment=field_in.is_auto_increment,
        comment=field_in.comment,
        length=field_in.length,
        precision=field_in.precision,
        scale=field_in.scale,
    )

    db.add(field)
    db.commit()
    db.refresh(field)

    log_message(f"🟢 Campo '{field.name}' criado para estrutura ID {structure_id}", "info")
    print(f"[CRIADO] Campo '{field.name}' adicionado à estrutura {structure_id}")
    return field




def get_fields_by_structure(db: Session, structure_id: int) -> List[DBField]:
    return db.query(DBField).filter(DBField.structure_id == structure_id).all()


def get_fields_by_tablename(db: Session, structure_id: int, col_name: str) -> Optional[DBField]:
    return db.query(DBField).filter(
        DBField.structure_id == structure_id,
        DBField.name == col_name
    ).first()


def delete_field(db: Session, field_id: int) -> bool:
    field = db.query(DBField).filter(DBField.id == field_id).first()
    if field:
        db.delete(field)
        db.commit()
        log_message(f"⚠️ Campo com ID {field_id} deletado.", "warning")
        return True
    log_message(f"❌ Campo não encontrado para exclusão: ID {field_id}", "error")
    return False

def delete_field_name(db: Session, field_name: str, structure_id: int) -> bool:
    deleted_count = db.query(DBField).filter(
        DBField.name == field_name,
        DBField.structure_id == structure_id
    ).delete(synchronize_session=False)

    if deleted_count > 0:
        db.commit()
        log_message(f"⚠️ {deleted_count} campo(s) com nome '{field_name}' deletado(s) da estrutura ID {structure_id}.", "warning")
        return True

    log_message(f"❌ Nenhum campo encontrado com nome '{field_name}' na estrutura ID {structure_id} para exclusão.", "error")
    return False

# ------------------------------------------------------------------------------
# DBEnum_field CRUD
# ------------------------------------------------------------------------------

def create_enum_field(db: Session, data: DBEnum_field) -> DBEnum_field:
    # item = DBEnum_field(**data.dict())
    db.add(data)
    db.commit()
    db.refresh(data)
    log_message(f"🆕 Valor ENUM '{data.valor}' criado para field ID {data.field_id}", "info")
    return data


def get_enum_field(db: Session, field_id: int, valor: str) -> Optional[DBEnum_field]:
    return db.query(DBEnum_field).filter_by(field_id=field_id, valor=valor).first()


def list_enum_fields(db: Session) -> List[DBEnum_field]:
    return db.query(DBEnum_field).all()


def list_enum_fields_by_field(db: Session, field_id: int) -> List[DBEnum_field]:
    return db.query(DBEnum_field).filter_by(field_id=field_id).all()


def delete_enum_field(db: Session, field_id: int, valor: str) -> bool:
    obj = get_enum_field(db, field_id, valor)
    if not obj:
        log_message(f"❌ Valor ENUM '{valor}' não encontrado para exclusão (field ID: {field_id})", "error")
        return False
    db.delete(obj)
    db.commit()
    log_message(f"🗑️ Valor ENUM '{valor}' removido com sucesso (field ID: {field_id})", "warning")
    return True
