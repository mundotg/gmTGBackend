# app/cruds/dbstructure_crud.py
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.models.dbstructure_models import DBField, DBStructure, DBEnumField
from app.schemas.dbstructure_schema import DBFieldCreate, DBStructureCreate
from app.ultils.logger import log_message


# ==============================================================================
#                           CRUD: DBStructure
# ==============================================================================
def create_db_structure(db: Session, structure_in: DBStructureCreate) -> DBStructure:
    """
    Cria uma nova estrutura de tabela (DBStructure) e salva no banco de dados.
    """
    try:
        structure = DBStructure(
            db_connection_id=structure_in.db_connection_id,
            table_name=structure_in.table_name,
            schema_name=structure_in.schema_name,
            description=structure_in.description,
        )
        db.add(structure)
        db.commit()
        db.refresh(structure)

        # log_message(
        #     f"✅ Estrutura '{structure.table_name}' criada (Conexão ID: {structure.db_connection_id})",
        #     "success",
        # )
        return structure

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao criar estrutura '{structure_in.table_name}': {e}", "error")
        raise


def update_db_structure(db: Session, estrutura: DBStructure) -> DBStructure:
    """
    Atualiza uma estrutura de tabela existente.
    """
    try:
        db.add(estrutura)
        db.commit()
        db.refresh(estrutura)
        # log_message(f"✅ Estrutura atualizada: {estrutura.table_name}", "info")
        return estrutura
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar estrutura '{estrutura.table_name}': {e}", "error")
        raise


def get_db_structures(db: Session, connection_id: int) -> List[DBStructure]:
    """
    Retorna todas as estruturas associadas a uma conexão.
    """
    return db.query(DBStructure).filter(DBStructure.db_connection_id == connection_id).all()


def get_db_structures_by_conn_id_and_table(
    db: Session, db_connection_id: int, table_name: str
) -> Optional[DBStructure]:
    """
    Busca estrutura por ID da conexão e nome da tabela.
    """
    return (
        db.query(DBStructure)
        .filter(
            DBStructure.db_connection_id == db_connection_id,
            DBStructure.table_name == table_name,
        )
        .first()
    )


def get_structure_by_id(db: Session, structure_id: int) -> Optional[DBStructure]:
    return db.query(DBStructure).filter(DBStructure.id == structure_id).first()


def get_structure_by_id_and_name(
    db: Session, connection_id: int, table_name: str
) -> Optional[DBStructure]:
    return (
        db.query(DBStructure)
        .filter(
            DBStructure.db_connection_id == connection_id,
            DBStructure.table_name == table_name,
        )
        .first()
    )


def delete_structure(db: Session, structure_id: int) -> bool:
    """
    Exclui uma estrutura de tabela pelo ID.
    """
    structure = get_structure_by_id(db, structure_id)
    if structure:
        db.delete(structure)
        db.commit()
        log_message(f"⚠️ Estrutura com ID {structure_id} deletada.", "warning")
        return True

    log_message(f"❌ Estrutura não encontrada para exclusão: ID {structure_id}", "error")
    return False


def delete_structure_by_name(db: Session, table_name: str, db_connection_id: int) -> bool:
    """
    Exclui estrutura com base no nome e conexão.
    """
    deleted_count = (
        db.query(DBStructure)
        .filter_by(table_name=table_name, db_connection_id=db_connection_id)
        .delete(synchronize_session=False)
    )

    if deleted_count > 0:
        db.commit()
        log_message(
            f"⚠️ Estrutura '{table_name}' (conn_id={db_connection_id}) deletada.",
            "warning",
        )
        return True

    log_message(
        f"❌ Estrutura '{table_name}' (conn_id={db_connection_id}) não encontrada para exclusão.",
        "error",
    )
    return False


# ==============================================================================
#                           CRUD: DBField
# ==============================================================================
def create_db_field(db: Session, field_in: DBFieldCreate, structure_id: int) -> DBField:
    """
    Cria ou atualiza um campo (DBField) em uma estrutura de tabela.
    """
    try:
        existing_field = (
            db.query(DBField)
            .filter_by(structure_id=structure_id, name=field_in.name)
            .first()
        )

        if existing_field:
            # Atualiza campo existente
            for attr, value in field_in.model_dump().items():
                if hasattr(existing_field, attr):
                    setattr(existing_field, attr, value)

            db.commit()
            db.refresh(existing_field)
            # log_message(
            #     f"🔄 Campo '{field_in.name}' atualizado na estrutura ID {structure_id}",
            #     "info",
            # )
            return existing_field

        # Criação de novo campo
        field = DBField(structure_id=structure_id, **field_in.model_dump())
        db.add(field)
        db.commit()
        db.refresh(field)

        # log_message(f"🟢 Campo '{field.name}' criado na estrutura ID {structure_id}", "info")
        return field

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao criar/atualizar campo '{field_in.name}': {e}", "error")
        raise


def get_fields_by_structure_pk(db: Session, structure_id: int) -> Optional[DBField]:
    """
    Retorna a melhor coluna candidata como chave primária.
    """
    for condition in [
        DBField.is_primary_key == True,
        DBField.is_unique == True,
        or_(DBField.is_auto_increment == True, DBField.is_nullable == True),
    ]:
        column = db.query(DBField).filter(and_(DBField.structure_id == structure_id, condition)).first()
        if column:
            return column

    return db.query(DBField).filter(DBField.structure_id == structure_id).first()


def get_fields_by_structure(db: Session, structure_id: int) -> List[DBField]:
    return db.query(DBField).filter(DBField.structure_id == structure_id).all()


def get_fields_by_tablename(db: Session, structure_id: int, col_name: str) -> Optional[DBField]:
    return (
        db.query(DBField)
        .filter(DBField.structure_id == structure_id, DBField.name == col_name)
        .first()
    )


def delete_field(db: Session, field_id: int) -> bool:
    """
    Exclui um campo com base no ID.
    """
    field = db.query(DBField).filter(DBField.id == field_id).first()
    if field:
        db.delete(field)
        db.commit()
        log_message(f"⚠️ Campo com ID {field_id} deletado.", "warning")
        return True

    log_message(f"❌ Campo não encontrado para exclusão: ID {field_id}", "error")
    return False


def delete_field_name(db: Session, field_name: str, structure_id: int) -> bool:
    deleted_count = (
        db.query(DBField)
        .filter(DBField.name == field_name, DBField.structure_id == structure_id)
        .delete(synchronize_session=False)
    )

    if deleted_count > 0:
        db.commit()
        log_message(
            f"⚠️ {deleted_count} campo(s) com nome '{field_name}' deletado(s) da estrutura ID {structure_id}.",
            "warning",
        )
        return True

    log_message(
        f"❌ Nenhum campo encontrado com nome '{field_name}' na estrutura ID {structure_id} para exclusão.",
        "error",
    )
    return False


# ==============================================================================
#                           CRUD: DBEnumField
# ==============================================================================
def create_enum_field(db: Session, data: DBEnumField) -> DBEnumField:
    """
    Cria um novo valor ENUM para um campo, se ainda não existir.
    """
    existing = db.query(DBEnumField).filter_by(field_id=data.field_id, value=data.value).first()
    if existing:
        log_message(f"⚠️ Valor ENUM '{data.value}' já existe para field ID {data.field_id}", "warning")
        return existing

    db.add(data)
    db.commit()
    db.refresh(data)
    # log_message(f"🆕 Valor ENUM '{data.value}' criado para field ID {data.field_id}", "info")
    return data


def get_enum_field(db: Session, field_id: int, valor: str) -> Optional[DBEnumField]:
    return db.query(DBEnumField).filter_by(field_id=field_id, value=valor).first()


def list_enum_fields(db: Session) -> List[DBEnumField]:
    return db.query(DBEnumField).all()


def list_enum_fields_by_field(db: Session, field_id: int) -> List[DBEnumField]:
    return db.query(DBEnumField).filter_by(field_id=field_id).all()


def delete_enum_field(db: Session, field_id: int, valor: str) -> bool:
    obj = get_enum_field(db, field_id, valor)
    if not obj:
        log_message(f"❌ Valor ENUM '{valor}' não encontrado para exclusão (field ID: {field_id})", "error")
        return False

    db.delete(obj)
    db.commit()
    log_message(f"🗑️ Valor ENUM '{valor}' removido com sucesso (field ID: {field_id})", "warning")
    return True
