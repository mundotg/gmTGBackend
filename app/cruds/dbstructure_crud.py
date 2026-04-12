# app/cruds/dbstructure_crud.py
from __future__ import annotations

from typing import List, Optional, Dict, Any, Iterable, Set, Sequence

from sqlalchemy.orm import Session, load_only, noload
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.models.dbstructure_models import (
    DBField,
    DBStructure,
    DBEnumField,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_DELETED,
    STATUS_ERROR,
)
from app.schemas.dbstructure_schema import DBFieldCreate, DBStructureCreate
from app.ultils.logger import log_message


# ==============================================================================
# Helpers (commit/rollback + status filters + apply data)
# ==============================================================================


class CRUDCommitError(RuntimeError):
    pass


VISIBLE_STRUCTURE_STATUS: Set[str] = {STATUS_ACTIVE}  # padrão
VISIBLE_FIELD_STATUS: Set[str] = {STATUS_ACTIVE}  # padrão
VISIBLE_ENUM_STATUS: Set[str] = {STATUS_ACTIVE}  # padrão


def _status_filter(col, allowed: Iterable[str]) -> Any:
    return col.in_(list(allowed))


def _safe_commit(db: Session, *, action: str, context: str = "") -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        msg = f"Violação de integridade ao {action}. {context}".strip()
        log_message(f"❌ {msg} | detalhe={exc}", "error")
        raise CRUDCommitError(msg) from exc
    except SQLAlchemyError as exc:
        db.rollback()
        msg = f"Erro de banco ao {action}. {context}".strip()
        log_message(f"❌ {msg} | detalhe={exc}", "error")
        raise CRUDCommitError(msg) from exc


def _apply_model_data(obj: Any, data: Dict[str, Any]) -> None:
    for attr, value in data.items():
        if hasattr(obj, attr):
            setattr(obj, attr, value)


def _normalize_visible_status(
    visible: Optional[Sequence[str]],
    default: Set[str],
) -> List[str]:
    if visible is None:
        return list(default)
    cleaned = [str(s).strip().lower() for s in visible if str(s).strip()]
    allowed_all = {STATUS_ACTIVE, STATUS_INACTIVE, STATUS_DELETED, STATUS_ERROR}
    bad = [s for s in cleaned if s not in allowed_all]
    if bad:
        raise ValueError(
            f"Status inválido(s): {bad}. Permitidos: {sorted(allowed_all)}"
        )
    return cleaned


# ==============================================================================
# CRUD: DBStructure
# ==============================================================================


def update_db_structure(db: Session, estrutura: DBStructure) -> DBStructure:
    try:
        db.add(estrutura)
        _safe_commit(
            db, action="atualizar estrutura", context=f"table={estrutura.table_name}"
        )
        db.refresh(estrutura)
        return estrutura
    finally:
        # Não fechamos a sessão aqui pois ela é gerenciada pelo caller
        pass


def get_db_structures(
    db: Session,
    connection_id: int,
    *,
    visible_status: Optional[Sequence[str]] = None,
    include_deleted_flag: bool = False,
) -> list[DBStructure]:
    """
    Lista estruturas:
    - Filtra por status (default: active)
    - Filtra por is_deleted (default: False)

    ✅ Otimizado: SELECT só das colunas necessárias + sem relações
    """
    try:
        status_list = _normalize_visible_status(
            visible_status, VISIBLE_STRUCTURE_STATUS
        )

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.db_connection_id,  # type: ignore
                    DBStructure.table_name,  # type: ignore
                    DBStructure.schema_name,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                    DBStructure.created_at,  # type: ignore
                    DBStructure.updated_at,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.db_connection_id == connection_id)
            .filter(_status_filter(DBStructure.status, status_list))
        )

        if not include_deleted_flag:
            q = q.filter(DBStructure.is_deleted.is_(False))

        return q.order_by(
            DBStructure.schema_name.asc().nullsfirst(), DBStructure.table_name.asc()
        ).all()
    finally:
        # A sessão será fechada pelo caller (Depends(get_db))
        pass


def get_db_structures_by_conn_id_and_table(
    db: Session,
    db_connection_id: int,
    table_name: str,
    *,
    visible_status: Optional[Sequence[str]] = None,
    include_deleted_flag: bool = False,
) -> Optional[DBStructure]:
    """
    Busca uma estrutura de base de dados pelo id da conexão e nome da tabela.

    Otimizações:
    - SELECT apenas das colunas necessárias
    - Sem carregar relações
    """
    try:
        if not table_name or not table_name.strip():
            raise ValueError("table_name inválido.")

        status_list = _normalize_visible_status(
            visible_status, VISIBLE_STRUCTURE_STATUS
        )

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.db_connection_id,  # type: ignore
                    DBStructure.table_name,  # type: ignore
                    DBStructure.schema_name,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                    DBStructure.created_at,  # type: ignore
                    DBStructure.updated_at,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(
                DBStructure.db_connection_id == db_connection_id,
                DBStructure.table_name == table_name,
            )
            .filter(_status_filter(DBStructure.status, status_list))
        )

        if not include_deleted_flag:
            q = q.filter(DBStructure.is_deleted.is_(False))

        return q.first()

    except SQLAlchemyError as e:
        log_message(
            message=f"Erro SQL ao buscar DBStructure | conn_id={db_connection_id} | table={table_name} | erro={str(e)}",
            level="error",
        )
        raise

    except ValueError as e:
        log_message(
            message=f"Erro de validação em get_db_structures_by_conn_id_and_table | {str(e)}",
            level="warning",
        )
        raise

    except Exception as e:
        log_message(
            message=f"Erro inesperado ao buscar DBStructure | conn_id={db_connection_id} | table={table_name} | erro={str(e)}",
            level="critical",
        )
        raise
    finally:
        # A sessão será fechada pelo caller
        pass


def _norm_schema(schema_name: Optional[str]) -> Optional[str]:
    s = (schema_name or "").strip()
    return s or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    s = (v or "").strip()
    return s or None


# Certifique-se de que DBStructure, STATUS_ACTIVE, _norm_schema e _norm_text estão importados


def create_db_structure(
    db: Session,
    *,
    db_connection_id: int,
    table_name: str,
    schema_name: Optional[str] = None,
    description: Optional[str] = None,
    engine: Optional[str] = None,
    charset: Optional[str] = None,
    collation: Optional[str] = None,
) -> DBStructure:
    try:
        table_name = (table_name or "").strip()
        if not table_name:
            raise ValueError("O nome da tabela não pode estar vazio.")

        schema_name_n = _norm_schema(schema_name)

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.Engine,  # type: ignore
                    DBStructure.Charset,  # type: ignore
                    DBStructure.Collation,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.db_connection_id == db_connection_id)
            .filter(DBStructure.table_name == table_name)
        )
        if schema_name_n is None:
            q = q.filter(DBStructure.schema_name.is_(None))
        else:
            q = q.filter(DBStructure.schema_name == schema_name_n)

        existing = q.first()
        if existing:
            if existing.is_deleted:
                existing.is_deleted = False
                existing.status = STATUS_ACTIVE
                existing.description = _norm_text(description)
                existing.Engine = _norm_text(engine)
                existing.Charset = _norm_text(charset)
                existing.Collation = _norm_text(collation)

                # 🟢 CORREÇÃO 1: Efetiva a reativação da tabela no banco
                db.commit()
                return existing

            raise ValueError("Metadata da tabela já existe (DBStructure).")

        obj = DBStructure(
            db_connection_id=db_connection_id,
            table_name=table_name,
            schema_name=schema_name_n,
            description=_norm_text(description),
            Engine=_norm_text(engine),
            Charset=_norm_text(charset),
            Collation=_norm_text(collation),
            status=STATUS_ACTIVE,
            is_deleted=False,
        )

        db.add(obj)

        _safe_commit(db, action="criar estrutura", context=f"table={table_name}")

        return obj
    finally:
        # A sessão será fechada pelo caller
        pass


def update_db_structure_by_name(
    db: Session,
    *,
    db_connection_id: int,
    original_table_name: str,
    original_schema_name: Optional[str],
    new_table_name: str,
    new_schema_name: Optional[str],
    description: Optional[str],
    engine: Optional[str] = None,
    charset: Optional[str] = None,
    collation: Optional[str] = None,
) -> DBStructure:
    try:
        original_table_name = (original_table_name or "").strip()
        new_table_name = (new_table_name or "").strip()
        if not original_table_name:
            raise ValueError("original_table_name não pode estar vazio.")
        if not new_table_name:
            raise ValueError("new_table_name não pode estar vazio.")

        original_schema_n = _norm_schema(original_schema_name)
        new_schema_n = _norm_schema(new_schema_name)

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.table_name,  # type: ignore
                    DBStructure.schema_name,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.Engine,  # type: ignore
                    DBStructure.Charset,  # type: ignore
                    DBStructure.Collation,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.db_connection_id == db_connection_id)
            .filter(DBStructure.table_name == original_table_name)
            .filter(DBStructure.is_deleted.is_(False))
        )

        if original_schema_n is None:
            q = q.filter(DBStructure.schema_name.is_(None))
        else:
            q = q.filter(DBStructure.schema_name == original_schema_n)

        obj = q.first()
        if not obj:
            raise ValueError(
                "Metadata da tabela não encontrada para atualização (DBStructure)."
            )

        if (original_table_name != new_table_name) or (
            original_schema_n != new_schema_n
        ):
            q2 = (
                db.query(DBStructure.id)
                .filter(DBStructure.db_connection_id == db_connection_id)
                .filter(DBStructure.table_name == new_table_name)
                .filter(DBStructure.is_deleted.is_(False))
            )
            if new_schema_n is None:
                q2 = q2.filter(DBStructure.schema_name.is_(None))
            else:
                q2 = q2.filter(DBStructure.schema_name == new_schema_n)

            clash_id = q2.scalar()
            if clash_id and clash_id != obj.id:
                raise ValueError(
                    "Já existe uma tabela com esse nome/schema (conflito de metadata)."
                )

        obj.table_name = new_table_name
        obj.schema_name = new_schema_n
        obj.description = _norm_text(description)

        if engine is not None:
            obj.Engine = _norm_text(engine)
        if charset is not None:
            obj.Charset = _norm_text(charset)
        if collation is not None:
            obj.Collation = _norm_text(collation)

        obj.status = STATUS_ACTIVE
        obj.is_deleted = False

        _safe_commit(
            db, action="atualizar estrutura", context=f"table={new_table_name}"
        )

        return obj
    finally:
        # A sessão será fechada pelo caller
        pass


def soft_delete_db_structure_by_name(
    db: Session,
    *,
    db_connection_id: int,
    table_name: str,
    schema_name: Optional[str],
) -> None:
    try:
        table_name = (table_name or "").strip()
        if not table_name:
            raise ValueError("table_name não pode estar vazio.")

        # ✅ aqui já é leve, mas evita carregar relações
        q = (
            db.query(DBStructure)
            .options(
                load_only(DBStructure.id, DBStructure.is_deleted, DBStructure.status),  # type: ignore
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.db_connection_id == db_connection_id)
            .filter(DBStructure.table_name == table_name)
        )

        obj = q.first()
        if not obj:
            log_message(
                f"⚠️ DBStructure não encontrado para soft delete: conn={db_connection_id} table={table_name} schema={schema_name}",
                level="warning",
            )
            return

        obj.is_deleted = True
        obj.status = STATUS_DELETED
        db.commit()
    finally:
        # A sessão será fechada pelo caller
        pass


def get_structure_by_id(
    db: Session,
    structure_id: int,
    *,
    visible_status: Optional[Sequence[str]] = None,
    include_deleted_flag: bool = False,
) -> Optional[DBStructure]:
    try:
        status_list = _normalize_visible_status(
            visible_status, VISIBLE_STRUCTURE_STATUS
        )

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.db_connection_id,  # type: ignore
                    DBStructure.table_name,  # type: ignore
                    DBStructure.schema_name,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                    DBStructure.created_at,  # type: ignore
                    DBStructure.updated_at,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.id == structure_id)
            .filter(_status_filter(DBStructure.status, status_list))
        )

        if not include_deleted_flag:
            q = q.filter(DBStructure.is_deleted.is_(False))

        return q.first()
    finally:
        # A sessão será fechada pelo caller
        pass


def get_structure_by_id_and_name(
    db: Session,
    connection_id: int,
    table_name: str,
    *,
    visible_status: Optional[Sequence[str]] = None,
    include_deleted_flag: bool = False,
) -> Optional[DBStructure]:
    try:
        status_list = _normalize_visible_status(
            visible_status, VISIBLE_STRUCTURE_STATUS
        )

        q = (
            db.query(DBStructure)
            .options(
                load_only(
                    DBStructure.id,  # type: ignore
                    DBStructure.db_connection_id,  # type: ignore
                    DBStructure.table_name,  # type: ignore
                    DBStructure.schema_name,  # type: ignore
                    DBStructure.description,  # type: ignore
                    DBStructure.status,  # type: ignore
                    DBStructure.is_deleted,  # type: ignore
                    DBStructure.created_at,  # type: ignore
                    DBStructure.updated_at,  # type: ignore
                ),
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(
                DBStructure.db_connection_id == connection_id,
                DBStructure.table_name == table_name,
            )
            .filter(_status_filter(DBStructure.status, status_list))
        )

        if not include_deleted_flag:
            q = q.filter(DBStructure.is_deleted.is_(False))

        return q.first()
    finally:
        # A sessão será fechada pelo caller
        pass


# ✅ retorna apenas o ID (já está eficiente)
def get_structure_id_by_connection_and_table(
    db: Session,
    connection_id: int,
    table_name: str,
    *,
    visible_status: Optional[Sequence[str]] = None,
    include_deleted_flag: bool = False,
) -> Optional[int]:
    try:
        status_list = _normalize_visible_status(
            visible_status, VISIBLE_STRUCTURE_STATUS
        )

        q = (
            db.query(DBStructure.id)
            .filter(
                DBStructure.db_connection_id == connection_id,
                DBStructure.table_name == table_name,
            )
            .filter(_status_filter(DBStructure.status, status_list))
        )

        if not include_deleted_flag:
            q = q.filter(DBStructure.is_deleted.is_(False))

        return q.scalar()
    finally:
        # A sessão será fechada pelo caller
        pass


def delete_structure(db: Session, structure_id: int) -> bool:
    try:
        structure = (
            db.query(DBStructure)
            .options(
                load_only(DBStructure.id),  # type: ignore
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(DBStructure.id == structure_id)
            .first()
        )
        if not structure:
            log_message(
                f"❌ Estrutura não encontrada para exclusão: ID {structure_id}", "error"
            )
            return False

        db.delete(structure)
        _safe_commit(db, action="deletar estrutura", context=f"id={structure_id}")
        log_message(f"⚠️ Estrutura com ID {structure_id} deletada.", "warning")
        return True
    finally:
        # A sessão será fechada pelo caller
        pass


def delete_structure_by_name(
    db: Session, structure_name: str, connection_id: int
) -> bool:
    try:
        structure = (
            db.query(DBStructure)
            .options(
                load_only(DBStructure.id),  # type: ignore
                noload(DBStructure.fields),
                noload(DBStructure.connection),
            )
            .filter(
                DBStructure.table_name == structure_name,
                DBStructure.db_connection_id == connection_id,
            )
            .first()
        )
        if not structure:
            log_message(
                f"❌ Estrutura não encontrada para exclusão: nome {structure_name}, conexão {connection_id}",
                "error",
            )
            return False

        db.delete(structure)
        _safe_commit(
            db,
            action="deletar estrutura",
            context=f"name={structure_name}, connection_id={connection_id}",
        )
        log_message(f"⚠️ Estrutura com nome {structure_name} deletada.", "warning")
        return True
    finally:
        # A sessão será fechada pelo caller
        pass


# ==============================================================================
# CRUD: DBField
# ==============================================================================


def create_db_field(db: Session, field_in: DBFieldCreate, structure_id: int) -> DBField:
    try:
        existing = (
            db.query(DBField)
            .options(
                load_only(DBField.id),  # type: ignore
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(DBField.structure_id == structure_id, DBField.name == field_in.name)
            .first()
        )

        data = field_in.model_dump(exclude_unset=True, exclude_none=True)
        data.pop("structure_id", None)

        if existing:
            _apply_model_data(existing, data)
            _safe_commit(
                db,
                action="atualizar campo",
                context=f"structure_id={structure_id} field={field_in.name}",
            )
            db.refresh(existing)
            return existing

        obj = DBField(structure_id=structure_id, **data)
        db.add(obj)
        _safe_commit(
            db,
            action="criar campo",
            context=f"structure_id={structure_id} field={field_in.name}",
        )
        db.refresh(obj)
        return obj
    finally:
        # A sessão será fechada pelo caller
        pass


def get_fields_by_structure_pk(
    db: Session,
    structure_id: int,
    *,
    visible_status: Optional[Sequence[str]] = None,
) -> Optional[DBField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_FIELD_STATUS)

        base = (
            db.query(DBField)
            .options(
                load_only(
                    DBField.id,  # type: ignore
                    DBField.structure_id,  # type: ignore
                    DBField.name,  # type: ignore
                    DBField.status,  # type: ignore
                    DBField.is_primary_key,  # type: ignore
                    DBField.is_unique,  # type: ignore
                    DBField.is_nullable,  # type: ignore
                    DBField.is_auto_increment,  # type: ignore
                ),
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(DBField.structure_id == structure_id)
            .filter(_status_filter(DBField.status, status_list))
        )

        col = base.filter(DBField.is_primary_key.is_(True)).first()
        if col:
            return col

        col = base.filter(
            DBField.is_unique.is_(True), DBField.is_nullable.is_(False)
        ).first()
        if col:
            return col

        col = base.filter(DBField.is_auto_increment.is_(True)).first()
        if col:
            return col

        return base.first()
    finally:
        # A sessão será fechada pelo caller
        pass


def get_fields_by_structure(
    db: Session,
    structure_id: int,
    *,
    visible_status: Optional[Sequence[str]] = None,
) -> List[DBField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_FIELD_STATUS)
        return (
            db.query(DBField)
            .options(
                load_only(
                    DBField.id,  # type: ignore
                    DBField.structure_id,  # type: ignore
                    DBField.name,  # type: ignore
                    DBField.type,  # type: ignore
                    DBField.status,  # type: ignore
                    DBField.is_nullable,  # type: ignore
                    DBField.default_value,  # type: ignore
                    DBField.is_primary_key,  # type: ignore
                    DBField.is_unique,  # type: ignore
                    DBField.is_foreign_key,  # type: ignore
                    DBField.is_auto_increment,  # type: ignore
                    DBField.length,  # type: ignore
                    DBField.precision,  # type: ignore
                    DBField.scale,  # type: ignore
                    DBField.comment,  # type: ignore
                    DBField.referenced_table,  # type: ignore
                    DBField.referenced_field,  # type: ignore
                    DBField.fk_on_delete,  # type: ignore
                    DBField.fk_on_update,  # type: ignore
                    DBField.created_at,  # type: ignore
                    DBField.updated_at,  # type: ignore
                ),
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(DBField.structure_id == structure_id)
            .filter(_status_filter(DBField.status, status_list))
            .all()
        )
    finally:
        # A sessão será fechada pelo caller
        pass


def get_field_by_structure_and_name(
    db: Session,
    structure_id: int,
    col_name: str,
    *,
    visible_status: Optional[Sequence[str]] = None,
) -> Optional[DBField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_FIELD_STATUS)
        return (
            db.query(DBField)
            .options(
                load_only(
                    DBField.id,  # type: ignore
                    DBField.structure_id,  # type: ignore
                    DBField.name,  # type: ignore
                    DBField.type,  # type: ignore
                    DBField.status,  # type: ignore
                    DBField.is_nullable,  # type: ignore
                    DBField.default_value,  # type: ignore
                    DBField.is_primary_key,  # type: ignore
                    DBField.is_unique,  # type: ignore
                    DBField.is_foreign_key,  # type: ignore
                    DBField.is_auto_increment,  # type: ignore
                    DBField.length,  # type: ignore
                    DBField.precision,  # type: ignore
                    DBField.scale,  # type: ignore
                    DBField.comment,  # type: ignore
                    DBField.referenced_table,  # type: ignore
                    DBField.referenced_field,  # type: ignore
                    DBField.fk_on_delete,  # type: ignore
                    DBField.fk_on_update,  # type: ignore
                    DBField.created_at,  # type: ignore
                    DBField.updated_at,  # type: ignore
                ),
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(
                DBField.structure_id == structure_id,
                DBField.name == col_name,
            )
            .filter(_status_filter(DBField.status, status_list))
            .first()
        )
    finally:
        # A sessão será fechada pelo caller
        pass


def update_fields_by_tablename(
    db: Session,
    structure_id: int,
    original_name: str,
    field_update: DBFieldCreate,
    *,
    exclude_none: bool = True,
    visible_status: Optional[Sequence[str]] = None,
) -> Optional[DBField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_FIELD_STATUS)

        field = (
            db.query(DBField)
            .options(
                load_only(DBField.id),  # type: ignore
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(DBField.structure_id == structure_id, DBField.name == original_name)
            .filter(_status_filter(DBField.status, status_list))
            .first()
        )
        if not field:
            return None

        data = field_update.model_dump(exclude_unset=True, exclude_none=exclude_none)
        data.pop("structure_id", None)

        _apply_model_data(field, data)

        _safe_commit(
            db,
            action="atualizar campo",
            context=f"structure_id={structure_id} field={original_name}",
        )
        db.refresh(field)
        return field
    finally:
        # A sessão será fechada pelo caller
        pass


def delete_field(db: Session, field_id: int) -> bool:
    try:
        obj = (
            db.query(DBField)
            .options(
                load_only(DBField.id),  # type: ignore
                noload(DBField.structure),
                noload(DBField.enum_values),
            )
            .filter(DBField.id == field_id)
            .first()
        )
        if not obj:
            log_message(
                f"❌ Campo não encontrado para exclusão: ID {field_id}", "error"
            )
            return False

        db.delete(obj)
        _safe_commit(db, action="deletar campo", context=f"id={field_id}")
        log_message(f"⚠️ Campo com ID {field_id} deletado.", "warning")
        return True
    finally:
        # A sessão será fechada pelo caller
        pass


def soft_delete_field_name(db: Session, field_name: str, structure_id: int) -> bool:
    try:
        updated = (
            db.query(DBField)
            .filter(DBField.name == field_name, DBField.structure_id == structure_id)
            .update({"status": STATUS_DELETED}, synchronize_session=False)
        )
        if updated <= 0:
            log_message(
                f"❌ Nenhum campo encontrado com nome '{field_name}' na estrutura ID {structure_id} para soft delete.",
                "error",
            )
            return False

        _safe_commit(
            db,
            action="soft delete campo",
            context=f"name={field_name} structure_id={structure_id}",
        )
        log_message(
            f"⚠️ {updated} campo(s) com nome '{field_name}' marcado(s) como '{STATUS_DELETED}' na estrutura ID {structure_id}.",
            "warning",
        )
        return True
    finally:
        # A sessão será fechada pelo caller
        pass


# ==============================================================================
# CRUD: DBEnumField
# ==============================================================================


def create_enum_field(db: Session, data: DBEnumField) -> DBEnumField:
    try:
        existing = (
            db.query(DBEnumField)
            .options(
                # load_only(DBEnumField.id),  # type: ignore
                noload(DBEnumField.field),
            )
            .filter(
                DBEnumField.field_id == data.field_id,
                DBEnumField.value == data.value,
                DBEnumField.is_active.is_(True),
                _status_filter(DBEnumField.status, VISIBLE_ENUM_STATUS),
            )
            .first()
        )
        if existing:
            log_message(
                f"⚠️ Valor ENUM '{data.value}' já existe para field ID {data.field_id}",
                "warning",
            )
            return existing

        db.add(data)
        _safe_commit(
            db,
            action="criar enum",
            context=f"field_id={data.field_id} value={data.value}",
        )
        db.refresh(data)
        return data
    finally:
        # A sessão será fechada pelo caller
        pass


def get_enum_field(
    db: Session,
    field_id: int,
    valor: str,
    *,
    visible_status: Optional[Sequence[str]] = None,
    only_active: bool = True,
) -> Optional[DBEnumField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_ENUM_STATUS)

        q = (
            db.query(DBEnumField)
            .options(
                load_only(
                    # DBEnumField.id,  # type: ignore
                    DBEnumField.field_id,  # type: ignore
                    DBEnumField.value,  # type: ignore
                    DBEnumField.status,  # type: ignore
                    DBEnumField.is_active,  # type: ignore
                ),
                noload(DBEnumField.field),
            )
            .filter(DBEnumField.field_id == field_id, DBEnumField.value == valor)
            .filter(_status_filter(DBEnumField.status, status_list))
        )
        if only_active:
            q = q.filter(DBEnumField.is_active.is_(True))

        return q.first()
    finally:
        # A sessão será fechada pelo caller
        pass


def list_enum_fields_by_field(
    db: Session,
    field_id: int,
    *,
    visible_status: Optional[Sequence[str]] = None,
    only_active: bool = True,
) -> List[DBEnumField]:
    try:
        status_list = _normalize_visible_status(visible_status, VISIBLE_ENUM_STATUS)

        q = (
            db.query(DBEnumField)
            .options(
                load_only(
                    DBEnumField.field_id,  # type: ignore
                    DBEnumField.value,  # type: ignore
                    DBEnumField.status,  # type: ignore
                    DBEnumField.is_active,  # type: ignore
                ),
                noload(DBEnumField.field),
            )
            .filter(DBEnumField.field_id == field_id)
            .filter(_status_filter(DBEnumField.status, status_list))
        )
        if only_active:
            q = q.filter(DBEnumField.is_active.is_(True))

        return q.all()
    finally:
        # A sessão será fechada pelo caller
        pass


def soft_delete_enum_field(db: Session, field_id: int, valor: str) -> bool:
    try:
        updated = (
            db.query(DBEnumField)
            .filter(DBEnumField.field_id == field_id, DBEnumField.value == valor)
            .update(
                {"status": STATUS_DELETED, "is_active": False},
                synchronize_session=False,
            )
        )
        if updated <= 0:
            log_message(
                f"❌ Valor ENUM '{valor}' não encontrado para soft delete (field ID: {field_id})",
                "error",
            )
            return False

        _safe_commit(
            db, action="soft delete enum", context=f"field_id={field_id} value={valor}"
        )
        log_message(
            f"⚠️ Valor ENUM '{valor}' marcado como '{STATUS_DELETED}' (field ID: {field_id})",
            "warning",
        )
        return True
    finally:
        # A sessão será fechada pelo caller
        pass


def delete_enum_field(db: Session, field_id: int, valor: str) -> bool:
    try:
        obj = (
            db.query(DBEnumField)
            .options(
                load_only(DBEnumField.id),  # type: ignore
                noload(DBEnumField.field),
            )
            .filter(DBEnumField.field_id == field_id, DBEnumField.value == valor)
            .first()
        )
        if not obj:
            log_message(
                f"❌ Valor ENUM '{valor}' não encontrado para exclusão (field ID: {field_id})",
                "error",
            )
            return False

        db.delete(obj)
        _safe_commit(
            db, action="deletar enum", context=f"field_id={field_id} value={valor}"
        )
        log_message(
            f"🗑️ Valor ENUM '{valor}' removido com sucesso (field ID: {field_id})",
            "warning",
        )
        return True
    finally:
        # A sessão será fechada pelo caller
        pass
