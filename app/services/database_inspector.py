from __future__ import annotations

import traceback
from typing import Dict, List, Optional, Any, cast

from sqlalchemy import Engine, inspect, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import (
    SQLAlchemyError
)

from app.config.dependencies import get_session_by_connection_id
from app.config.engine_manager_cache import EngineManager
from app.cruds.dbstatistics_crud import create_statistics, get_cached_row_count_all, get_statistics_by_connection, update_statistics
from app.cruds.dbstructure_crud import (
    create_db_structure,
    get_db_structures,
    get_structure_by_id_and_name,
    update_db_structure,
)
from app.models.dbstructure_models import DBField
from app.schemas.dbstatistics_schema import DBStatisticsDict
from app.schemas.dbstructure_schema import DBStructureOut
from app.services.field_info import get_fields_of_table, get_fields_of_tables_bulk, sincronizar_metadados_da_tabela_simple
from app.ultils.Database_error_logger import _lidar_com_erro_sql
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message


# ============================================================
# Helpers
# ============================================================

def _get_engine(connection_id: int, user_id: int, db: Session) -> Engine:
    """
    Obtém o engine ativo ou recria a sessão a partir da conexão.
    """
    engine: Optional[Engine] = EngineManager.get(user_id)

    if engine is not None:
        return engine

    return get_session_by_connection_id(connection_id, db)


def _get_default_schema(engine: Engine, inspector: Any) -> Optional[str]:
    dialect = engine.dialect.name.lower()

    if dialect == "postgresql":
        return "public"

    if dialect in ("mssql", "sqlserver"):
        return "dbo"

    if dialect in ("mysql", "mariadb"):
        return engine.url.database

    if dialect == "oracle":
        return engine.url.username.upper() if engine.url.username else None

    if dialect == "sqlite":
        return None

    return getattr(inspector, "default_schema_name", None)


def _get_schemas(engine: Engine, inspector: Any) -> list[Optional[str]]:
    dialect = engine.dialect.name.lower()

    if dialect == "sqlite":
        return [None]

    try:
        schemas: list[str] = inspector.get_schema_names()

        if not schemas:
            raise ValueError("O banco não retornou schemas")

        return cast(list[Optional[str]], schemas)

    except Exception as e:
        default_schema = _get_default_schema(engine, inspector)
        log_message(
            f"Erro ao obter schemas: {e}. Usando schema padrão '{default_schema}' para '{dialect}'",
            "warning",
        )
        return [default_schema]


def _list_tables_and_views(
    engine: Engine,
    inspector: Any,
) -> list[tuple[str, Optional[str], str]]:

    results: list[tuple[str, Optional[str], str]] = []
    seen: set[tuple[str, Optional[str], str]] = set()

    dialect = engine.dialect.name.lower()

    def _add_items(names: list[str], schema: Optional[str], kind: str):

        for name in names:

            item = (name, schema, kind)

            if item not in seen:
                seen.add(item)
                results.append(item)

    if dialect == "sqlite":

        try:
            _add_items(inspector.get_table_names(), None, "table")

        except Exception as e:
            log_message(f"Erro ao obter tabelas SQLite: {e}", "warning")

        try:
            _add_items(inspector.get_view_names(), None, "view")

        except Exception as e:
            log_message(f"Erro ao obter views SQLite: {e}", "warning")

        return results

    schemas = _get_schemas(engine, inspector)

    for schema in schemas:

        try:
            _add_items(inspector.get_table_names(schema=schema), schema, "table")

        except Exception as e:
            log_message(
                f"Erro ao obter tabelas schema '{schema}': {e}",
                "warning",
            )

        try:
            _add_items(inspector.get_view_names(schema=schema), schema, "view")

        except Exception as e:
            log_message(
                f"Erro ao obter views schema '{schema}': {e}",
                "warning",
            )

    return results

def verificar_ou_atualizar_estrutura(
    db: Session,
    connection_id: int,
    table_name: str,
    schema_name: Optional[str] = None,
) -> Optional[DBStructureOut]:

    try:
        if connection_id <= 0:
            raise ValueError("connection_id inválido")

        if not table_name.strip():
            raise ValueError("table_name inválido")

        structure = create_db_structure(
            db=db,
            db_connection_id=connection_id,
            table_name=table_name,
            schema_name=schema_name,
            description="",
        )
        log_message(
            f"Estrutura registrada | connection_id={connection_id} | table={table_name} | schema={schema_name}",
            "info",
        )
        return DBStructureOut.model_validate(structure)

    except ValueError as e:
        log_message(
            f"Erro validação estrutura | connection_id={connection_id} | table={table_name} | erro={e}",
            "warning",
        )
        return None
    except Exception as e:
        log_message(
            f"Erro estrutura | connection_id={connection_id} | table={table_name} | erro={e} | trace={traceback.format_exc()}",
            "warning",
        )
        return None
# ============================================================
# Tabelas
# ============================================================

def get_table_names(
    connection_id: int,
    id_user: int,
    db: Session,
) -> list[str]:

    try:

        if connection_id <= 0:
            raise ValueError("connection_id inválido")

        if id_user <= 0:
            raise ValueError("id_user inválido")

        engine = _get_engine(connection_id, id_user, db)

        inspector: Any = inspect(engine)

        items = _list_tables_and_views(engine, inspector)

        seen: set[str] = set()
        names: list[str] = []
        # print("line: 244")
        for name, schema, obj_type in items:
            # print("\nname:",name,"\nschema:",schema,"\nobj_type:",obj_type)

            if not name:

                log_message(
                    f"Nome inválido ignorado | connection_id={connection_id} | schema={schema} | type={obj_type}",
                    "warning",
                )

                continue

            try:

                st=verificar_ou_atualizar_estrutura(
                    db,
                    connection_id,
                    name,
                    schema,
                )
                # print(st)

            except Exception as e:

                log_message(
                    f"Erro atualizar estrutura | table={name} | erro={e}",
                    "warning",
                )

            if name not in seen:
                seen.add(name)
                names.append(name)

        return sorted(names, key=str.lower)

    except ValueError as e:

        log_message(
            f"Erro validação get_table_names | connection_id={connection_id} | user={id_user} | erro={e}",
            "warning",
        )

        return []

    except Exception as e:

        log_message(
            f"Erro get_table_names | connection_id={connection_id} | user={id_user} | erro={e} | trace={traceback.format_exc()}",
            "error",
        )

        return []


def get_table_names_with_count(connection_id: int, id_user: int, db: Session) -> list[dict]:
    """
    Retorna nomes das tabelas com rowcount.
    Usa cache se disponível.
    """
    table_info = get_cached_row_count_all( connection_id)
    if table_info:
        log_message(
            f"🔍 Usando cache para {len(table_info)} tabelas na conexão {connection_id}",
            "info",
        )
        return sorted(
            [{"name": row.table_name, "rowcount": row.row_count} for row in table_info],
            key=lambda x: x["name"].lower(),
        )

    try:
        names = get_table_names(connection_id, id_user, db)
        if not names:
            log_message("⚠️ Nenhuma tabela encontrada na conexão.", "warning")
            return []

        return [{"name": name, "rowcount": -1} for name in sorted(names, key=str.lower)]

    except Exception as e:
        log_message(f"❌ Erro ao montar lista de tabelas com contagem: {e}", "error")
        return []


def get_strutures_names(
    connection_id: int,
    id_user: int,
    db: Session,
) -> list[DBStructureOut]:
    """
    Retorna tabelas e views como estruturas, sincronizando metadados.
    """
    structures = get_db_structures(db, connection_id)
    if structures:
        for item in structures:
            sincronizar_metadados_da_tabela_simple(
                db=db,
                table_name=item.table_name,
                user_id=id_user,
                connection_id=connection_id,
            )
        return [DBStructureOut.model_validate(s) for s in structures]

    try:
        engine = _get_engine(connection_id, id_user, db)
        inspector = inspect(engine)

        items = _list_tables_and_views(engine, inspector)
        all_structures: list[DBStructureOut] = []

        for name, schema, _obj_type in items:
            structure = verificar_ou_atualizar_estrutura(db, connection_id, name, schema)
            if not structure:
                continue

            try:
                sincronizar_metadados_da_tabela_simple(
                    db=db,
                    table_name=structure.table_name,
                    user_id=id_user,
                    connection_id=connection_id,
                )
            except Exception as e:
                log_message(
                    f"⚠️ Erro ao sincronizar metadados da tabela '{structure.table_name}': {e}",
                    "warning",
                )

            all_structures.append(structure)

        return all_structures

    except Exception as e:
        log_message(
            f"❌ Erro ao obter estruturas da conexão {connection_id}: {e}\n{traceback.format_exc()}",
            "error",
        )
        return []


def get_strutures_names_only(
    connection_id: int,
    id_user: int,
    db: Session,
) -> list[DBStructureOut]:
    """
    Retorna tabelas e views da conexão como estruturas, sem sincronizar metadados.
    """
    structures = get_db_structures(db, connection_id)
    if structures:
        print(f"structures: {structures}")
        return [DBStructureOut.model_validate(s) for s in structures]

    try:
        engine = _get_engine(connection_id, id_user, db)
        inspector = inspect(engine)

        items = _list_tables_and_views(engine, inspector)
        all_structures: list[DBStructureOut] = []

        for name, schema, _obj_type in items:
            structure = verificar_ou_atualizar_estrutura(db, connection_id, name, schema)
            if structure:
                all_structures.append(structure)
        # print("all_structures",all_structures)
        return all_structures

    except Exception as e:
        log_message(
            f"❌ Erro ao obter estruturas simples da conexão {connection_id}: {e}\n{traceback.format_exc()}",
            "error",
        )
        return []


# ============================================================
# Campos
# ============================================================

def get_fields_info_cached(
    connection_id: int,
    table_name: str,
    user_id: int,
    db: Session,
):
    try:
        return get_fields_of_table(
            db=db,
            table_name=table_name,
            user_id=user_id,
            connection_id=connection_id,
        )
    except Exception as e:
        log_message(
            f"❌ Erro ao obter informações de campos para '{table_name}': {e}",
            "error",
        )
        return []


def get_fields_info_bulk_cached(
    connection_id: int,
    table_names: list[str],
    user_id: int,
    db: Session,
)-> Dict[str, List[DBField]]:
    try:
        return get_fields_of_tables_bulk(
            db=db,
            table_names=table_names,
            user_id=user_id,
            connection_id=connection_id,
        )
    except Exception as e:
        log_message(
            f"❌ Erro ao obter informações de campos para '{table_names}': {e}\n{traceback.format_exc()}",
            "error",
        )
        raise


# ============================================================
# Contagem
# ============================================================

def get_table_count(
    connection_id: int,
    table_name: str,
    db: Session,
    id_user: int,
) -> int:
    """
    Retorna a contagem de registros de uma tabela.
    Retorna 0 se for view.
    Retorna -1 em caso de erro.
    """
    try:
        engine = _get_engine(connection_id, id_user, db)
        inspector = inspect(engine)

        view_names = set()
        try:
            for _, schema, obj_type in _list_tables_and_views(engine, inspector):
                if obj_type == "view":
                    view_names.add((table_name, schema))
        except Exception:
            pass

        default_schema = getattr(inspector, "default_schema_name", None)
        if (table_name, default_schema) in view_names or any(name == table_name for name, _schema in [(n, s) for n, s, t in _list_tables_and_views(engine, inspector) if t == "view"]):
            log_message(f"ℹ️ '{table_name}' é uma VIEW, retornando 0.", "info")
            return 0

        with engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
            return count or 0

    except SQLAlchemyError as e:
        error_type = _lidar_com_erro_sql(e)
        log_message(
            f"⚠️ Erro ao contar registros da tabela '{table_name}': {error_type} - {e}",
            "error",
        )
        return -1
    except Exception as e:
        log_message(
            f"⚠️ Erro inesperado ao contar registros da tabela '{table_name}': {e}",
            "error",
        )
        return -1


# ============================================================
# Estatísticas
# ============================================================

def collect_statistics(engine: Engine, db_type: str) -> DBStatisticsDict:
    """
    Coleta estatísticas gerais do banco, compatível com múltiplos SGBDs.
    """
    inspector = inspect(engine)
    dialect = db_type.lower()

    tables_name = inspector.get_table_names()
    views_name = inspector.get_view_names()

    stats: DBStatisticsDict = {
        "server_version": "Desconhecida",
        "table_count": len(tables_name),
        "view_count": len(views_name),
        "procedure_count": 0,
        "function_count": 0,
        "trigger_count": 0,
        "index_count": 0,
        "tables_connected": len(tables_name),
        "queries_today": 0,
        "records_analyzed": 0,
        "connection_name": "",
        "db_connection_id": 0,
    }

    version_query = {
        "postgresql": "SHOW server_version",
        "mysql": "SELECT VERSION()",
        "sqlite": "SELECT sqlite_version()",
        "mssql": "SELECT @@VERSION",
        "sql server": "SELECT @@VERSION",
        "sqlserver": "SELECT @@VERSION",
        "oracle": "SELECT * FROM v$version WHERE banner LIKE 'Oracle Database%'",
    }.get(dialect)

    with engine.connect() as conn:
        if version_query:
            try:
                result = conn.execute(text(version_query)).scalar()
                stats["server_version"] = result if result else "Desconhecida"
            except Exception as e:
                log_message(f"⚠️ Erro ao obter versão do servidor: {e}", "warning")

        try:
            if dialect == "postgresql":
                stats["procedure_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'p'")
                ).scalar() or 0
                stats["function_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'f'")
                ).scalar() or 0
                stats["trigger_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal")
                ).scalar() or 0
                stats["index_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM pg_indexes")
                ).scalar() or 0

            elif dialect == "mysql":
                stats["procedure_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")
                ).scalar() or 0
                stats["function_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='FUNCTION'")
                ).scalar() or 0
                stats["trigger_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.TRIGGERS")
                ).scalar() or 0
                stats["index_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.STATISTICS")
                ).scalar() or 0

            elif dialect == "sqlite":
                stats["trigger_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")
                ).scalar() or 0
                stats["index_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
                ).scalar() or 0

            elif dialect in ("mssql", "sql server", "sqlserver"):
                stats["procedure_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sys.procedures")
                ).scalar() or 0
                stats["function_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN', 'TF', 'IF')")
                ).scalar() or 0
                stats["trigger_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sys.triggers")
                ).scalar() or 0
                stats["index_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM sys.indexes WHERE name IS NOT NULL")
                ).scalar() or 0

            elif dialect == "oracle":
                stats["procedure_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE'")
                ).scalar() or 0
                stats["function_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'FUNCTION'")
                ).scalar() or 0
                stats["trigger_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM ALL_TRIGGERS")
                ).scalar() or 0
                stats["index_count"] = conn.execute(
                    text("SELECT COUNT(*) FROM ALL_INDEXES")
                ).scalar() or 0

        except Exception as e:
            log_message(f"⚠️ Erro ao coletar estatísticas específicas do banco: {e}", "warning")

    return stats


def save_or_update_statistics(connection_id: int, stats: dict, db: Session):
    """
    Salva ou atualiza estatísticas da conexão.
    """
    from app.schemas.dbstatistics_schema import DBStatisticsCreate, DBStatisticsUpdate

    existing = get_statistics_by_connection( connection_id)

    if not existing:
        data_create = DBStatisticsCreate(
            db_connection_id=connection_id,
            **{
                k: stats[k]
                for k in DBStatisticsCreate.__annotations__
                if k in stats and k != "db_connection_id"
            },
        )
        log_message(f"🆕 Criando estatísticas para a conexão {connection_id}.", "info")
        return create_statistics(data_create) or "created"

    watched_fields = [
        "table_count",
        "view_count",
        "procedure_count",
        "function_count",
        "trigger_count",
        "index_count",
        "tables_connected",
    ]

    changed = any(getattr(existing, key, None) != stats.get(key) for key in watched_fields)

    if changed:
        data_update = DBStatisticsUpdate(
            **{k: stats[k] for k in DBStatisticsUpdate.__annotations__ if k in stats}
        )
        update_statistics( connection_id, data_update)
        return "updated"

    log_message(
        f"ℹ️ Nenhuma mudança nas estatísticas da conexão {connection_id}.",
        "info",
    )
    return "unchanged"

def sync_connection_statistics(id_user: int, db: Session) -> dict | None:
    """
    Sincroniza estatísticas da conexão: coleta e persiste se necessário.
    """
    connection = None

    try:
        engine, connection = ConnectionManager.ensure_connection(db, id_user)
        
        # Verificação de segurança
        if not connection or not hasattr(connection, 'id'):
            log_message(f"❌ Conexão inválida para usuário ID={id_user}", "error")
            return None
            
        id_conn = cast(int, connection.id)

        existing = get_statistics_by_connection( id_conn)
        if existing:
            # Se for objeto SQLAlchemy, converter para dict
            if hasattr(existing, '__dict__'):
                return {k: v for k, v in existing.__dict__.items() 
                       if not k.startswith('_')}
            return existing

        stats = collect_statistics(engine, str(id_conn))
        if not stats:
            log_message(
                f"⚠️ Nenhuma estatística coletada para a conexão {connection.id}.",
                "warning",
            )
            return None

        log_message(f"📊 Estatísticas coletadas: {stats}", "info")
        action = save_or_update_statistics(id_conn, dict(stats), db)

        log_message(
            f"✅ Estatísticas '{action}' registradas para conexão ID={connection.id}.",
            "success",
        )

        # Adicionar metadados sem modificar stats original
        result = dict(stats)
        result["connection_name"] = connection.name
        result["db_connection_id"] = connection.id
        return result

    except Exception as e:
        connection_id = getattr(connection, "id", "?")
        log_message(
            f"❌ Erro ao sincronizar estatísticas da conexão ID={connection_id}, usuário ID={id_user}:\n"
            f"Tipo: {type(e).__name__}\n"
            f"Mensagem: {str(e)}\n"
            f"StackTrace:\n{traceback.format_exc()}",
            "error",
        )
        return None