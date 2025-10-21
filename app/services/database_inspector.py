from datetime import datetime, timezone
import traceback
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Engine, inspect, text
from sqlalchemy.orm import Session

from app.config.dependencies import EngineManager, get_session_by_connection_id
from app.cruds.dbstatistics_crud import (
    create_statistics,
    get_cached_row_count_all,
    get_statistics_by_connection,
    update_statistics,
)
from app.cruds.dbstructure_crud import (
    create_db_structure,
    get_db_structures,
    get_structure_by_id_and_name,
    update_db_structure,
)
# from app.models.dbstructure_models import DBStructure
from app.schemas.dbstructure_schema import DBStructureOut, DBStructureCreate
from app.schemas.dbstatistics_schema import DBStatisticsDict
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

# ============================================================

# 🔍 Funções de Estrutura de Banco de Dados

# ============================================================

def verificar_ou_atualizar_estrutura(
    db: Session, connection_id: int, table_name: str, schema_name: str | None = None
) -> DBStructureOut | None:  # Adicionado | None no retorno
    """
    Verifica se a estrutura da tabela já existe. Se não existir, cria.
    Se o schema estiver diferente ou description for nula, atualiza.
    """
    try:
        estrutura = get_structure_by_id_and_name(db, connection_id, table_name)
        structure = None  # Corrigido nome da variável (era "struture")
        
        if not estrutura:
            nova = DBStructureCreate(
                db_connection_id=connection_id,
                table_name=table_name,
                schema_name=schema_name,
                description="",
                created_at=datetime.now(timezone.utc),
            )
            structure = create_db_structure(db, nova)
            log_message(f"🆕 Estrutura registrada: {table_name}", "info")
            return DBStructureOut.model_validate(structure)  # Corrigido nome da variável

        update_needed = False
        if estrutura.schema_name != schema_name:
            estrutura.schema_name = schema_name
            update_needed = True
        if estrutura.description is None:
            estrutura.description = ""
            update_needed = True

        if update_needed:
            structure = update_db_structure(db, estrutura)  # Corrigido nome da variável
            log_message(f"🔄 Estrutura atualizada: {table_name}", "info")
        else:
            structure = estrutura  # Usar a estrutura existente se não houve atualização
            
        return DBStructureOut.model_validate(structure)  # Corrigido nome da variável

    except Exception as e:
        log_message(f"⚠️ Erro ao gerenciar estrutura da tabela '{table_name}': {e}", "warning")
        return None


def get_table_names_with_count(connection_id: int, id_user: int, db: Session):
    """
    Retorna os nomes das tabelas e a contagem de registros.
    Usa cache se disponível.
    """
    table_info = get_cached_row_count_all(db, connection_id)
    if table_info:
        log_message(f"🔍 Usando cache para {len(table_info)} tabelas na conexão {connection_id}", "info")
        return sorted(
            [{"name": row.table_name, "rowcount": row.row_count} for row in table_info],
            key=lambda x: x["name"].lower(),
        )

    try:
        engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        schema = getattr(inspector, "default_schema_name", None)
    except Exception as e:
        log_message(f"❌ Erro ao iniciar inspeção: {e}", "error")
        return []

    if not table_names:
        log_message("⚠️ Nenhuma tabela encontrada na conexão.", "warning")
        return []

    table_info = []
    with engine.connect() as conn:
        for table in table_names:
            verificar_ou_atualizar_estrutura(db, connection_id, table, schema)
            table_info.append({"name": table, "rowcount": -1})

    return sorted(table_info, key=lambda x: x["name"].lower())


def get_table_names(connection_id: int, id_user: int, db: Session):
    """
    Retorna todas as tabelas e views de uma conexão, atualizando estruturas no banco local.
    """
    structures = get_db_structures(db, connection_id)
    if structures:
        return [s.table_name for s in structures]

    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)
    inspector = inspect(engine)

    try:
        schemas = inspector.get_schema_names()
    except Exception as e:
        log_message(f"⚠️ Erro ao obter schemas: {e}", "warning")
        schemas = [getattr(inspector, "default_schema_name", None)]

    all_table_names = []

    for schema in schemas:
        try:
            tables = inspector.get_table_names(schema=schema)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter tabelas do schema '{schema}': {e}", "warning")
            tables = []

        for table in tables:
            verificar_ou_atualizar_estrutura(db, connection_id, table, schema)
        all_table_names.extend(tables)

        try:
            views = inspector.get_view_names(schema=schema)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter views do schema '{schema}': {e}", "warning")
            views = []
        all_table_names.extend(views)

    return all_table_names


def get_strutures_names(connection_id: int, id_user: int, db: Session) -> list[DBStructureOut]:
    """
    Retorna todas as tabelas e views de uma conexão, atualizando estruturas no banco local.
    """
    structures = get_db_structures(db, connection_id)
    if structures:
        return [DBStructureOut.model_validate(s) for s in structures]

    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)
    inspector = inspect(engine)

    try:
        schemas = inspector.get_schema_names()
    except Exception as e:
        log_message(f"⚠️ Erro ao obter schemas: {e}", "warning")
        schemas = [getattr(inspector, "default_schema_name", None)]

    all_table_names: list[DBStructureOut] = []

    for schema in schemas:
        try:
            tables = inspector.get_table_names(schema=schema)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter tabelas do schema '{schema}': {e}", "warning")
            tables = []
        tablesStructure = []  # Corrigido nome da variável
        for table in tables:
            structure = verificar_ou_atualizar_estrutura(db, connection_id, table, schema)
            if structure:  # Verificar se não é None
                tablesStructure.append(structure)
                
        all_table_names.extend(tablesStructure)
        viewsStructure = []  # Corrigido nome da variável
        try:
            views = inspector.get_view_names(schema=schema)
            
            for v in views:
                structure = verificar_ou_atualizar_estrutura(db, connection_id, v, schema)  # Corrigido: era "views" em vez de "v"
                if structure:  # Verificar se não é None
                    viewsStructure.append(structure)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter views do schema '{schema}': {e}", "warning")
        all_table_names.extend(viewsStructure)

    return all_table_names


# ============================================================

# 📊 Estatísticas e Contagem de Tabelas

# ============================================================

def get_table_count(connection_id: int, table_name: str, db: Session, id_user: int) -> int:
    """
    Retorna a contagem de registros de uma tabela.
    Retorna 0 se for view. Retorna -1 em caso de erro.
    """
    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)

    try:
        inspector = inspect(engine)
        if table_name in inspector.get_view_names(schema=inspector.default_schema_name):
            log_message(f"ℹ️ '{table_name}' é uma VIEW, retornando 0.", "info")
            return 0

        with engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
            return count or 0

    except SQLAlchemyError as e:
        error_type = _lidar_com_erro_sql(e)
        log_message(f"⚠️ Erro ao contar registros da tabela '{table_name}': {error_type} - {e}", "error")
        return -1


# ============================================================

# 📈 Coleta e Atualização de Estatísticas

# ============================================================

def collect_statistics(engine: Engine, db_type: str) -> DBStatisticsDict:
    """
    Coleta estatísticas gerais do banco de dados de forma compatível com múltiplos SGBDs.
    """
    inspector = inspect(engine)
    dialect = db_type.lower()
    tables_name = inspector.get_table_names()

    stats: DBStatisticsDict = {
        "server_version": "Desconhecida",
        "table_count": len(tables_name),
        "view_count": len(inspector.get_view_names()),
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

        if dialect == "postgresql":
            stats["procedure_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'p'")).scalar() or 0
            stats["function_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'f'")).scalar() or 0
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal")).scalar() or 0
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_indexes")).scalar() or 0

        elif dialect == "mysql":
            stats["procedure_count"] = conn.execute(
                text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")
            ).scalar() or 0
            stats["function_count"] = conn.execute(
                text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='FUNCTION'")
            ).scalar() or 0
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.TRIGGERS")).scalar() or 0
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.STATISTICS")).scalar() or 0

        elif dialect == "sqlite":
            stats["trigger_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")
            ).scalar() or 0
            stats["index_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
            ).scalar() or 0

        elif dialect in ["mssql", "sql server", "sqlserver"]:
            stats["procedure_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.procedures")).scalar() or 0
            stats["function_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN', 'TF', 'IF')")
            ).scalar() or 0
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.triggers")).scalar() or 0
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
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM ALL_TRIGGERS")).scalar() or 0
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM ALL_INDEXES")).scalar() or 0

    return stats


def save_or_update_statistics(connection_id: int, stats: dict, db: Session):
    """
    Salva ou atualiza estatísticas no banco de dados.
    """
    from app.schemas.dbstatistics_schema import DBStatisticsCreate, DBStatisticsUpdate

    existing = get_statistics_by_connection(db, connection_id)
    if not existing:
        data_create = DBStatisticsCreate(
            db_connection_id=connection_id,
            **{
                k: stats[k]
                for k in DBStatisticsCreate.__annotations__
                if k in stats and k != "db_connection_id"
            },
        )
        log_message(f"🆕 Criando novas estatísticas para a conexão {connection_id}.", "info")
        return create_statistics(db, data_create) or "created"

    changed = any(
        getattr(existing, key, None) != stats.get(key)
        for key in [
            "table_count",
            "view_count",
            "procedure_count",
            "function_count",
            "trigger_count",
            "index_count",
            "tables_connected",
        ]
    )

    if changed:
        data_update = DBStatisticsUpdate(
            **{k: stats[k] for k in DBStatisticsUpdate.__annotations__ if k in stats}
        )
        update_statistics(db, connection_id, data_update)
        return "updated"

    log_message(f"ℹ️ Nenhuma mudança nas estatísticas da conexão {connection_id}.", "info")
    return "unchanged"


def sync_connection_statistics(id_user: int, db: Session) -> dict | None:
    """
    Sincroniza estatísticas da conexão: coleta e armazena se necessário.
    """
    try:
        engine, connection = ConnectionManager.ensure_connection(db, id_user)
        existing = get_statistics_by_connection(db, connection.id)
        if existing:
            return existing

        stats = collect_statistics(engine, connection.type)
        if not stats:
            log_message(f"⚠️ Nenhuma estatística coletada para a conexão {connection.id}.", "warning")
            return None

        log_message(f"📊 Estatísticas coletadas: {stats}", "info")
        action = save_or_update_statistics(connection.id, stats, db)

        log_message(f"✅ Estatísticas '{action}' registradas para conexão ID={connection.id}.", "success")
        stats["connection_name"] = connection.name
        return stats

    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        stack_trace = traceback.format_exc()
        connection_id = getattr(connection, 'id', '?') if 'connection' in locals() else '?'
        log_message(
            f"❌ Erro ao sincronizar estatísticas da conexão ID={connection_id}, usuário ID={id_user}:\n"
            f"Tipo: {error_type}\nMensagem: {error_message}\nStackTrace:\n{stack_trace}",
            "error",
        )
        return None