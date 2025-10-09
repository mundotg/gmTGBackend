from datetime import datetime, timezone
import traceback
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Engine, inspect, text
from sqlalchemy.orm import Session
from app.config.dependencies import EngineManager, get_session_by_connection_id
from app.cruds.dbstatistics_crud import create_statistics, get_cached_row_count_all, get_statistics_by_connection, update_or_create_cache, update_statistics
from app.cruds.dbstructure_crud import create_db_structure, get_db_structures, get_structure_by_id_and_name, update_db_structure
from app.models.dbstructure_models import DBStructure
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

def verificar_ou_atualizar_estrutura(db: Session, connection_id: int, table_name: str, schema_name: str | None = None):
    """
    Verifica se a estrutura da tabela já existe. Se não existir, cria.
    Se o schema estiver diferente ou description for nula, atualiza.
    """
    try:
        estrutura = get_structure_by_id_and_name(db, connection_id, table_name)
        if not estrutura:
            nova = DBStructure(
                db_connection_id=connection_id,
                table_name=table_name,
                schema_name=schema_name,
                description="",
                created_at=datetime.now(timezone.utc),
            )
            create_db_structure(db, nova)
            log_message(f"🆕 Estrutura registrada: {table_name}", "info")
            return

        update_needed = False

        if estrutura.schema_name != schema_name:
            estrutura.schema_name = schema_name
            update_needed = True
        if estrutura.description is None:
            estrutura.description = ""
            update_needed = True

        if update_needed:
            update_db_structure(db, estrutura)
            log_message(f"🔄 Estrutura atualizada: {table_name}", "info")

    except Exception as e:
        log_message(f"⚠️ Erro ao gerenciar estrutura da tabela '{table_name}': {e}", "warning")

def get_table_names_with_count(connection_id: int, id_user: int, db: Session):
    table_info = get_cached_row_count_all(db, connection_id)

    if table_info:
        log_message(f"🔍 Usando cache para {len(table_info)} tabelas na conexão {connection_id}", "info")
        return sorted(
            [{"name": row.table_name, "rowcount": row.row_count} for row in table_info],
            key=lambda x: x["name"].lower()
        )

    try:
        engine = EngineManager.get(id_user)
        if not engine:
            engine = get_session_by_connection_id(connection_id, db)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        schema = inspector.default_schema_name or None
    except Exception as e:
        log_message(f"❌ Erro ao iniciar inspeção: {e}", "error")
        return []

    if not table_names:
        log_message("⚠️ Nenhuma tabela encontrada na conexão", "warning")
        return []

    table_info = []

    with engine.connect() as conn:
        for table in table_names:
            verificar_ou_atualizar_estrutura(db, connection_id, table, schema)

            try:
                query = text(f'SELECT COUNT(*) FROM "{table}"')
                count = conn.execute(query).scalar()
            except Exception as e:
                error_type = type(e).__name__
                error_message = str(e)
                stack_trace = traceback.format_exc()
                log_message(f"❌ Erro ao contar registros da tabela '{table}': "
                            f"Tipo: {error_type}\n"
                            f"Mensagem: {error_message}\n"
                            f"StackTrace:\n{stack_trace}", "error")
                count = -1

            table_info.append({"name": table, "rowcount": count})
            update_or_create_cache(db, connection_id, table, count)

    return sorted(table_info, key=lambda x: x["name"].lower())


def get_table_names(connection_id: int, id_user: int, db: Session):
    """
    Retorna todas as tabelas e views de uma conexão e atualiza a estrutura das tabelas no banco local.
    """
    # 1️⃣ Tenta pegar do cache local
    structures = get_db_structures(db, connection_id)
    if structures:
        # Retorna nomes das tabelas já registradas
        # print("retorn da base de dados a estruture ficheiro database_inspector")
        return [s.table_name for s in structures]

    # 2️⃣ Caso não exista no cache, conecta ao banco
    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)
    inspector = inspect(engine)

    # 3️⃣ Pega todos os schemas
    try:
        schemas = inspector.get_schema_names()
    except Exception as e:
        log_message(f"⚠️ Erro ao obter schemas: {e}", "warning")
        schemas = [inspector.default_schema_name]

    all_table_names = []

    # 4️⃣ Processa tabelas e atualiza estrutura
    for schema in schemas:
        try:
            tables = inspector.get_table_names(schema=schema)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter tabelas do schema '{schema}': {e}", "warning")
            tables = []

        for table in tables:
            verificar_ou_atualizar_estrutura(db, connection_id, table, schema)
        all_table_names.extend(tables)

    # 5️⃣ Processa views
    for schema in schemas:
        try:
            views = inspector.get_view_names(schema=schema)
        except Exception as e:
            log_message(f"⚠️ Erro ao obter views do schema '{schema}': {e}", "warning")
            views = []
        all_table_names.extend(views)

    return all_table_names



def get_table_count(connection_id: int, table_name: str, db: Session, id_user: int) -> int:
    """
    Retorna a contagem de registros de uma tabela de forma segura e eficiente.
    - Retorna 0 se for uma view.
    - Usa quote para evitar SQL injection.
    - Faz fallback para -1 em caso de erro.
    """
    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)

    try:
        inspector = inspect(engine)

        # 🚩 Verifica se o objeto é uma view
        if table_name in inspector.get_view_names(schema=inspector.default_schema_name):
            log_message(f"ℹ️ '{table_name}' é uma VIEW, retornando 0.", "info")
            return 0
        else:
            log_message(f"ℹ️ '{table_name}' é uma TABELA, procedendo com a contagem.", "info")

        with engine.connect() as conn:
            query = text(f'SELECT COUNT(*) FROM "{table_name}"')
            count = conn.execute(query).scalar()
            return count if count is not None else 0

    except SQLAlchemyError as e:
        error_type = _lidar_com_erro_sql(e)
        log_message(
            f"⚠️ Erro ao contar registros da tabela '{table_name}': {error_type} - {e}",
            "error"
        )
        return -1




from app.schemas.dbstatistics_schema import DBStatisticsDict

def collect_statistics(engine: Engine , db_type: str) -> DBStatisticsDict:
    """
    Coleta estatísticas gerais do banco de dados de forma compatível com múltiplos SGBDs.
    """
    inspector = inspect(engine)
    dialect = db_type.lower()
    tables_name = inspector.get_table_names()
    stats:DBStatisticsDict = {
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
        "db_connection_id": 0
    }

    version_query = {
        "postgresql": "SHOW server_version",
        "mysql": "SELECT VERSION()",
        "sqlite": "SELECT sqlite_version()",
        "mssql": "SELECT @@VERSION",
        "sql server": "SELECT @@VERSION",
        "sqlserver": "SELECT @@VERSION",
    }.get(dialect)

    with engine.connect() as conn:
        if version_query:
            stats["server_version"] = conn.execute(text(version_query)).scalar()

        if dialect == "postgresql":
            stats["procedure_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'p'")).scalar()
            stats["function_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_proc WHERE prokind = 'f'")).scalar()
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal")).scalar()
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM pg_indexes")).scalar()

        elif dialect == "mysql":
            stats["procedure_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")).scalar()
            stats["function_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='FUNCTION'")).scalar()
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.TRIGGERS")).scalar()
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM information_schema.STATISTICS")).scalar()

        elif dialect == "sqlite":
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")).scalar()
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")).scalar()

        elif dialect in ["mssql", "sql server", "sqlserver"]:
            stats["procedure_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.procedures")).scalar()
            stats["function_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN', 'TF', 'IF')")).scalar()
            stats["trigger_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.triggers")).scalar()
            stats["index_count"] = conn.execute(text("SELECT COUNT(*) FROM sys.indexes WHERE name IS NOT NULL")).scalar()

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
            }
        )
        log_message(f"🆕 Criando novas estatísticas para a conexão {data_create}.", "info")
        return create_statistics(db, data_create) or "created"

    # Verifica se há alguma diferença
    changed = any(getattr(existing, key, None) != stats.get(key) for key in [
        "table_count", "view_count", "procedure_count", "function_count",
        "trigger_count", "index_count", "tables_connected"
    ])

    if changed:
        data_update = DBStatisticsUpdate(
            **{k: stats[k] for k in DBStatisticsUpdate.__annotations__ if k in stats}
        )
        update_statistics(db, connection_id, data_update)
        return "updated"

    log_message(f"ℹ️ Nenhuma mudança nas estatísticas da conexão {connection_id}.", "info")
    return "unchanged"





def sync_connection_statistics( id_user: int,   db: Session) -> dict | None:
    """
    Sincroniza estatísticas da conexão: coleta e armazena se necessário.
    """
    try:

        engine,connection = ConnectionManager.ensure_connection(db, id_user)
        existing = get_statistics_by_connection(db, connection.id)
        if existing:
            return existing

        stats = collect_statistics(engine,connection.type)
        if not stats:
            log_message(f"⚠️ Nenhuma estatística coletada para a conexão {connection.id}.", "warning")
            return None

        log_message(f"📊 Estatísticas coletadas: {stats}", "info")

        action = save_or_update_statistics(connection.id, stats, db)
        

        log_message(f"✅ Estatísticas '{action}' registradas para conexão ID={connection.id}.", "success")
        stats["connection_name"] = connection.name
        return stats

    except Exception as e:
        # Captura detalhes avançados do erro
        error_type = type(e).__name__
        error_message = str(e)
        stack_trace = traceback.format_exc()

        log_message(
            f"❌ Erro ao sincronizar estatísticas da conexão ID={connection.id}, usuário ID={id_user}:\n"
            f"Tipo: {error_type}\n"
            f"Mensagem: {error_message}\n"
            f"StackTrace:\n{stack_trace}",
            level="error"
        )
        return None