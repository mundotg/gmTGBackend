
import traceback
from sqlalchemy import Engine, text

from app.ultils.logger import log_message


def _is_system_field(col_name: str, col_type: str, db_type: str, table_name: str, engine) -> bool:
    """Determina se um campo é do sistema e deve ser ignorado."""
    
   
    
    try:
        # Palavras-chave para identificar colunas auto-incrementáveis
        auto_increment_keywords = {
            "serial", "bigserial", "identity", "autoincrement", "uuid",
            "integer primary key", "auto_increment", "integer primary key autoincrement", 
            "sequence", "smallserial",
        }

        # Campos do sistema que sempre devem ser ignorados
        system_field_names = {"id", "created_at", "updated_at", "created_by", "updated_by"}

        # Normaliza o nome da coluna e o tipo
        col_name_lower = col_name.lower().strip()
        col_type_lower = col_type.lower().strip() if col_type else ""

        # Verifica se é um campo de sistema e auto-incrementável
        if col_name_lower in system_field_names and any(keyword in col_type_lower for keyword in auto_increment_keywords):
            return True

        # Verificações por tipo de banco de dados
        if db_type == "sql server" or db_type == "sqlserver" and col_type_lower in {"smallint", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT COLUMNPROPERTY(OBJECT_ID(:table_name), :col_name, 'IsIdentity') AS IsIdentity
                """)
                try:
                    result = conn.execute(query, {"table_name": table_name, "col_name": col_name}).fetchone()
                    return bool(result and result[0] == 1)
                except Exception as e:
                    log_message(f"Erro ao consultar SQL Server: {e}")
                    return False

        elif db_type == "postgresql" and col_type_lower in {"smallint", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = :table_name AND is_identity = 'YES'
                """)
                try:
                    result = conn.execute(query, {"table_name": table_name}).fetchall()
                    return bool(result)
                except Exception as e:
                    log_message(f"Erro ao consultar PostgreSQL: {e}")
                    return False

        elif db_type == "oracle" and col_type_lower in {"number", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT column_name 
                    FROM all_tab_columns 
                    WHERE table_name = :table_name AND identity_column = 'YES'
                """)
                try:
                    result = conn.execute(query, {"table_name": table_name.upper()}).fetchall()
                    return bool(result)
                except Exception as e:
                    log_message(f"Erro ao consultar Oracle: {e}")
                    return False

        elif db_type == "mysql" and col_type_lower in {"tinyint", "smallint", "mediumint", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name AND EXTRA LIKE '%auto_increment%'
                """)
                try:
                    result = conn.execute(query, {"table_name": table_name}).fetchall()
                    return bool(result)
                except Exception as e:
                    log_message(f"Erro ao consultar MySQL: {e}")
                    return False
        return False

    except Exception as e:
        log_message(f"Erro geral na função _is_system_field: {e} {traceback.format_exc()}")
        return False
    

def obter_schema_do_engine(engine: Engine, db_type : str) -> str:
 
    if db_type == "postgresql":
        # PostgreSQL usa 'current_schema()'
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_schema();"))
            return result.scalar()

    elif db_type in ("mysql", "mariadb"):
        # MySQL/MariaDB: schema é o banco atual
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE();"))
            return result.scalar()

    elif db_type in ("mssql", "sql server", "sqlserver"):
        with engine.connect() as conn:
            result = conn.execute(text("SELECT SCHEMA_NAME();"))
            return result.scalar()

    elif db_type == "sqlite":
        # SQLite não tem schemas
        return "main"

    else:
        return "public"