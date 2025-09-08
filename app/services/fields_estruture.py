
import traceback
from sqlalchemy import Engine, text

from app.ultils.logger import log_message

def _is_system_field(col_name: str, col_type: str, db_type: str, table_name: str, engine) -> bool:
    """Determina se um campo é do sistema (id, created_at, etc.) ou auto-incrementável."""

    try:
        
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
            # print(f" é  col_type_lower  system_field_names")
            return True

        # Campos do sistema que sempre devem ser ignorados
        system_field_names = {"id", "created_at", "updated_at", "created_by", "updated_by"}
        if col_name_lower in system_field_names:
            return True

        # Verificações específicas por banco
        if db_type in {"sql server", "sqlserver"} and col_type_lower in {"smallint", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT COLUMNPROPERTY(OBJECT_ID(:table_name), :col_name, 'IsIdentity') AS IsIdentity
                """)
                result = conn.execute(query, {"table_name": table_name, "col_name": col_name}).fetchone()
                return bool(result and result[0] == 1)

        elif db_type == "postgresql" and col_type_lower in {"smallint", "integer", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT column_name, column_default, is_identity
                    FROM information_schema.columns
                    WHERE table_name = :table_name AND column_name = :col_name
                """)
                result = conn.execute(query, {"table_name": table_name, "col_name": col_name}).fetchone()
                return bool(
                    result and (
                        result["is_identity"] == "YES"
                        or (result["column_default"] and "nextval" in result["column_default"])
                    )
                )

        elif db_type == "oracle" and col_type_lower in {"number", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT column_name 
                    FROM all_tab_columns 
                    WHERE table_name = :table_name AND column_name = :col_name AND identity_column = 'YES'
                """)
                result = conn.execute(query, {"table_name": table_name.upper(), "col_name": col_name.upper()}).fetchone()
                return bool(result)

        elif db_type == "mysql" and col_type_lower in {"tinyint", "smallint", "mediumint", "int", "bigint"}:
            with engine.connect() as conn:
                query = text("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name AND COLUMN_NAME = :col_name
                      AND EXTRA LIKE '%auto_increment%'
                """)
                result = conn.execute(query, {"table_name": table_name, "col_name": col_name}).fetchone()
                return bool(result)

        return False

    except Exception as e:
        log_message(f"Erro geral na função _is_system_field: {e} {traceback.format_exc()}")
        return False
    
def _is_system_field_from_column(
    col_type,
    column_info=None,
    is_foreign_key=False
) -> bool:
    """
    Determina se um campo é de sistema (ex: id, created_at) ou auto-incrementável,
    usando apenas os metadados já fornecidos (sem consultar o banco).

    :param col_type: Tipo da coluna (ex: VARCHAR, SERIAL, BIGINT, etc.)
    :param column_info: Metadados da coluna (ex: {"is_identity": True, "default": "...", "extra": "auto_increment"}).
                        Pode ser None ou um dict vazio.
    """

    try:
        # Garante que column_info é um dict
        column_info = column_info or {}

        # Normalização do tipo e valores auxiliares
        col_type_lower = str(col_type or "").lower().strip()
        default_value  = str(column_info.get("default") or "").lower()
        extra_value    = str(column_info.get("extra") or "").lower()
        # print(f" {column_info} {col_type_lower} {default_value} {extra_value}")
        
        # Palavras-chave universais que indicam auto incremento
        auto_increment_keywords = {
            "serial", "bigserial", "smallserial",
            "identity", "autoincrement", "auto_increment",
            "uuid", "sequence",
            "integer primary key", "integer primary key autoincrement",
        }
        if any(keyword in col_type_lower for keyword in auto_increment_keywords):
            return True and not is_foreign_key

        # Regras específicas por banco / ORM
        is_auto = (
            column_info.get("autoincrement", False)                  # ORM flag
            or "nextval" in default_value                             # PostgreSQL
            or "auto_increment" in extra_value                        # MySQL
            or str(column_info.get("is_identity", False)).lower() in ("1", "true")  # SQL Server
            or str(column_info.get("identity_column", "")).lower() == "yes"         # Oracle
        )

        return is_auto

    except Exception as e:
        log_message(f"[WARN] Erro em _is_system_field_from_column: {e} {traceback.format_exc()}","error")
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