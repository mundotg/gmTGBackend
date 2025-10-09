from datetime import datetime
import json
import uuid
from app.ultils.logger import log_message

def _map_column_type(col_type: str):
    """Mapeia o tipo da coluna para a função de conversão correspondente."""
    col_type = col_type.lower()

    if any(t in col_type for t in [
        "int", "integer", "smallint", "bigint", "tinyint", "serial", "bigserial", "number"
    ]):
        return int

    if any(t in col_type for t in [
        "float", "real", "double", "double precision", "decimal", "numeric"
    ]):
        return float
    elif col_type == "bit":
        return lambda x: None if x is None else (1 if str(x).lower() in ("1", "true", "t", "yes", "on") else 0)

    elif any(t in col_type for t in ["bool", "boolean"]):
        return lambda x: str(x).lower() in ("true", "yes", "t", "on") if x is not None else None

    elif "timestamp" in col_type:
        return lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if x else None

    elif "uuid" in col_type:
        return lambda x: uuid.UUID(x) if x else None

    elif any(t in col_type for t in ["json", "jsonb"]):
        return lambda x: json.loads(x) if isinstance(x, str) and x.strip() else None

    elif any(t in col_type for t in ["blob", "binary"]):
        return lambda x: bytes(x, "utf-8") if x is not None else None

    else:
        return str


def _convert_column_type_for_string_one(value, col_type):
    """Converte o valor baseado no tipo da coluna e retorna string SQL."""
    if value is None or value == "":
        return "NULL"

    converter = _map_column_type(col_type)
    try:
        if converter in [int, float]:
            return str(converter(value))
        elif converter.__name__ == "<lambda>":  # bool, datetime, uuid, json, bytes
            converted = converter(value)
            if isinstance(converted, bool):
                return str(converted).upper()  # PostgreSQL espera TRUE/FALSE
            elif isinstance(converted, (datetime, uuid.UUID)):
                return f"'{converted}'"
            elif isinstance(converted, (dict, list)):
                return f"'{json.dumps(converted)}'"
            elif isinstance(converted, bytes):
                return f"'{converted.decode('utf-8')}'"
            else:
                return f"'{converted}'"
        else:
            return f"'{converter(value)}'"
    except Exception as e:
        log_message(f"Erro ao converter valor '{value}' para {col_type}: {e}", "error")
        return "NULL"

def quote_identifier(db_type: str, identifier: str) -> str:
    """
    Escapa um identificador SQL conforme o tipo de banco.
    Suporta identificadores compostos como 'tabela.coluna'.

    Ex:
    - PostgreSQL: "tabela"."coluna"
    - MySQL: `tabela`.`coluna`
    - MSSQL: [tabela].[coluna]
    """
    db_type = db_type.lower()
    parts = identifier.split(".")
    
    if db_type in ['postgresql', 'postgres', 'oracle']:
        return ".".join(f'"{part}"' for part in parts)
    elif db_type in ['mssql', 'sql server', 'sqlserver']:
        return ".".join(f'[{part}]' for part in parts)
    elif db_type in ['mysql']:
        return ".".join(f'`{part}`' for part in parts)
    else:
        return identifier