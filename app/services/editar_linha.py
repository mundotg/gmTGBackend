from datetime import datetime
import json
import uuid
from app.ultils.logger import log_message

from datetime import datetime
import uuid
import json

def _map_column_type(col_type: str):
    """Mapeia o tipo da coluna para a função de conversão correspondente."""
    col_type = col_type.lower()

    # 🔢 Inteiros
    if any(t in col_type for t in [
        "int", "integer", "smallint", "bigint", "tinyint", "serial", "bigserial", "number"
    ]):
        return int

    # 🔣 Números decimais
    if any(t in col_type for t in [
        "float", "real", "double", "double precision", "decimal", "numeric"
    ]):
        return float

    # ⚙️ Bits (0/1)
    elif col_type == "bit":
        return lambda x: None if x is None else (1 if str(x).lower() in ("1", "true", "t", "yes", "on") else 0)

    # 🟢 Booleanos
    elif any(t in col_type for t in ["bool", "boolean"]):
        return lambda x: str(x).lower() in ("true", "yes", "t", "on", "1") if x is not None else None

    # 🕒 Timestamps e datas (mais tolerante)
    elif "timestamp" in col_type or "date" in col_type:
        def parse_datetime(x):
            if not x:
                return None
            try:
                # Caso venha direto como datetime
                if isinstance(x, datetime):
                    return x
                # Normaliza formatos ISO
                val = str(x).replace("T", " ").replace("Z", "").split(".")[0]
                # Adiciona segundos se faltarem
                if len(val) == 16:  # yyyy-MM-dd HH:mm
                    val += ":00"
                return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Fallback para formato apenas de data
                try:
                    return datetime.strptime(str(x), "%Y-%m-%d")
                except Exception:
                    return None
        return parse_datetime
        # ⏱️ Tempo puro (sem data)
    elif "time" in col_type :
        return lambda x: datetime.strptime(x, "%H:%M:%S").time() if x else None

    # ⏳ Intervalos
    elif "interval" in col_type:
        from datetime import timedelta
        def parse_interval(x):
            if not x:
                return None
            # Exemplo simples: '2 days 03:00:00'
            try:
                parts = str(x).split()
                days = int(parts[0]) if "day" in parts else 0
                time_part = parts[-1]
                h, m, s = map(int, time_part.split(":"))
                return timedelta(days=days, hours=h, minutes=m, seconds=s)
            except Exception:
                return None
        return parse_interval

    # 🧮 Arrays SQL
    elif "[]" in col_type:
        return lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else None

    # 💰 Money
    elif "money" in col_type:
        return lambda x: float(str(x).replace("$", "").replace(",", "").strip()) if x else None


    # 🧩 UUIDs
    elif "uuid" in col_type:
        return lambda x: uuid.UUID(str(x)) if x else None

    # 🧠 JSON e JSONB
    elif any(t in col_type for t in ["json", "jsonb"]):
        return lambda x: json.loads(x) if isinstance(x, str) and x.strip() else None

    # 📦 Blobs e binários
    elif any(t in col_type for t in ["blob", "binary", "bytea"]):
        return lambda x: bytes(x, "utf-8") if x is not None else None

    # 🔤 Fallback: string
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
    
def _convert_column_type_for_string_one_V1(value, col_type):
    """Converte o valor baseado no tipo da coluna e retorna valor Python (não string SQL)."""
    if value is None or value == "" or value == "NULL":
        return None

    converter = _map_column_type(col_type)
    try:
        converted = converter(value)

        # ✅ Deixar tipos especiais como objetos Python
        from datetime import datetime, date
        import uuid, json

        if isinstance(converted, (bool, datetime, date, uuid.UUID, dict, list, bytes)):
            return converted
        
        # ✅ Retorno como string normal sem aspas extras — asyncpg coloca aspas sozinho
        return str(converted)

    except Exception as e:
        log_message(f"Erro ao converter valor '{value}' para {col_type}: {e}", "error")
        return None
    
RESERVED_KEYWORDS = {
    'default', 'select', 'insert', 'update', 'delete', 'from', 'where',
    'order', 'group', 'by', 'having', 'join', 'inner', 'left', 'right',
    'outer', 'on', 'as', 'and', 'or', 'not', 'null', 'is', 'true', 'false',
    'primary', 'key', 'foreign', 'references', 'table', 'column', 'index',
    'create', 'alter', 'drop', 'truncate', 'grant', 'revoke', 'commit',
    'rollback', 'savepoint', 'begin', 'transaction', 'lock', 'unlock',
    'user', 'role', 'database', 'schema', 'view', 'function', 'procedure',
    'trigger', 'event', 'type', 'domain', 'constraint', 'check', 'unique',
    'current', 'time', 'date', 'timestamp', 'interval', 'year',
    'month', 'day', 'hour', 'minute', 'second', 'zone', 'value', 'values'
}


def needs_quoting(identifier_part: str, db_type: str) -> bool:
    if not identifier_part:
        return False

    db_type = db_type.lower()

    # 🔥 POSTGRESQL / SQLITE → case-sensitive se usar maiúsculas
    if db_type in ["postgresql", "postgres", "sqlite"]:
        if identifier_part != identifier_part.lower():
            return True

    # 🔥 MYSQL → normalmente não precisa por causa do case-insensitive
    # (mas ainda precisa para keywords e caracteres inválidos)

    # palavra reservada
    if identifier_part.lower() in RESERVED_KEYWORDS:
        return True

    # caracteres inválidos
    if not identifier_part.replace('_', '').isalnum():
        return True

    # começa com número
    if identifier_part[0].isdigit():
        return True

    return False


def quote_char(db_type: str) -> str:
    db_type = db_type.lower()

    if db_type in ["postgresql", "postgres", "sqlite"]:
        return '"'
    elif db_type in ["mysql", "mariadb"]:
        return '`'
    else:
        return '"'  # fallback seguro


def quote_identifier(db_type: str, identifier: str) -> str:
    """
    Escapa identificadores SQL corretamente por banco.
    Suporta: schema.tabela.coluna
    """
    db_type = db_type.lower()
    parts = identifier.split(".")
    qchar = quote_char(db_type)

    quoted_parts = []

    for part in parts:
        if needs_quoting(part, db_type):
            quoted_parts.append(f"{qchar}{part}{qchar}")
        else:
            quoted_parts.append(part)

    return ".".join(quoted_parts)