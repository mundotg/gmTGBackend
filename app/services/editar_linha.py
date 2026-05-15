from datetime import datetime
import json
import uuid
from app.ultils.logger import log_message

from datetime import datetime
import uuid
import json
import re
from functools import lru_cache


def _map_column_type(col_type: str):
    """Mapeia o tipo da coluna para a função de conversão correspondente."""
    col_type = col_type.lower()

    # 🔢 Inteiros
    if any(
        t in col_type
        for t in [
            "int",
            "integer",
            "smallint",
            "bigint",
            "tinyint",
            "serial",
            "bigserial",
            "number",
        ]
    ):
        return int

    # 🔣 Números decimais
    if any(
        t in col_type
        for t in ["float", "real", "double", "double precision", "decimal", "numeric"]
    ):
        return float

    # ⚙️ Bits (0/1)
    elif col_type == "bit":
        return lambda x: (
            None
            if x is None
            else (1 if str(x).lower() in ("1", "true", "t", "yes", "on") else 0)
        )

    # 🟢 Booleanos
    elif any(t in col_type for t in ["bool", "boolean"]):
        return lambda x: (
            str(x).lower() in ("true", "yes", "t", "on", "1") if x is not None else None
        )

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
    elif "time" in col_type:
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
        return lambda x: (
            json.loads(x) if isinstance(x, str) and x.startswith("[") else None
        )

    # 💰 Money
    elif "money" in col_type:
        return lambda x: (
            float(str(x).replace("$", "").replace(",", "").strip()) if x else None
        )

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


# =========================================================
# RESERVED KEYWORDS
# =========================================================

RESERVED_KEYWORDS = {
    # SQL
    "select",
    "insert",
    "update",
    "delete",
    "from",
    "where",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "outer",
    "cross",
    "on",
    "group",
    "order",
    "by",
    "having",
    "limit",
    "offset",
    "union",
    "all",
    "distinct",
    "exists",
    "between",
    "like",
    "in",
    "as",
    "and",
    "or",
    "not",
    "null",
    "is",
    # DDL
    "table",
    "column",
    "index",
    "view",
    "sequence",
    "trigger",
    "procedure",
    "function",
    "package",
    "database",
    "schema",
    "tablespace",
    # Constraints
    "primary",
    "foreign",
    "references",
    "constraint",
    "unique",
    "check",
    "default",
    # Transactions
    "commit",
    "rollback",
    "savepoint",
    "transaction",
    "lock",
    # Users
    "user",
    "role",
    "grant",
    "revoke",
    # Date
    "current",
    "date",
    "time",
    "timestamp",
    "interval",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    # Boolean
    "true",
    "false",
    # Misc
    "value",
    "values",
    "type",
    "event",
}

# =========================================================
# DATABASE CONFIG
# =========================================================

DB_QUOTES = {
    "postgresql": '"',
    "postgres": '"',
    "sqlite": '"',
    "oracle": '"',
    "mysql": "`",
    "mariadb": "`",
    "sqlserver": "[",
    "mssql": "[",
}

# =========================================================
# REGEX
# =========================================================

VALID_IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")

# =========================================================
# HELPERS
# =========================================================


@lru_cache(maxsize=2048)
def quote_char(db_type: str) -> str:
    """
    Retorna caractere de escape do banco.
    """
    return DB_QUOTES.get(db_type.lower(), '"')


def normalize_identifier(identifier: str) -> str:
    """
    Remove espaços extras.
    """
    return identifier.strip()


def is_reserved_keyword(identifier: str) -> bool:
    return identifier.lower() in RESERVED_KEYWORDS


def has_invalid_chars(identifier: str) -> bool:
    """
    Oracle aceita:
    A-Z 0-9 _ $ #

    PostgreSQL/MySQL aceitam parecido.
    """
    return not bool(VALID_IDENTIFIER_REGEX.match(identifier))


def starts_with_number(identifier: str) -> bool:
    return identifier[0].isdigit()


def is_case_sensitive_required(
    identifier: str,
    db_type: str,
) -> bool:
    """
    PostgreSQL:
    Tudo vira lowercase se não usar aspas.

    Oracle:
    Tudo vira UPPERCASE.

    Então:
    CamelCase precisa quote.
    """

    db_type = db_type.lower()

    if db_type in ["postgresql", "postgres"]:
        return identifier != identifier.lower()

    if db_type == "oracle":
        return identifier != identifier.upper()

    return False


def needs_quoting(
    identifier: str,
    db_type: str,
) -> bool:
    """
    Decide se precisa escapar.
    """

    if not identifier:
        return False

    identifier = normalize_identifier(identifier)

    if is_reserved_keyword(identifier):
        return True

    if starts_with_number(identifier):
        return True

    if has_invalid_chars(identifier):
        return True

    if is_case_sensitive_required(identifier, db_type):
        return True

    return False


def quote_part(
    db_type: str,
    identifier_part: str,
) -> str:
    """
    Escapa uma única parte:
    schema
    table
    column
    """

    identifier_part = normalize_identifier(identifier_part)

    if not identifier_part:
        return ""

    if not needs_quoting(identifier_part, db_type):
        return identifier_part

    qchar = quote_char(db_type)

    # SQL Server usa []
    if qchar == "[":
        escaped = identifier_part.replace("]", "]]")
        return f"[{escaped}]"

    # PostgreSQL / Oracle / SQLite / MySQL
    escaped = identifier_part.replace(qchar, qchar * 2)

    return f"{qchar}{escaped}{qchar}"


def quote_identifier(
    db_type: str,
    identifier: str,
) -> str:
    """
    Escapa:
    schema.table.column

    Exemplo:

    postgres:
        public.users.id
        -> public.users.id

    postgres camelCase:
        public.UserTable.id
        -> public."UserTable".id

    oracle:
        SYSTEM.TABLE_TEST.ID

    mysql:
        `user`.`order`
    """

    if not identifier:
        return identifier

    db_type = db_type.lower()

    # remove:
    # .table
    # table.
    # schema..table
    raw_parts = identifier.split(".")

    parts = []

    for part in raw_parts:
        part = normalize_identifier(part)

        if not part:
            continue

        parts.append(part)

    if not parts:
        return ""

    quoted_parts = [quote_part(db_type, part) for part in parts]

    return ".".join(quoted_parts)


# =========================================================
# FULL TABLE NAME
# =========================================================


def build_table_name(
    db_type: str,
    table: str,
    schema: str | None = None,
) -> str:
    """
    Monta:
    schema.table
    ou:
    table
    """

    table = quote_identifier(db_type, table)

    if schema and schema.strip():
        schema = quote_identifier(db_type, schema)
        return f"{schema}.{table}"

    return table
