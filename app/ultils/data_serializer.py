# app/ultils/data_serializer.py
from __future__ import annotations

import datetime
from decimal import Decimal
import json
import math
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from bson import ObjectId
    from bson.decimal128 import Decimal128
except ImportError:
    ObjectId = None
    Decimal128 = None


def is_valid_object_id(val: Any) -> bool:
    """Verifica se o valor é uma representação válida de ObjectId."""
    if ObjectId is None:
        return False
    if isinstance(val, ObjectId):
        return True
    if isinstance(val, str) and len(val.strip()) == 24:
        return ObjectId.is_valid(val.strip())
    return False


def serialize_for_mongo(
    val: Any,
    dst_type: Optional[str] = None,
    col_name: Optional[str] = None,
) -> Any:
    """
    Serializa e normaliza um valor individual para garantir que seja 100%
    compatível com os tipos nativos do BSON/MongoDB.
    Evita erros de serialização como 'cannot encode object of type ...'.
    """
    if val is None:
        return None

    # Normalização de tipos NumPy ou outros tipos escalares com método .item()
    if hasattr(val, "item") and callable(getattr(val, "item")):
        try:
            val = val.item()
        except Exception:
            pass

    t = (dst_type or "").lower().strip()
    col = (col_name or "").lower().strip()

    # 1. Trata ObjectId e chave primária _id
    if t in ("objectid", "object_id") or col == "_id":
        if ObjectId is not None:
            if isinstance(val, ObjectId):
                return val
            s = str(val).strip()
            if ObjectId.is_valid(s):
                try:
                    return ObjectId(s)
                except Exception:
                    return s
        return str(val) if val is not None else None

    # Se for string vazia e o tipo esperado for numérico, booleano ou data, retorna None
    if isinstance(val, str) and val.strip() == "":
        if any(k in t for k in ("int", "float", "number", "double", "decimal", "date", "time", "bool")):
            return None
        if t in ("string", "str", "text", "varchar") or not t:
            return ""

    # 2. Inteiros
    if any(k in t for k in ("int", "long", "serial", "smallint", "bigint")):
        try:
            return int(float(str(val).strip()))
        except (ValueError, TypeError):
            return val

    # 3. Decimais / Floats
    if any(k in t for k in ("float", "double", "decimal", "numeric", "number", "real")):
        try:
            f = float(str(val).strip())
            return f
        except (ValueError, TypeError):
            return val

    # 4. Booleanos
    if any(k in t for k in ("bool", "boolean", "bit")):
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ("true", "1", "yes", "sim", "t", "s", "on")

    # 5. Datas e Horas (Crucial: BSON só aceita datetime.datetime, NÃO datetime.date!)
    if any(k in t for k in ("date", "datetime", "timestamp", "time")):
        if isinstance(val, datetime.datetime):
            return val
        if isinstance(val, datetime.date):
            return datetime.datetime(val.year, val.month, val.day)
        if isinstance(val, str):
            s = val.strip()
            try:
                # Tenta ISO format padrão (com ou sem Z / timezone)
                return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                try:
                    return datetime.datetime.strptime(s.split(".")[0], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    try:
                        return datetime.datetime.strptime(s, "%Y-%m-%d")
                    except Exception:
                        return s
        return val

    # 6. UUID
    if isinstance(val, uuid.UUID):
        return str(val)

    # 7. Decimal do Python (BSON gera erro se inserir Decimal puro)
    if isinstance(val, Decimal):
        if Decimal128 is not None:
            try:
                return Decimal128(val)
            except Exception:
                return float(val)
        return float(val)

    # 8. Data pura sem tipo explícito (converte para datetime porque BSON não aceita date)
    if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
        return datetime.datetime(val.year, val.month, val.day)

    # 9. Dicionários / Documentos aninhados
    if isinstance(val, dict):
        return serialize_mongo_document(val)

    # 10. Listas / Conjuntos / Tuplas
    if isinstance(val, (list, tuple, set)):
        return [serialize_for_mongo(item) for item in val]

    # 11. Se string json para tipo objeto/array
    if isinstance(val, str) and (val.startswith("{") or val.startswith("[")):
        if any(k in t for k in ("json", "object", "dict", "array", "list", "[]")):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    return serialize_mongo_document(parsed)
                elif isinstance(parsed, list):
                    return [serialize_for_mongo(item) for item in parsed]
            except Exception:
                pass

    # 12. Strings literais
    if any(k in t for k in ("string", "str", "varchar", "char", "text")):
        return str(val)

    return val


def serialize_mongo_document(
    doc: Dict[str, Any],
    column_types: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Serializa um documento completo do MongoDB antes da inserção.
    - Sanitiza chaves (remove '.' e '$' proibidos no BSON).
    - Converte todos os valores de forma recursiva.
    """
    if not isinstance(doc, dict):
        return doc

    sanitized: Dict[str, Any] = {}
    col_types = column_types or {}

    for k, v in doc.items():
        # Sanitização de chave para MongoDB
        key_str = str(k).strip()
        if "." in key_str:
            key_str = key_str.replace(".", "_")
        if key_str.startswith("$"):
            key_str = "_" + key_str[1:]

        target_type = col_types.get(k) or col_types.get(key_str)
        sanitized[key_str] = serialize_for_mongo(v, dst_type=target_type, col_name=key_str)

    return sanitized


def serialize_for_sql(val: Any, dst_type: Optional[str] = None) -> Any:
    """
    Serializa um valor para inserção em bancos SQL (PostgreSQL, MySQL, SQLite, etc.).
    - Converte ObjectId para string.
    - Converte Decimal128 para Decimal/float.
    - Converte dicionários/listas para JSON string quando a coluna não for JSON nativo.
    - Converte strings vazias para None quando o tipo for numérico/data/booleano/uuid (evita SQL syntax errors).
    """
    if val is None:
        return None

    # Normalização de tipos NumPy
    if hasattr(val, "item") and callable(getattr(val, "item")):
        try:
            val = val.item()
        except Exception:
            pass

    t = (dst_type or "").lower().strip()

    # ObjectId para SQL sempre vira string
    if ObjectId is not None and isinstance(val, ObjectId):
        return str(val)

    # Decimal128 para SQL vira Decimal ou float
    if Decimal128 is not None and isinstance(val, Decimal128):
        try:
            return val.to_decimal()
        except Exception:
            return float(str(val))

    # Strings vazias para tipos numéricos/datas/uuid viram None (NULL no SQL)
    if isinstance(val, str) and val.strip() == "":
        if any(k in t for k in ("int", "float", "double", "decimal", "numeric", "number", "date", "time", "bool", "uuid")):
            return None
        if val == "NULL":
            return None

    # Dicionários e listas para SQL:
    # Se a coluna de destino for array nativo do Postgres (ex: text[], int[]), mantém list.
    # Para JSON, JSONB, TEXT ou qualquer outro tipo SQL, serializa para JSON string (necessário para drivers como asyncpg).
    if isinstance(val, (dict, list)):
        if isinstance(val, list) and (any(k in t for k in ("array", "[]")) or (t.startswith("_") and len(t) > 1)):
            return val
        try:
            return json.dumps(val, default=str, ensure_ascii=False)
        except Exception:
            return str(val)

    # UUID
    if isinstance(val, uuid.UUID):
        return str(val)

    # Tuplas e Sets
    if isinstance(val, (tuple, set)):
        if any(k in t for k in ("array", "[]")) or (t.startswith("_") and len(t) > 1):
            return list(val)
        return json.dumps(list(val), default=str, ensure_ascii=False)

    return val


def serialize_sql_params(
    params: Dict[str, Any],
    column_types: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Serializa todos os parâmetros de uma query SQL parametrizada."""
    if not isinstance(params, dict):
        return params

    col_types = column_types or {}
    sanitized: Dict[str, Any] = {}

    for k, v in params.items():
        dst_type = col_types.get(k)
        sanitized[k] = serialize_for_sql(v, dst_type)

    return sanitized
