import re
import traceback
from dataclasses import dataclass
from typing import Optional, Any, Dict, Tuple, Union

from sqlalchemy.exc import (
    SQLAlchemyError,
    IntegrityError,
    ProgrammingError,
    OperationalError,
    DataError,
)

from app.ultils.logger import log_message


# ------------------------------------------------------------
# Regras (primeiro match vence)
# ------------------------------------------------------------

@dataclass(frozen=True)
class _SqlErrRule:
    code: str
    message: str
    patterns: Tuple[Union[str, re.Pattern], ...]
    # por padrão, "contains" em lower; regex ignora esse flag
    lower_contains: bool = True


def _match_rule(text: str, low: str, rule: _SqlErrRule) -> bool:
    for p in rule.patterns:
        if isinstance(p, re.Pattern):
            if p.search(text):
                return True
        else:
            needle = str(p)
            if rule.lower_contains:
                if needle.lower() in low:
                    return True
            else:
                if needle in text:
                    return True
    return False


_SQL_ERR_RULES: Tuple[_SqlErrRule, ...] = (
    _SqlErrRule(
        code="FK_VIOLATION",
        message=(
            "❌ Falha: integridade referencial violada (FK). "
            "O registro está a ser usado noutra tabela. "
            "Verifique dependências e chaves estrangeiras."
        ),
        patterns=(
            "foreign key",
            "violates foreign key",
            "foreignkeyviolation",
            "integrity constraint violation",
            "restrição reference",
            re.compile(r"\bconflict\b.*\breference\b", re.I),
        ),
    ),
    _SqlErrRule(
        code="UNIQUE_VIOLATION",
        message="❌ Falha: violação de unicidade. Já existe um registro com o mesmo valor.",
        patterns=("uniqueviolation", "duplicate key value", "unique constraint"),
    ),
    _SqlErrRule(
        code="NOT_NULL_VIOLATION",
        message="❌ Falha: campo obrigatório vazio (NOT NULL). Preencha os campos obrigatórios.",
        patterns=("notnullviolation", "null value in column", "not-null constraint"),
    ),
    _SqlErrRule(
        code="CHECK_VIOLATION",
        message="❌ Falha: restrição CHECK violada. Os valores não atendem às regras do banco.",
        patterns=("checkviolation", "violates check constraint"),
    ),
    _SqlErrRule(
        code="VALUE_TOO_LONG",
        message="❌ Falha: valor muito longo para o campo. Reduza o tamanho do texto.",
        patterns=("value too long", "data too long", "right truncation", "string data, right truncation"),
    ),
    _SqlErrRule(
        code="TYPE_MISMATCH",
        message="❌ Falha: tipo/formato de dado inválido. Verifique os valores enviados.",
        patterns=(
            "datatypemismatch",
            "invalidtextrepresentation",
            "invalid input syntax",
            "cannot cast",
            "type mismatch",
            "operator does not exist",
        ),
    ),
    _SqlErrRule(
        code="UNDEFINED_TABLE",
        message="❌ Falha: a tabela informada não existe no banco de dados.",
        patterns=(
            "undefinedtable",
            re.compile(r"\brelation\b.*\bdoes not exist\b", re.I),
            re.compile(r"\btable\b.*\bdoes not exist\b", re.I),
        ),
        lower_contains=False,
    ),
    _SqlErrRule(
        code="UNDEFINED_COLUMN",
        message="❌ Falha: uma coluna informada não existe na tabela.",
        patterns=(
            "undefinedcolumn",
            re.compile(r"\bcolumn\b.*\bdoes not exist\b", re.I),
        ),
        lower_contains=False,
    ),
    _SqlErrRule(
        code="SYNTAX_ERROR",
        message="❌ Erro de sintaxe SQL. Verifique a estrutura do comando.",
        patterns=("syntaxerror", "syntax error at", "syntax error"),
    ),
    _SqlErrRule(
        code="OPERATIONAL_ERROR",
        message="❌ Erro operacional. Verifique conexão, locks, permissões ou timeout.",
        patterns=(
            "operationalerror",
            "timeout",
            "deadlock",
            "could not connect",
            "connection refused",
            "permission denied",
            "lock timeout",
            "too many connections",
        ),
    ),
    _SqlErrRule(
        code="DATA_ERROR",
        message="❌ Erro de dados (tipo/tamanho/cast/overflow). Verifique o valor e o formato.",
        patterns=("dataerror", "numeric value out of range", "out of range", "overflow"),
    ),
)


def _lidar_com_erro_sql(
    exc: Exception,
    *,
    operation: Optional[str] = None,
    dialect: Optional[str] = None,
    table: Optional[str] = None,
    column: Optional[str] = None,
    sql: Optional[str] = None,
    return_details: bool = False,
) -> Union[str, Tuple[str, Dict[str, Any]]]:
    """
    Trata erros SQL e retorna mensagem padronizada (user-friendly).

    - Usa regras por texto (prioridade) e fallback por tipo SQLAlchemy.
    - Faz log com contexto + traceback.
    - Opcional: return_details=True devolve (msg, details).
    """
    err = str(exc) or repr(exc)
    low = err.lower()

    # contexto estruturado (pra log e auditoria)
    details: Dict[str, Any] = {
        "error_type": type(exc).__name__,
        "operation": operation,
        "dialect": dialect,
        "table": table,
        "column": column,
        "sql": sql ,
    }

    # 1) classificar por regras (mais preciso pro usuário)
    code = None
    msg = None
    for rule in _SQL_ERR_RULES:
        if _match_rule(err, low, rule):
            code = rule.code
            msg = rule.message
            break

    # 2) fallback por classe SQLAlchemy (quando texto não bate)
    if msg is None:
        if isinstance(exc, IntegrityError):
            code = "INTEGRITY_ERROR"
            msg = "❌ Falha: violação de integridade. Verifique constraints (PK/FK/unique/check)."
        elif isinstance(exc, ProgrammingError):
            code = "PROGRAMMING_ERROR"
            msg = "❌ Falha: erro de SQL (sintaxe/objeto inexistente)."
        elif isinstance(exc, OperationalError):
            code = "OPERATIONAL_ERROR"
            msg = "❌ Falha: erro operacional (conexão/lock/permissão/timeout)."
        elif isinstance(exc, DataError):
            code = "DATA_ERROR"
            msg = "❌ Falha: erro de dados (tipo/tamanho/cast/overflow)."
        elif isinstance(exc, SQLAlchemyError):
            code = "SQLALCHEMY_ERROR"
            msg = "❌ Falha ao executar operação no banco. Verifique parâmetros e tente novamente."
        else:
            code = "UNEXPECTED_ERROR"
            msg = f"⚠️ Ocorreu um erro inesperado: {err}"

    details["error_code"] = code

    # log com traceback (auditoria/diagnóstico)
    log_message(
        "Erro SQL tratado | "
        f"code={code} type={details['error_type']} "
        f"op={operation} dialect={dialect} table={table} column={column} "
        f"sql={details['sql']}\n{traceback.format_exc()}",
        level="error",
    )

    if return_details:
        return msg, details
    return msg, {"None": None}


class DDLExecutionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        operation: str,
        dialect: str,
        table: str,
        column: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.operation = operation
        self.dialect = dialect
        self.table = table
        self.column = column
        self.details = details or {}