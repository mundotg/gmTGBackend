"""
Análise de dados / ciência de dados sobre o RESULTADO de uma consulta.

Recebe as linhas (lista de dicts — como devolvidas pelo motor de query, seja
SQL ou MongoDB) e calcula estatísticas descritivas por coluna, correlações
numéricas e insights automáticos, tudo com pandas.

`analisar_stream(rows)` é um GERADOR que produz (stage, data) por etapas, para
o WebSocket enviar resultados em TEMPO REAL. `analisar(rows)` devolve tudo de
uma vez (para cache Redis / REST).
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import math
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

MAX_ROWS = 50000        # trava de segurança (memória)
TOP_VALUES = 10
HIST_BINS = 12
HIGH_CARD = 50          # acima disto, categórica é tratada como "texto"


def _clean(v: Any) -> Any:
    """Torna um valor JSON-safe (numpy/NaN/inf → tipos nativos/None)."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(v, (np.bool_,)):
        return bool(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (pd.Timestamp,)):
        return v.isoformat()
    return v


import re as _re
import warnings as _warnings

_DATE_LIKE = _re.compile(r"\d{4}-\d{2}|\d{2}/\d{2}|\d{2}:\d{2}")

# Regex para identificadores técnicos (chaves primárias, chaves estrangeiras, UUIDs, códigos)
_RE_IDENTIFIER = _re.compile(
    r"^(id|_id|uuid|pk|guid|code|codigo|cod)$|(^id_|_id$|id$|_pk$|_uuid$|_code$|_codigo$|_cod$)",
    _re.I,
)

# Regex para campos críticos de negócio e governança
_RE_CRITICAL = _re.compile(
    r"^(id|created_at|updated_at|data|timestamp|status|estado|email|user|usuario|cliente|valor|total|price|preco|amount|cpf|cnpj|telefone|phone)$|(_id$|_pk$|created_at|data_criacao|updated_at)",
    _re.I,
)


def _parse_schema_info(schema_info: Optional[Any]) -> Dict[str, Dict[str, Any]]:
    """
    Normaliza a estrutura de tabelas e colunas (Record<string, CampoDetalhado[]> ou List[CampoDetalhado])
    num dicionário indexado por nome da coluna (em minúsculas) e 'tabela.coluna' (em minúsculas).
    Retorna mapeamento com os metadados reais do banco de dados:
    - is_primary_key: bool
    - is_foreign_key: bool
    - is_auto_increment: bool
    - is_nullable: bool
    - is_unique: bool
    - tipo: str
    - referenced_table: Optional[str]
    - field_references: Optional[str]
    - comentario: Optional[str]
    - default: Optional[str]
    """
    if not schema_info:
        return {}
    col_map: Dict[str, Dict[str, Any]] = {}

    def _add_col(c: Dict[str, Any], table_name: Optional[str] = None):
        if not isinstance(c, dict):
            return
        name = str(c.get("nome") or c.get("name") or "").strip()
        if not name:
            return
        tbl = str(c.get("tableName") or table_name or "").strip()

        parsed = {
            "tableName": tbl if tbl else None,
            "nome": name,
            "tipo": c.get("tipo"),
            "is_nullable": bool(c.get("is_nullable", True)),
            "is_primary_key": bool(c.get("is_primary_key", False)),
            "is_foreign_key": bool(c.get("is_foreign_key", False)),
            "is_auto_increment": bool(c.get("is_auto_increment", False)),
            "referenced_table": c.get("referenced_table"),
            "field_references": c.get("field_references"),
            "is_unique": bool(c.get("is_unique", False)),
            "default": c.get("default"),
            "comentario": c.get("comentario"),
            "length": c.get("length"),
            "precision": c.get("precision"),
            "scale": c.get("scale"),
            "is_unsigned": bool(c.get("is_unsigned", False)),
        }
        col_map[name.lower()] = parsed
        if tbl:
            col_map[f"{tbl.lower()}.{name.lower()}"] = parsed

    if isinstance(schema_info, dict):
        for k, v in schema_info.items():
            if isinstance(v, list):
                for item in v:
                    _add_col(item, table_name=k)
            elif isinstance(v, dict):
                _add_col(v, table_name=k)
    elif isinstance(schema_info, list):
        for item in schema_info:
            if isinstance(item, dict):
                _add_col(item)
            elif isinstance(item, list):
                for sub in item:
                    _add_col(sub)

    return col_map


def _lookup_db_meta(name: str, schema_dict: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Procura metadados do banco de dados para a coluna especificada."""
    if not schema_dict:
        return None
    raw = str(name).strip().lower()
    if raw in schema_dict:
        return schema_dict[raw]
    # Caso contenha alias ou ponto (ex: 'u.id' -> 'id')
    if "." in raw:
        col_only = raw.split(".")[-1]
        if col_only in schema_dict:
            return schema_dict[col_only]
    # Se procurou 'id' e no schema está 'users.id'
    for k, v in schema_dict.items():
        if "." in k and k.split(".")[-1] == raw:
            return v
    return None


def _is_identifier(
    name: str,
    s: Optional[pd.Series] = None,
    db_meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """Deteta se a coluna é um identificador técnico (ex: id, uuid, id_cliente).
    Essas colunas devem ser desconsideradas como métricas de negócio para evitar
    distorções em correlações, clusters K-Means e detecção de anomalias.
    Utiliza os metadados do banco de dados (PK, FK, auto_increment) com prioridade máxima."""
    if db_meta:
        if (
            db_meta.get("is_primary_key")
            or db_meta.get("is_foreign_key")
            or db_meta.get("is_auto_increment")
        ):
            return True

    col_str = str(name).strip().lower()
    leaf = col_str.split(".")[-1]
    if _RE_IDENTIFIER.search(col_str) or _RE_IDENTIFIER.search(leaf):
        return True
    if s is not None:
        if pd.api.types.is_numeric_dtype(s):
            d = s.dropna()
            if len(d) > 3 and d.nunique() == len(d):
                if pd.api.types.is_integer_dtype(d):
                    diff = d.max() - d.min()
                    if diff == len(d) - 1:
                        return True
        elif s.dtype == object:
            sample = s.dropna().astype(str).head(50)
            if len(sample) and sample.str.contains(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-", regex=True, case=False).mean() >= 0.7:
                return True
    return False


def _is_critical_field(name: str, db_meta: Optional[Dict[str, Any]] = None) -> bool:
    """Deteta se a coluna representa um campo crítico de negócio ou governança.
    Prioriza restrições formais do banco de dados (PK, FK, NOT NULL sem default)."""
    if db_meta:
        if db_meta.get("is_primary_key") or db_meta.get("is_foreign_key"):
            return True
        if not db_meta.get("is_nullable", True) and not db_meta.get("default"):
            return True
    return bool(_RE_CRITICAL.search(str(name).strip().lower()))


def _build_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows[:MAX_ROWS])
    # Tenta refinar colunas object → numérico ou data (valores da BD podem vir string).
    for col in df.columns:
        if df[col].dtype == object:
            # 1. Se contiver objetos unhashable (dict, list, set), serializa para JSON para permitir
            # operações do pandas como duplicated(), nunique(), value_counts() sem 'unhashable type: dict'.
            try:
                has_unhashable = df[col].apply(lambda x: isinstance(x, (dict, list, set))).any()
                if has_unhashable:
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x, default=str, ensure_ascii=False)
                        if isinstance(x, (dict, list, set))
                        else x
                    )
            except Exception:
                pass

            # 2. Trava de segurança contra colunas com strings massivas (dumps, base64, result_preview gigante):
            # limita cada célula a 2000 caracteres para evitar estourar a memória ou a base de dados.
            try:
                df[col] = df[col].apply(
                    lambda x: (x[:2000] + "... [truncado]") if isinstance(x, str) and len(x) > 2000 else x
                )
            except Exception:
                pass

            num = pd.to_numeric(df[col], errors="coerce")
            if num.notna().mean() >= 0.8:  # 80%+ convertem → numérica
                df[col] = num
                continue
            # Só tenta data se os valores PARECEM datas (evita parsear texto e o aviso).
            sample = df[col].dropna().astype(str)
            if len(sample) and sample.str.contains(_DATE_LIKE).mean() >= 0.8:
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore")
                    dt = pd.to_datetime(df[col], errors="coerce", utc=False)
                if dt.notna().mean() >= 0.8:
                    df[col] = dt
    return df


def _kind(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    if pd.api.types.is_numeric_dtype(s):
        return "numeric"
    return "categorical"


def _overview(df: pd.DataFrame) -> Dict[str, Any]:
    total_cells = int(df.size)
    total_nulls = int(df.isna().sum().sum())
    try:
        n_duplicated = int(df.duplicated().sum())
    except Exception:
        n_duplicated = 0
    return {
        "n_rows": int(len(df)),
        "n_cols": int(df.shape[1]),
        "n_duplicated": n_duplicated,
        "total_cells": total_cells,
        "total_nulls": total_nulls,
        "null_pct": round(total_nulls / total_cells * 100, 2) if total_cells else 0,
        "memory_kb": float(round(df.memory_usage(deep=True).sum() / 1024, 1)),
    }


def _clean_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for k, v in row_dict.items():
        val = _clean(v)
        if val is not None and not isinstance(val, (int, float, bool, str)):
            val = str(val)
        cleaned[str(k)] = val
    return cleaned


def _get_extreme_rows(
    df: Optional[pd.DataFrame], d: pd.Series
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Any], Optional[Any]]:
    if df is None or len(d) == 0:
        return None, None, None, None
    try:
        arr = d.to_numpy()
        pos_min = int(np.argmin(arr))
        pos_max = int(np.argmax(arr))
        idx_min = d.index[pos_min]
        idx_max = d.index[pos_max]

        row_min = df.loc[idx_min]
        if isinstance(row_min, pd.DataFrame):
            row_min = row_min.iloc[0]
        row_max = df.loc[idx_max]
        if isinstance(row_max, pd.DataFrame):
            row_max = row_max.iloc[0]

        min_row = _clean_row(row_min.to_dict())
        max_row = _clean_row(row_max.to_dict())
        min_idx = int(idx_min) if isinstance(idx_min, (int, np.integer)) else str(idx_min)
        max_idx = int(idx_max) if isinstance(idx_max, (int, np.integer)) else str(idx_max)
        return min_row, max_row, min_idx, max_idx
    except Exception:
        return None, None, None, None


def _column_stats(
    name: str,
    s: pd.Series,
    df: Optional[pd.DataFrame] = None,
    db_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    kind = _kind(s)
    n = int(len(s))
    nulls = int(s.isna().sum())
    try:
        distinct = int(s.nunique(dropna=True))
    except Exception:
        try:
            distinct = int(s.astype(str).nunique(dropna=True))
        except Exception:
            distinct = 0

    is_id = _is_identifier(name, s, db_meta)
    is_crit = _is_critical_field(name, db_meta) or (distinct == n and n > 1)
    comp_pct = round((n - nulls) / n * 100, 2) if n else 0.0

    base: Dict[str, Any] = {
        "name": name,
        "kind": kind,
        "is_identifier": is_id,
        "is_critical": is_crit,
        "count": n - nulls,
        "nulls": nulls,
        "null_pct": round(nulls / n * 100, 2) if n else 0,
        "completeness_pct": comp_pct,
        "distinct": distinct,
        "distinct_pct": round(distinct / n * 100, 2) if n else 0,
        "is_primary_key": bool(db_meta.get("is_primary_key", False)) if db_meta else False,
        "is_foreign_key": bool(db_meta.get("is_foreign_key", False)) if db_meta else False,
        "is_auto_increment": bool(db_meta.get("is_auto_increment", False)) if db_meta else False,
        "is_nullable": bool(db_meta.get("is_nullable", True)) if db_meta else True,
        "referenced_table": db_meta.get("referenced_table") if db_meta else None,
        "field_references": db_meta.get("field_references") if db_meta else None,
        "tipo_db": db_meta.get("tipo") if db_meta else None,
        "comentario": db_meta.get("comentario") if db_meta else None,
        "db_meta": db_meta,
    }

    if kind == "numeric":
        d = s.dropna()
        if len(d):
            min_row, max_row, min_idx, max_idx = _get_extreme_rows(df, d)
            counts, edges = np.histogram(d, bins=min(HIST_BINS, max(1, distinct)))
            q1 = float(d.quantile(0.25))
            q3 = float(d.quantile(0.75))
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            outliers = int(((d < lower) | (d > upper)).sum())
            mean = float(d.mean())
            std = float(d.std()) if len(d) > 1 else 0.0
            mode_vals = d.mode()
            base["numeric"] = {
                "min": _clean(d.min()),
                "max": _clean(d.max()),
                "min_row": min_row,
                "max_row": max_row,
                "min_index": min_idx,
                "max_index": max_idx,
                "mean": _clean(mean),
                "median": _clean(d.median()),
                "std": _clean(std),
                "q25": _clean(q1),
                "q75": _clean(q3),
                "p90": _clean(d.quantile(0.90)),
                "p95": _clean(d.quantile(0.95)),
                "p99": _clean(d.quantile(0.99)),
                "mode": _clean(mode_vals.iloc[0]) if len(mode_vals) else None,
                "range": _clean(float(d.max()) - float(d.min())),
                "cv": _clean(std / mean) if mean else None,   # coef. de variação
                "skew": _clean(d.skew()) if len(d) > 2 else None,      # assimetria
                "kurtosis": _clean(d.kurt()) if len(d) > 3 else None,  # curtose
                "zeros": int((d == 0).sum()),
                "negatives": int((d < 0).sum()),
                "outliers": {
                    "count": outliers,
                    "pct": round(outliers / len(d) * 100, 2),
                    "lower": _clean(lower),
                    "upper": _clean(upper),
                },
                "histogram": {
                    "counts": [int(c) for c in counts],
                    "edges": [_clean(e) for e in edges],
                },
            }
    elif kind == "datetime":
        d = s.dropna()
        if len(d):
            min_row, max_row, min_idx, max_idx = _get_extreme_rows(df, d)
            base["datetime"] = {
                "min": _clean(d.min()),
                "max": _clean(d.max()),
                "min_row": min_row,
                "max_row": max_row,
                "min_index": min_idx,
                "max_index": max_idx,
            }
    elif kind == "boolean":
        vc = s.dropna().value_counts()
        base["boolean"] = {
            "true": int(vc.get(True, 0)),
            "false": int(vc.get(False, 0)),
        }
    else:  # categorical / texto
        clean = s.dropna().astype(str)
        vc_full = clean.value_counts()
        vc = vc_full.head(TOP_VALUES)
        total = int(clean.shape[0]) or 1
        # Entropia normalizada (0 = uma só categoria; 1 = uniforme).
        entropy = 0.0
        if len(vc_full) > 1:
            p = (vc_full / vc_full.sum()).to_numpy()
            entropy = float(-(p * np.log2(p)).sum() / math.log2(len(vc_full)))
        base["categorical"] = {
            "cardinality": distinct,
            "is_text": distinct > HIGH_CARD,
            "mode": str(vc_full.index[0]) if len(vc_full) else None,
            "imbalance_pct": round(int(vc_full.iloc[0]) / total * 100, 2) if len(vc_full) else 0,
            "entropy": round(entropy, 3),
            "semantic": _detect_semantic(clean),
            "top": [
                {"value": k, "count": int(v), "pct": round(int(v) / total * 100, 2)}
                for k, v in vc.items()
            ],
        }
    return base


_SEMANTIC = {
    "email": _re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "url": _re.compile(r"^https?://", _re.I),
    "uuid": _re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-", _re.I),
    "telefone": _re.compile(r"^\+?\d[\d\s\-()]{6,}$"),
    "cep/código": _re.compile(r"^\d{4,8}$"),
}


def _detect_semantic(s: pd.Series) -> Optional[str]:
    """Deteta o 'significado' de uma coluna de texto (email, url, uuid, …)."""
    sample = s.head(80)
    if not len(sample):
        return None
    for name, pat in _SEMANTIC.items():
        if sample.map(lambda v: bool(pat.match(str(v)))).mean() >= 0.8:
            return name
    return None


def _correlations(
    df: pd.DataFrame, schema_dict: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    # Exclui identificadores técnicos (ex: id, id_cliente, PK, FK) para não gerar correlações espúrias
    num_cols = [
        c
        for c in df.select_dtypes(include=[np.number]).columns
        if not _is_identifier(str(c), df[c], _lookup_db_meta(str(c), schema_dict or {}))
    ]
    if len(num_cols) < 2:
        return {"columns": [], "matrix": []}
    num = df[num_cols]
    corr = num.corr(numeric_only=True).round(3)
    cols = list(corr.columns)
    matrix = [[_clean(corr.iloc[i, j]) for j in range(len(cols))] for i in range(len(cols))]
    return {"columns": [str(c) for c in cols], "matrix": matrix}


def _quality(
    overview: Dict[str, Any],
    columns: List[Dict[str, Any]],
    schema_dict: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Resumo de QUALIDADE dos dados + score baseado em 4 dimensões (0–100).
    Dimensões avaliadas:
    1. Completude (40%): Ausência de valores nulos, com ênfase em campos críticos.
    2. Unicidade (25%): Ausência de linhas duplicadas.
    3. Variabilidade (20%): Ausência de colunas com valor único constante.
    4. Identificabilidade (15%): Presença de chave candidata única ou identificadores técnicos.
    """
    n = overview.get("n_rows", 0) or 1
    total_cols = max(1, len(columns))
    constant = [c["name"] for c in columns if c["distinct"] <= 1]
    high_null = [c["name"] for c in columns if c["null_pct"] >= 50]
    keys = [
        c["name"] for c in columns
        if c["distinct"] == overview.get("n_rows") and overview.get("n_rows", 0) > 1
    ]

    # 1. Completude por coluna e por campos críticos
    completeness_by_column: List[Dict[str, Any]] = []
    critical_fields: List[Dict[str, Any]] = []
    for c in columns:
        c_name = str(c["name"])
        comp_pct = round(100.0 - float(c.get("null_pct", 0.0)), 2)
        db_meta = c.get("db_meta") or _lookup_db_meta(c_name, schema_dict or {})
        is_crit = bool(c.get("is_critical", False) or _is_critical_field(c_name, db_meta))
        is_id = bool(c.get("is_identifier", False) or _is_identifier(c_name, db_meta=db_meta))

        reason = "Campo Crítico de Negócio"
        if db_meta:
            if db_meta.get("is_primary_key"):
                reason = "Chave Primária (PK) - Integridade Estrutural"
            elif db_meta.get("is_foreign_key"):
                ref = f" → {db_meta.get('referenced_table')}" if db_meta.get("referenced_table") else ""
                reason = f"Chave Estrangeira (FK{ref}) - Integridade Relacional"
            elif not db_meta.get("is_nullable", True) and not db_meta.get("default"):
                reason = "Restrição NOT NULL no Banco de Dados"

        item = {
            "column": c_name,
            "completeness_pct": comp_pct,
            "nulls": int(c.get("nulls", 0)),
            "is_critical": is_crit,
            "is_identifier": is_id,
            "reason": reason if is_crit else None,
            "is_primary_key": c.get("is_primary_key", False),
            "is_foreign_key": c.get("is_foreign_key", False),
            "referenced_table": c.get("referenced_table"),
            "tipo_db": c.get("tipo_db"),
        }
        completeness_by_column.append(item)
        if is_crit:
            critical_fields.append({
                "column": c_name,
                "completeness_pct": comp_pct,
                "nulls": int(c.get("nulls", 0)),
                "reason": reason,
                "is_primary_key": c.get("is_primary_key", False),
                "is_foreign_key": c.get("is_foreign_key", False),
                "referenced_table": c.get("referenced_table"),
                "tipo_db": c.get("tipo_db"),
            })

    if critical_fields:
        crit_comp = float(np.mean([cf["completeness_pct"] for cf in critical_fields]))
    else:
        crit_comp = round(100.0 - float(overview.get("null_pct", 0.0)), 2)

    # Dimensão 1: Completude (40%)
    raw_comp = max(0.0, 100.0 - float(overview.get("null_pct", 0.0)))
    score_comp = round((raw_comp * 0.5) + (crit_comp * 0.5), 1)

    # Dimensão 2: Unicidade (25%)
    dup_pct = (float(overview.get("n_duplicated", 0)) / n) * 100.0
    score_uniq = max(0.0, round(100.0 - dup_pct, 1))

    # Dimensão 3: Variabilidade (20%)
    const_pct = (float(len(constant)) / total_cols) * 100.0
    score_var = max(0.0, round(100.0 - const_pct, 1))

    # Dimensão 4: Identificabilidade (15%)
    has_key = len(keys) > 0 or any(c.get("is_identifier") for c in columns)
    score_ident = 100.0 if has_key else 70.0

    # Fórmula ponderada:
    final_score = round(
        (0.40 * score_comp)
        + (0.25 * score_uniq)
        + (0.20 * score_var)
        + (0.15 * score_ident),
        1,
    )
    final_score = max(0.0, min(100.0, final_score))

    dimensions = [
        {
            "name": "Completude",
            "weight": 0.40,
            "weight_pct": 40,
            "score": score_comp,
            "weighted_score": round(0.40 * score_comp, 1),
            "status": "Excelente" if score_comp >= 90 else ("Bom" if score_comp >= 75 else ("Atenção" if score_comp >= 50 else "Crítico")),
            "description": "Preenchimento de células e campos críticos da base (ausência de nulos).",
            "metrics": f"{raw_comp:.1f}% geral preenchido · {crit_comp:.1f}% em campos críticos.",
        },
        {
            "name": "Unicidade",
            "weight": 0.25,
            "weight_pct": 25,
            "score": score_uniq,
            "weighted_score": round(0.25 * score_uniq, 1),
            "status": "Excelente" if score_uniq >= 95 else ("Atenção" if score_uniq >= 80 else "Crítico"),
            "description": "Ausência de linhas redundantes ou duplicadas na amostra.",
            "metrics": f"{overview.get('n_duplicated', 0)} linhas duplicadas ({dup_pct:.1f}% da amostra).",
        },
        {
            "name": "Variabilidade",
            "weight": 0.20,
            "weight_pct": 20,
            "score": score_var,
            "weighted_score": round(0.20 * score_var, 1),
            "status": "Excelente" if score_var >= 90 else ("Atenção" if score_var >= 70 else "Crítico"),
            "description": "Riqueza informativa (penaliza colunas com 1 só valor constante).",
            "metrics": f"{len(constant)} coluna(s) constante(s) de {total_cols} analisadas.",
        },
        {
            "name": "Identificabilidade",
            "weight": 0.15,
            "weight_pct": 15,
            "score": score_ident,
            "weighted_score": round(0.15 * score_ident, 1),
            "status": "Excelente" if score_ident >= 90 else "Atenção",
            "description": "Presença de chaves primárias ou identificadores únicos para integridade.",
            "metrics": f"{len(keys)} chave(s) única(s) encontrada(s)." if keys else "Sem chave primária única explícita.",
        },
    ]

    return {
        "score": final_score,
        "formula": "Score = 0.40 × Completude + 0.25 × Unicidade + 0.20 × Variabilidade + 0.15 × Identificabilidade",
        "dimensions": dimensions,
        "critical_completeness_pct": round(crit_comp, 1),
        "critical_fields": critical_fields,
        "completeness_by_column": completeness_by_column,
        "constant_columns": constant,
        "high_null_columns": high_null,
        "potential_keys": keys,
    }


def _insights(
    overview: Dict[str, Any],
    columns: List[Dict[str, Any]],
    corr: Dict[str, Any],
    trend: Optional[Dict[str, Any]] = None,
    segments: Optional[Dict[str, Any]] = None,
    anomalies: Optional[Dict[str, Any]] = None,
    quality: Optional[Dict[str, Any]] = None,
) -> List[str]:
    out: List[str] = []
    if overview["n_rows"] == 0:
        return ["O resultado não tem linhas para analisar."]

    # 🔭 Negócio primeiro (o que mais interessa).
    if trend:
        if trend.get("has_enough_data"):
            prev = trend.get("forecast_next")
            ci_l = trend.get("forecast_ci_lower")
            ci_u = trend.get("forecast_ci_upper")
            time_col = trend.get("time_column", "tempo")
            ci_str = f" [IC 95%: {ci_l} a {ci_u}]" if ci_l is not None and ci_u is not None else ""
            out.append(
                f"📈 Tendência {trend['freq']} de '{trend['measure']}' (baseada em '{time_col}'): {trend['direction']} "
                f"({trend['change_pct']:+}%). Previsão para o próximo período: ~{prev}{ci_str}."
            )
        else:
            out.append(
                f"⏳ Série temporal de '{trend.get('measure')}': dados insuficientes para projeção estatística confiável "
                f"({trend.get('data_points', 0)} de 5 períodos necessários)."
            )

    if segments:
        if segments.get("has_enough_data"):
            out.append(f"👥 Os dados dividem-se em {segments['k']} segmento(s)/perfil(is) de negócio distintos.")
        else:
            out.append(f"👥 Segmentação K-Means: {segments.get('reason', 'Dados insuficientes para formar clusters.')}")

    if anomalies and anomalies.get("count", 0) > 0:
        rows = anomalies.get("rows", [])
        if rows and rows[0].get("justification"):
            out.append(f"🚨 {anomalies['count']} anomalia(s) detectada(s) via {anomalies.get('method', 'Z-Score')}. Exemplo: {rows[0]['justification']}")
        else:
            out.append(f"🚨 {anomalies['count']} linha(s) anómala(s) detectada(s) via {anomalies.get('method', 'Z-Score')}.")

    if quality and quality.get("critical_fields"):
        crit_low = [cf["column"] for cf in quality["critical_fields"] if cf["completeness_pct"] < 90]
        if crit_low:
            out.append(f"⚠️ Atenção em campos críticos: '{', '.join(crit_low)}' com completude abaixo de 90%.")

    if overview["n_duplicated"] > 0:
        out.append(
            f"Há {overview['n_duplicated']} linha(s) com todas as colunas selecionadas idênticas. "
            f"(Dica: para distinguir cada registo individualmente, inclua a chave primária ou ID único da mensagem no SELECT)."
        )

    for c in columns:
        if c.get("is_identifier"):
            continue  # Não reporta métricas de negócio para chaves e IDs
        if c["null_pct"] >= 30:
            out.append(f"A coluna '{c['name']}' tem {c['null_pct']}% de valores em falta (completude de {c.get('completeness_pct', 100 - c['null_pct'])}%).")
        if c["distinct"] <= 1:
            if "." in c["name"]:
                out.append(
                    f"A coluna '{c['name']}' possui apenas 1 valor único em todas as linhas. "
                    f"Em dados combinados com JOIN (1:N), é normal a tabela pai ter o mesmo valor para todas as linhas filtradas."
                )
            else:
                out.append(f"A coluna '{c['name']}' é constante (um só valor em todas as linhas) — sem variância para análise comparativa.")
        if c["distinct"] == overview["n_rows"] and overview["n_rows"] > 1 and c["kind"] != "numeric":
            out.append(f"A coluna '{c['name']}' é única em todas as linhas (candidata a chave primária).")

        if c["kind"] == "numeric":
            num = c.get("numeric", {})
            o = num.get("outliers", {})
            if o.get("count", 0) > 0 and o.get("pct", 0) >= 2:
                out.append(f"'{c['name']}': {o['count']} outliers ({o['pct']}%) fora de [{o.get('lower')}, {o.get('upper')}].")
            sk = num.get("skew")
            if sk is not None and abs(sk) >= 1:
                lado = "à direita (cauda alta)" if sk > 0 else "à esquerda (cauda baixa)"
                out.append(f"'{c['name']}': distribuição assimétrica {lado} (skew={round(sk, 2)}).")

        if c["kind"] == "categorical":
            cat = c.get("categorical", {})
            if cat.get("imbalance_pct", 0) >= 80 and c["distinct"] > 1:
                out.append(f"'{c['name']}': muito desequilibrada — '{cat.get('mode')}' representa {cat['imbalance_pct']}%.")
            if cat.get("semantic"):
                out.append(f"'{c['name']}': parece conter {cat['semantic']}.")

    # correlações fortes
    cols = corr.get("columns", [])
    m = corr.get("matrix", [])
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            v = m[i][j]
            if v is not None and abs(v) >= 0.8:
                sinal = "positiva" if v > 0 else "negativa"
                out.append(f"Correlação {sinal} forte ({v}) entre '{cols[i]}' e '{cols[j]}'.")
    return out[:16]


# ══════════════════════════ ANÁLISES AVANÇADAS (negócio) ══════════════════════════
def _find_best_datetime_col(
    df: pd.DataFrame, schema_dict: Optional[Dict[str, Dict[str, Any]]] = None
) -> Optional[str]:
    """Seleciona a melhor coluna temporal priorizando created_at, data_criacao, timestamp."""
    dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    if not dt_cols:
        return None

    def _score_dt(col: str) -> int:
        c = str(col).lower().strip()
        db_meta = _lookup_db_meta(c, schema_dict or {}) if schema_dict else None
        if c == "created_at":
            return 100
        if "created_at" in c:
            return 90
        if any(k in c for k in ("data_criacao", "criado_em", "criadoem")):
            return 80
        if db_meta and any(
            t in str(db_meta.get("tipo", "")).lower()
            for t in ("timestamp", "datetime", "timestamptz")
        ):
            return 75
        if any(k in c for k in ("timestamp", "datetime", "data_hora", "datahora")):
            return 70
        if any(k in c for k in ("data", "date")):
            return 60
        return 10

    dt_cols.sort(key=_score_dt, reverse=True)
    return dt_cols[0]


def _tendencia(
    df: pd.DataFrame, schema_dict: Optional[Dict[str, Dict[str, Any]]] = None
) -> Optional[Dict[str, Any]]:
    """Tendência temporal: agrega uma medida por dia ou semana (priorizando created_at)
    e calcula regressão linear com intervalo de confiança de 95%."""
    dtc = _find_best_datetime_col(df, schema_dict)
    if not dtc:
        return None

    # Exclui identificadores técnicos de medidas numéricas de negócio
    num_cols = [
        c
        for c in df.select_dtypes(include=[np.number]).columns
        if not _is_identifier(str(c), df[c], _lookup_db_meta(str(c), schema_dict or {}))
    ]

    d = pd.DataFrame({"_t": df[dtc]})
    if num_cols:
        measure = num_cols[0]
        d["_m"] = pd.to_numeric(df[measure], errors="coerce")
        label = str(measure)
    else:
        d["_m"] = 1.0
        label = "contagem de registros"

    d = d.dropna(subset=["_t"])
    if len(d) < 3:
        return None

    span = (d["_t"].max() - d["_t"].min()).days
    # Requisito 2: Agregar por dia ou semana, usando created_at
    freq = "D" if span <= 90 else "W"
    freq_label = "diária" if freq == "D" else "semanal"

    ts = d.set_index("_t")["_m"].resample(freq).sum().dropna()
    series_points = [
        {"t": (idx.date().isoformat() if hasattr(idx, "date") else str(idx)), "v": _clean(v)}
        for idx, v in ts.items()
    ]

    # Requisito 3: Exigir dados temporais suficientes e apresentar intervalos de confiança
    if len(ts) < 5:
        return {
            "has_enough_data": False,
            "time_column": str(dtc),
            "measure": label,
            "freq": freq_label,
            "data_points": len(ts),
            "min_required_points": 5,
            "reason": (
                f"Dados temporais insuficientes ({len(ts)} períodos). São necessários no mínimo 5 pontos "
                f"temporais agregados ({freq_label}) para calcular uma tendência com intervalo de confiança estatisticamente confiável."
            ),
            "direction": "indeterminado",
            "change_pct": 0.0,
            "forecast_next": None,
            "series": series_points,
        }

    y = ts.to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_fit = slope * x + intercept
    residuals = y - y_fit
    dof = max(1, len(y) - 2)
    s_err = math.sqrt(float(np.sum(residuals**2)) / dof)
    x_next = float(len(y))
    x_bar = float(np.mean(x))
    ss_x = float(np.sum((x - x_bar) ** 2))
    se_pred = s_err * math.sqrt(1.0 + (1.0 / len(y)) + (((x_next - x_bar) ** 2) / (ss_x if ss_x > 0 else 1.0)))

    # t-Student aproximado para 95% de confiança
    t_crit = 2.0 if len(y) >= 15 else (2.3 if len(y) >= 8 else 2.78)
    margin = float(t_crit * se_pred)
    forecast = float(slope * x_next + intercept)
    forecast_clean = _clean(round(max(0.0, forecast), 2))
    forecast_ci_lower = _clean(round(max(0.0, forecast - margin), 2))
    forecast_ci_upper = _clean(round(forecast + margin, 2))

    change_pct = ((y[-1] - y[0]) / abs(y[0]) * 100) if y[0] else 0.0
    if slope > 0 and abs(change_pct) >= 5:
        direction = "a subir"
    elif slope < 0 and abs(change_pct) >= 5:
        direction = "a descer"
    else:
        direction = "estável"

    total_ss = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = float(max(0.0, 1.0 - (float(np.sum(residuals**2)) / (total_ss if total_ss > 0 else 1.0))))

    series_enriched = []
    for i, (idx, v) in enumerate(ts.items()):
        series_enriched.append({
            "t": (idx.date().isoformat() if hasattr(idx, "date") else str(idx)),
            "v": _clean(v),
            "trend": _clean(round(float(y_fit[i]), 2)),
        })

    return {
        "has_enough_data": True,
        "time_column": str(dtc),
        "measure": label,
        "freq": freq_label,
        "direction": direction,
        "change_pct": round(float(change_pct), 1),
        "forecast_next": forecast_clean,
        "forecast_ci_lower": forecast_ci_lower,
        "forecast_ci_upper": forecast_ci_upper,
        "confidence_level": 95,
        "margin_of_error": _clean(round(margin, 2)),
        "r_squared": _clean(round(r_squared, 3)),
        "series": series_enriched,
    }


def _anomalias(
    df: pd.DataFrame,
    schema_dict: Optional[Dict[str, Dict[str, Any]]] = None,
    top: int = 12,
) -> Dict[str, Any]:
    """Linhas anómalas: detecta anomalias multivariadas por Z-score em colunas numéricas de negócio,
    excluindo identificadores técnicos, e fornecendo justificativa clara para cada registro afetado."""
    method_name = "Z-Score Multivariado (|Z| > 3.0 desvios padrão)"
    # Requisito 5: Excluir identificadores técnicos (PK, FK, IDs)
    business_num = [
        c
        for c in df.select_dtypes(include=[np.number]).columns
        if not _is_identifier(str(c), df[c], _lookup_db_meta(str(c), schema_dict or {}))
    ]
    if len(business_num) == 0 or len(df) < 10:
        return {"count": 0, "method": method_name, "rows": []}

    num = df[business_num]
    std = num.std(ddof=0).replace(0, np.nan)
    z = (num - num.mean()) / std
    score = z.abs().max(axis=1)
    count = int((score > 3).sum())
    idxs = score[score > 3].sort_values(ascending=False).head(top).index

    # Identifica coluna de chave primária real na tabela para rotular o registro afetado com exatidão
    primary_id_col = None
    if schema_dict:
        for c in df.columns:
            m = _lookup_db_meta(str(c), schema_dict)
            if m and m.get("is_primary_key"):
                primary_id_col = str(c)
                break
    if not primary_id_col:
        id_cols = [
            c
            for c in df.columns
            if _is_identifier(str(c), df[c], _lookup_db_meta(str(c), schema_dict or {}))
        ]
        primary_id_col = id_cols[0] if id_cols else None

    rows = []
    for i in idxs:
        # Requisito 4: Mostrar o registro afetado, o método e a justificação
        culprit_col = str(z.loc[i].abs().idxmax())
        culprit_z = float(z.loc[i, culprit_col])
        col_mean = float(num[culprit_col].mean())
        col_std = float(num[culprit_col].std(ddof=0))
        culprit_val = _clean(df.loc[i, culprit_col])

        pk_val = _clean(df.loc[i, primary_id_col]) if primary_id_col else None
        record_id = f"{primary_id_col}={pk_val}" if pk_val is not None else f"#{i}"

        sinal = "acima" if culprit_z > 0 else "abaixo"
        justification = (
            f"A coluna '{culprit_col}' apresenta valor {culprit_val}, situando-se a "
            f"{abs(culprit_z):.1f} desvios padrão {sinal} da média da base (média: {col_mean:.2f} ± {col_std:.2f})."
        )

        row_dict = {}
        for c in df.columns:
            val = _clean(df.loc[i, c])
            if isinstance(val, str) and len(val) > 250:
                val = val[:250] + "... [truncado]"
            row_dict[str(c)] = val

        rows.append({
            "index": int(i),
            "record_id": str(record_id),
            "score": _clean(round(float(score[i]), 2)),
            "culprit_column": culprit_col,
            "culprit_value": culprit_val,
            "culprit_z": round(culprit_z, 2),
            "justification": justification,
            "row": row_dict,
        })

    return {"count": count, "method": method_name, "rows": rows}


def _kmeans(X: np.ndarray, k: int, iters: int = 30) -> np.ndarray:
    rng = np.random.default_rng(42)
    centroids = X[rng.choice(len(X), k, replace=False)]
    labels = np.zeros(len(X), dtype=int)
    for _ in range(iters):
        d = np.linalg.norm(X[:, None, :] - centroids[None, :, :], axis=2)
        labels = d.argmin(axis=1)
        new = np.array([
            X[labels == j].mean(axis=0) if (labels == j).any() else centroids[j]
            for j in range(k)
        ])
        if np.allclose(new, centroids):
            break
        centroids = new
    return labels


def _segmentos(
    df: pd.DataFrame, schema_dict: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Segmenta as linhas em grupos (k-means) pelas colunas numéricas de negócio — 'perfis'.
    Requisito 6: Explica quando não existem dados suficientes para formar clusters."""
    # Requisito 5: Excluir identificadores técnicos de clusters
    non_id_cols = [
        c
        for c in df.select_dtypes(include=[np.number]).columns
        if not _is_identifier(str(c), df[c], _lookup_db_meta(str(c), schema_dict or {}))
    ]
    clean_rows = df[non_id_cols].dropna() if len(non_id_cols) >= 1 else pd.DataFrame()

    # Requisito 6: Explicar didaticamente quando não há dados suficientes
    if len(non_id_cols) < 2 or len(clean_rows) < 15:
        return {
            "has_enough_data": False,
            "reason": "Dados insuficientes para formar clusters estatisticamente consistentes.",
            "details": (
                "O algoritmo K-Means requer pelo menos 2 métricas numéricas de negócio (identificadores "
                "técnicos como chaves primárias, estrangeiras e IDs são desconsiderados) e um mínimo de 15 registros válidos para descobrir perfis e personas."
            ),
            "requirements": [
                {
                    "label": "Mínimo de 2 variáveis numéricas de negócio (sem IDs)",
                    "met": len(non_id_cols) >= 2,
                    "current": f"{len(non_id_cols)} variável(eis) encontrada(s)" + (f": {', '.join(str(c) for c in non_id_cols)}" if non_id_cols else ""),
                },
                {
                    "label": "Mínimo de 15 registros válidos sem valores ausentes",
                    "met": len(clean_rows) >= 15,
                    "current": f"{len(clean_rows)} linha(s) preenchida(s)",
                },
            ],
            "k": 0,
            "columns": [str(c) for c in non_id_cols],
            "segments": [],
        }

    num = clean_rows
    X = num.to_numpy(dtype=float)
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd[sd == 0] = 1.0
    Xs = (X - mu) / sd
    k = min(4, max(2, len(num) // 30))
    labels = _kmeans(Xs, k)
    segs = []
    for j in range(k):
        mask = labels == j
        size = int(mask.sum())
        if size == 0:
            continue
        center = {str(col): _clean(round(float(num.iloc[:, i][mask].mean()), 2)) for i, col in enumerate(num.columns)}
        segs.append({"id": int(j), "size": size, "pct": round(size / len(num) * 100, 1), "center": center})
    segs.sort(key=lambda s: -s["size"])
    return {
        "has_enough_data": True,
        "k": len(segs),
        "columns": [str(c) for c in num.columns],
        "segments": segs,
    }


def analisar_stream(
    rows: List[Dict[str, Any]], schema_info: Optional[Any] = None
) -> Iterator[Tuple[str, Any]]:
    """Gera (stage, data) por etapas — para o WebSocket enviar em tempo real."""
    if not rows:
        yield "overview", {"n_rows": 0, "n_cols": 0}
        yield "insights", ["Sem dados para analisar."]
        yield "done", {"ok": True}
        return

    schema_dict = _parse_schema_info(schema_info)
    df = _build_df(rows)
    overview = _overview(df)
    yield "overview", overview

    columns: List[Dict[str, Any]] = []
    for name in df.columns:
        col_meta = _lookup_db_meta(str(name), schema_dict)
        stats = _column_stats(str(name), df[name], df, db_meta=col_meta)
        columns.append(stats)
        yield "column", stats

    corr = _correlations(df, schema_dict=schema_dict)
    yield "correlations", corr

    quality = _quality(overview, columns, schema_dict=schema_dict)
    yield "quality", quality

    # Análises avançadas (negócio) — cada uma pode não se aplicar (None).
    trend = _tendencia(df, schema_dict=schema_dict)
    if trend:
        yield "trend", trend

    anomalies = _anomalias(df, schema_dict=schema_dict)
    yield "anomalies", anomalies

    segments = _segmentos(df, schema_dict=schema_dict)
    if segments:
        yield "segments", segments

    insights = _insights(overview, columns, corr, trend, segments, anomalies, quality)
    yield "insights", insights

    yield "done", {"ok": True}


def analisar(rows: List[Dict[str, Any]], schema_info: Optional[Any] = None) -> Dict[str, Any]:
    """Resultado completo de uma vez (para cache Redis / REST)."""
    result: Dict[str, Any] = {
        "overview": {}, "columns": [], "correlations": {}, "quality": {},
        "trend": None, "anomalies": None, "segments": None, "insights": [],
    }
    for stage, data in analisar_stream(rows, schema_info=schema_info):
        if stage == "column":
            result["columns"].append(data)
        elif stage in result:
            result[stage] = data
    return result


# ══════════════════════════ INFRAESTRUTURA PREDITIVA (DIKW) ══════════════════════════
def prever_saturacao_infraestrutura(
    historico_valores: List[float],
    capacidade_maxima: float,
    unidade: str = "MB",
    intervalo_dias: float = 1.0,
    nome_metrica: str = "armazenamento",
) -> Dict[str, Any]:
    """
    Infraestrutura Preditiva (Escala DIKW - Tarefa 2.1):
    Aplica regressão linear (np.polyfit) sobre séries temporais de infraestrutura
    (contagem de linhas, volume em disco, conexões do pool) para calcular:
    - Taxa de crescimento diário (slope)
    - Previsão em dias até a saturação (100% da capacidade)
    - Data estimada do esgotamento
    - Nível de risco operacional ('critico', 'alerta', 'estavel')
    - Projeções futuras (+7d, +30d, +90d)
    """
    if not historico_valores or capacidade_maxima <= 0:
        return {
            "metrica": nome_metrica,
            "status": "indeterminado",
            "mensagem": "Dados insuficientes ou capacidade inválida para projeção.",
            "dias_ate_saturacao": None,
            "nivel_risco": "estavel",
        }

    y = np.array(historico_valores, dtype=float)
    valor_atual = float(y[-1])
    capacidade = float(capacidade_maxima)
    pct_atual = round((valor_atual / capacidade) * 100, 2)

    # Caso com 1 único valor: gera baseline comparativa
    if len(y) < 2:
        return {
            "metrica": nome_metrica,
            "unidade": unidade,
            "valor_atual": _clean(round(valor_atual, 2)),
            "capacidade_maxima": _clean(round(capacidade, 2)),
            "ocupacao_pct": pct_atual,
            "taxa_crescimento_diaria": 0.0,
            "dias_ate_saturacao": None,
            "data_estimada_saturacao": None,
            "nivel_risco": "alerta" if pct_atual >= 80 else "estavel",
            "recomendacao": (
                f"Ocupação atual em {pct_atual}%. Acumulando histórico para regressão linear."
            ),
            "projecao_proximos_dias": {
                "em_7_dias": _clean(round(valor_atual, 2)),
                "em_30_dias": _clean(round(valor_atual, 2)),
                "em_90_dias": _clean(round(valor_atual, 2)),
            },
        }

    x = np.arange(len(y), dtype=float) * intervalo_dias
    slope, intercept = np.polyfit(x, y, 1)
    slope_diario = float(slope / intervalo_dias)

    # Projeção de saturação
    if slope_diario > 0:
        dias_restantes = (capacidade - valor_atual) / slope_diario
        dias_ate_saturacao = round(max(0.0, float(dias_restantes)), 1)
        data_estimada = (datetime.now() + timedelta(days=dias_ate_saturacao)).strftime("%Y-%m-%d")

        if dias_ate_saturacao <= 7 or pct_atual >= 90:
            nivel_risco = "critico"
            recomendacao = (
                f"🚨 Risco Crítico: Saturação prevista em ~{dias_ate_saturacao} dias "
                f"({data_estimada}). Recomenda-se aumentar a cota ou liberar espaço imediatamente."
            )
        elif dias_ate_saturacao <= 30 or pct_atual >= 75:
            nivel_risco = "alerta"
            recomendacao = (
                f"⚠️ Alerta de Capacidade: Esgotamento projetado para daqui a ~{dias_ate_saturacao} dias. "
                "Planeje expansão de infraestrutura ou limpeza preventiva."
            )
        else:
            nivel_risco = "estavel"
            recomendacao = (
                f"✅ Capacidade estável. Esgotamento projetado em ~{dias_ate_saturacao} dias. "
                "Crescimento sob controle."
            )
    else:
        dias_ate_saturacao = None
        data_estimada = None
        nivel_risco = "estavel"
        recomendacao = "✅ Recurso estável ou com tendência de redução no consumo."

    proj_7 = max(0.0, float(valor_atual + (slope_diario * 7)))
    proj_30 = max(0.0, float(valor_atual + (slope_diario * 30)))
    proj_90 = max(0.0, float(valor_atual + (slope_diario * 90)))

    return {
        "metrica": nome_metrica,
        "unidade": unidade,
        "valor_atual": _clean(round(valor_atual, 2)),
        "capacidade_maxima": _clean(round(capacidade, 2)),
        "ocupacao_pct": pct_atual,
        "taxa_crescimento_diaria": _clean(round(slope_diario, 4)),
        "dias_ate_saturacao": dias_ate_saturacao,
        "data_estimada_saturacao": data_estimada,
        "nivel_risco": nivel_risco,
        "recomendacao": recomendacao,
        "projecao_proximos_dias": {
            "em_7_dias": _clean(round(proj_7, 2)),
            "em_30_dias": _clean(round(proj_30, 2)),
            "em_90_dias": _clean(round(proj_90, 2)),
        },
    }

