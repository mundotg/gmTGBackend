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


def _build_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows[:MAX_ROWS])
    # Tenta refinar colunas object → numérico ou data (valores da BD podem vir string).
    for col in df.columns:
        if df[col].dtype == object:
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
    return {
        "n_rows": int(len(df)),
        "n_cols": int(df.shape[1]),
        "n_duplicated": int(df.duplicated().sum()),
        "total_cells": total_cells,
        "total_nulls": total_nulls,
        "null_pct": round(total_nulls / total_cells * 100, 2) if total_cells else 0,
        "memory_kb": float(round(df.memory_usage(deep=True).sum() / 1024, 1)),
    }


def _column_stats(name: str, s: pd.Series) -> Dict[str, Any]:
    kind = _kind(s)
    n = int(len(s))
    nulls = int(s.isna().sum())
    distinct = int(s.nunique(dropna=True))
    base: Dict[str, Any] = {
        "name": name,
        "kind": kind,
        "count": n - nulls,
        "nulls": nulls,
        "null_pct": round(nulls / n * 100, 2) if n else 0,
        "distinct": distinct,
        "distinct_pct": round(distinct / n * 100, 2) if n else 0,
    }

    if kind == "numeric":
        d = s.dropna()
        if len(d):
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
            base["datetime"] = {"min": _clean(d.min()), "max": _clean(d.max())}
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


def _correlations(df: pd.DataFrame) -> Dict[str, Any]:
    num = df.select_dtypes(include=[np.number])
    if num.shape[1] < 2:
        return {"columns": [], "matrix": []}
    corr = num.corr(numeric_only=True).round(3)
    cols = list(corr.columns)
    matrix = [[_clean(corr.iloc[i, j]) for j in range(len(cols))] for i in range(len(cols))]
    return {"columns": [str(c) for c in cols], "matrix": matrix}


def _quality(overview: Dict[str, Any], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Resumo de QUALIDADE dos dados + score heurístico (0–100)."""
    n = overview.get("n_rows", 0) or 1
    total_cols = max(1, len(columns))
    constant = [c["name"] for c in columns if c["distinct"] <= 1]
    high_null = [c["name"] for c in columns if c["null_pct"] >= 50]
    keys = [
        c["name"] for c in columns
        if c["distinct"] == overview.get("n_rows") and overview.get("n_rows", 0) > 1
    ]
    penalties = (
        overview.get("null_pct", 0) * 0.5
        + (overview.get("n_duplicated", 0) / n * 100) * 0.3
        + (len(constant) / total_cols * 100) * 0.2
    )
    score = max(0.0, round(100 - penalties, 1))
    return {
        "score": score,
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
) -> List[str]:
    out: List[str] = []
    if overview["n_rows"] == 0:
        return ["O resultado não tem linhas para analisar."]

    # 🔭 Negócio primeiro (o que mais interessa).
    if trend:
        prev = trend.get("forecast_next")
        out.append(
            f"📈 Tendência {trend['freq']} de '{trend['measure']}': {trend['direction']} "
            f"({trend['change_pct']:+}%). Previsão para o próximo período: ~{prev}."
        )
    if segments:
        out.append(f"👥 Os dados dividem-se em {segments['k']} segmento(s)/perfil(is) distinto(s).")
    if anomalies and anomalies.get("count", 0) > 0:
        out.append(f"🚨 {anomalies['count']} linha(s) anómala(s) (comportamento fora do normal).")

    if overview["n_duplicated"] > 0:
        out.append(f"Há {overview['n_duplicated']} linha(s) duplicada(s).")
    for c in columns:
        if c["null_pct"] >= 30:
            out.append(f"A coluna '{c['name']}' tem {c['null_pct']}% de valores em falta.")
        if c["distinct"] <= 1:
            out.append(f"A coluna '{c['name']}' é constante (um só valor) — sem informação.")
        if c["distinct"] == overview["n_rows"] and overview["n_rows"] > 1 and c["kind"] != "numeric":
            out.append(f"A coluna '{c['name']}' é única em todas as linhas (candidata a chave).")

        if c["kind"] == "numeric":
            num = c.get("numeric", {})
            o = num.get("outliers", {})
            if o.get("count", 0) > 0 and o.get("pct", 0) >= 2:
                out.append(f"'{c['name']}': {o['count']} outliers ({o['pct']}%) fora de [{o.get('lower')}, {o.get('upper')}].")
            sk = num.get("skew")
            if sk is not None and abs(sk) >= 1:
                lado = "à direita (cauda alta)" if sk > 0 else "à esquerda (cauda baixa)"
                out.append(f"'{c['name']}': distribuição assimétrica {lado} (skew={round(sk, 2)}).")
            if num.get("negatives", 0) > 0 and "id" in c["name"].lower():
                out.append(f"A coluna '{c['name']}' (parece um id) tem valores negativos.")

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
def _tendencia(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Tendência temporal: agrega uma medida por período e mede se sobe/desce,
    com uma previsão simples (regressão linear) para o período seguinte."""
    dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    if not dt_cols:
        return None
    dtc = dt_cols[0]
    num_cols = list(df.select_dtypes(include=[np.number]).columns)
    d = pd.DataFrame({"_t": df[dtc]})
    if num_cols:
        measure = num_cols[0]
        d["_m"] = pd.to_numeric(df[measure], errors="coerce")
        label = str(measure)
    else:
        d["_m"] = 1.0
        label = "contagem"
    d = d.dropna(subset=["_t"])
    if len(d) < 3:
        return None
    span = (d["_t"].max() - d["_t"].min()).days
    freq = "D" if span <= 60 else ("W" if span <= 400 else "MS")
    ts = d.set_index("_t")["_m"].resample(freq).sum().dropna()
    if len(ts) < 3:
        return None
    y = ts.to_numpy(dtype=float)
    x = np.arange(len(y))
    slope, intercept = np.polyfit(x, y, 1)
    change_pct = ((y[-1] - y[0]) / abs(y[0]) * 100) if y[0] else 0.0
    if slope > 0 and abs(change_pct) >= 5:
        direction = "a subir"
    elif slope < 0 and abs(change_pct) >= 5:
        direction = "a descer"
    else:
        direction = "estável"
    forecast = float(slope * len(y) + intercept)
    return {
        "measure": label,
        "freq": {"D": "diária", "W": "semanal", "MS": "mensal"}.get(freq, freq),
        "direction": direction,
        "change_pct": round(float(change_pct), 1),
        "forecast_next": _clean(max(0.0, forecast)),
        "series": [
            {"t": (idx.date().isoformat() if hasattr(idx, "date") else str(idx)), "v": _clean(v)}
            for idx, v in ts.items()
        ],
    }


def _anomalias(df: pd.DataFrame, top: int = 12) -> Dict[str, Any]:
    """Linhas anómalas: score = maior |z-score| entre as colunas numéricas."""
    num = df.select_dtypes(include=[np.number])
    if num.shape[1] == 0 or len(num) < 10:
        return {"count": 0, "rows": []}
    std = num.std(ddof=0).replace(0, np.nan)
    z = (num - num.mean()) / std
    score = z.abs().max(axis=1)
    count = int((score > 3).sum())
    idxs = score[score > 3].sort_values(ascending=False).head(top).index
    rows = []
    for i in idxs:
        rows.append({
            "index": int(i),
            "score": _clean(round(float(score[i]), 2)),
            "row": {str(c): _clean(df.loc[i, c]) for c in df.columns},
        })
    return {"count": count, "rows": rows}


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


def _segmentos(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Segmenta as linhas em grupos (k-means) pelas colunas numéricas — 'perfis'."""
    num = df.select_dtypes(include=[np.number]).dropna()
    if num.shape[1] < 2 or len(num) < 20:
        return None
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
    return {"k": len(segs), "columns": [str(c) for c in num.columns], "segments": segs}


def analisar_stream(rows: List[Dict[str, Any]]) -> Iterator[Tuple[str, Any]]:
    """Gera (stage, data) por etapas — para o WebSocket enviar em tempo real."""
    if not rows:
        yield "overview", {"n_rows": 0, "n_cols": 0}
        yield "insights", ["Sem dados para analisar."]
        yield "done", {"ok": True}
        return

    df = _build_df(rows)
    overview = _overview(df)
    yield "overview", overview

    columns: List[Dict[str, Any]] = []
    for name in df.columns:
        stats = _column_stats(str(name), df[name])
        columns.append(stats)
        yield "column", stats

    corr = _correlations(df)
    yield "correlations", corr

    quality = _quality(overview, columns)
    yield "quality", quality

    # Análises avançadas (negócio) — cada uma pode não se aplicar (None).
    trend = _tendencia(df)
    if trend:
        yield "trend", trend

    anomalies = _anomalias(df)
    if anomalies.get("count", 0) > 0:
        yield "anomalies", anomalies

    segments = _segmentos(df)
    if segments:
        yield "segments", segments

    insights = _insights(overview, columns, corr, trend, segments, anomalies)
    yield "insights", insights

    yield "done", {"ok": True}


def analisar(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Resultado completo de uma vez (para cache Redis / REST)."""
    result: Dict[str, Any] = {
        "overview": {}, "columns": [], "correlations": {}, "quality": {},
        "trend": None, "anomalies": None, "segments": None, "insights": [],
    }
    for stage, data in analisar_stream(rows):
        if stage == "column":
            result["columns"].append(data)
        elif stage in result:
            result[stage] = data
    return result
