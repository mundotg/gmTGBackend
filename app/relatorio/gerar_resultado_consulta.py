"""
Gerador de Relatórios de Resultados de Query SQL
OrionForgeNexus (oFn)

Gera relatórios PDF baseados em resultados de consultas SQL executadas no sistema.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.schemas.query_select_upAndInsert_schema import QueryResultType
from app.ultils.logger import log_message


class QueryReportBuilder:
    """Constrói a estrutura declarativa de relatório para resultados de query SQL."""

    DEFAULT_PREVIEW_LIMIT = 2000
    MAX_COL_WIDTH = 15
    MIN_COL_WIDTH = 2
    
    # Cores
    COLOR_PRIMARY = "#1e3a8a"
    COLOR_SUCCESS = "#166534"
    COLOR_INFO = "#1d4ed8"
    COLOR_SECONDARY = "#0ea5e9"
    COLOR_GREEN = "#059669"
    COLOR_TEXT_MUTED = "#475569"
    COLOR_NEUTRAL = "#64748b"
    BG_LIGHT_BLUE = "#f0f9ff"
    BG_LIGHT_GREEN = "#ecfdf5"
    BG_VERY_LIGHT_BLUE = "#eff6ff"
    BG_WHITE = "#ffffff"

    def __init__(self, query_result: QueryResultType, preview_limit: int = DEFAULT_PREVIEW_LIMIT):
        self.query_result = query_result
        self.preview_limit = max(1, preview_limit)

    def build(self) -> List[Dict[str, Any]]:
        """Monta a estrutura completa do relatório."""
        estrutura = [
            *self._build_header(),
            *self._build_query_info(),
            *self._build_query_statistics(),
            *self._build_preview_table(),
            self._build_footer()
        ]
        log_message(f"📊 Relatório construído com {len(estrutura)} elementos")
        return estrutura

    def _build_header(self) -> List[Dict[str, Any]]:
        timestamp = datetime.now().strftime("%d/%m/%Y às %H:%M")
        return [
            {
                "header": {
                    "title": "Relatório de Execução de Query SQL",
                    "subtitle": f"Gerado em {timestamp}",
                    "logo": True,
                    "logo_width": 2.5,
                    "logo_height": 2.5,
                }
            },
            {"spacer": {"height": 0.3}},
            {"line": {"color": self.COLOR_PRIMARY, "width": 2}},
            {"spacer": {"height": 0.4}},
            {
                "text": {
                    "value": "Este relatório apresenta os resultados de uma consulta SQL executada, "
                             "incluindo parâmetros, colunas, duração e prévia dos dados retornados.",
                    "align": "justify",
                    "size": 10,
                }
            },
            {"spacer": {"height": 0.5}},
        ]

    def _build_query_info(self) -> List[Dict[str, Any]]:
        status_icon = "✅ Sucesso" if self.query_result.success else "❌ Falhou"
        param_str = "\n".join(f"{k}: {v}" for k, v in self.query_result.params.items()) if self.query_result.params else "Nenhum parâmetro informado"

        rows = [
            ["Status", status_icon],
            ["Query", self.query_result.query or "—"],
            ["Duração (ms)", str(self.query_result.duration_ms)],
            ["Total de Resultados", str(self.query_result.totalResults or "—")],
        ]

        return [
            self._create_section_title("📋 Detalhes da Query", self.COLOR_PRIMARY),
            {"spacer": {"height": 0.2}},
            {
                "table": {
                    "columns": ["Propriedade", "Valor"],
                    "rows": rows,
                    "header_color": self.COLOR_SECONDARY,
                    "row_colors": [self.BG_WHITE, self.BG_LIGHT_BLUE],
                    "col_widths": [5, 11],
                }
            },
            {"spacer": {"height": 0.3}},
            {"text": {"value": "🔧 Parâmetros", "bold": True, "size": 12}},
            {"text": {"value": param_str, "size": 9, "color": self.COLOR_TEXT_MUTED, "align": "left"}},
            {"spacer": {"height": 0.6}},
            {"line": {}},
        ]

    def _build_query_statistics(self) -> List[Dict[str, Any]]:
        payload = self.query_result.QueryPayload
        if not payload:
            return []

        join_text = "\n".join(f"{table} ⟶ {info.typeJoin}" for table, info in (payload.joins or {}).items()) or "Nenhum JOIN aplicado"
        alias_text = ", ".join(f"{alias} → {table}" for alias, table in (payload.aliaisTables or {}).items()) or "—"
        distinct_val = "Sim" if payload.distinct and payload.distinct.useDistinct else "Não"

        rows = [
            ["Tabela Base", payload.baseTable or "—"],
            ["Aliases", alias_text],
            ["JOINs", join_text],
            ["DISTINCT", distinct_val],
            ["Limite", str(payload.limit or "—")],
            ["Offset", str(payload.offset or "—")],
        ]

        return [
            {"spacer": {"height": 0.4}},
            self._create_section_title("📊 Estrutura da Query", self.COLOR_SUCCESS),
            {"spacer": {"height": 0.3}},
            {
                "table": {
                    "columns": ["Item", "Valor"],
                    "rows": rows,
                    "col_widths": [5, 11],
                    "header_color": self.COLOR_GREEN,
                    "row_colors": [self.BG_WHITE, self.BG_LIGHT_GREEN],
                }
            },
            {"spacer": {"height": 0.8}},
        ]

    def _build_preview_table(self) -> List[Dict[str, Any]]:
        if not self.query_result.preview:
            return [{"text": {"value": "Nenhum dado retornado pela query.", "align": "center", "color": self.COLOR_NEUTRAL}}]

        MAX_COLS = 10
        columns = self.query_result.columns
        truncated = len(columns) > MAX_COLS
        
        # 🚀 ALTERAÇÃO AQUI: Pega sempre o último elemento após dividir por '.'
        # Ex: "main.bank_full.age" -> ["main", "bank_full", "age"][-1] -> "age"
        visible_columns = [c.split('.')[-1] for c in columns[:MAX_COLS]]
        
        if truncated:
            visible_columns.append("...")

        # Monta linhas da prévia (Nota: usamos a lista original `columns` para ler do dicionário)
        rows = [
            [str(record.get(col, "")) for col in columns[:MAX_COLS]] + (["..."] if truncated else [])
            for record in self.query_result.preview[:self.preview_limit]
        ]

        width = max(self.MIN_COL_WIDTH, self.MAX_COL_WIDTH / max(1, len(visible_columns)))
        
        estrutura = [
            self._create_section_title("📄 Prévia dos Resultados", self.COLOR_INFO),
            {"spacer": {"height": 0.3}},
        ]

        if truncated:
            estrutura.extend([
                {"text": {"value": f"⚠️ Exibindo apenas {MAX_COLS} de {len(columns)} colunas. Consulte o Excel para o conjunto completo.", "align": "left", "color": self.COLOR_NEUTRAL, "size": 9}},
                {"spacer": {"height": 0.2}}
            ])

        estrutura.extend([
            {
                "table": {
                    "columns": visible_columns,
                    "rows": rows,
                    "col_widths": [width] * len(visible_columns),
                    "header_color": self.COLOR_INFO,
                    "row_colors": [self.BG_WHITE, self.BG_VERY_LIGHT_BLUE],
                }
            },
            {"spacer": {"height": 0.6}}
        ])
        
        return estrutura

    def _build_footer(self) -> Dict[str, Any]:
        return {
            "footer": {
                "left": "OrionForgeNexus (oFn)",
                "center": f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
                "right": "Relatório de Query SQL",
            }
        }

    def _create_section_title(self, text: str, color: str) -> Dict[str, Any]:
        return {"text": {"value": text, "bold": True, "size": 13, "color": color}}