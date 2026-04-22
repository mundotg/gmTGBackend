from datetime import datetime
from typing import Any, Dict, List

from app.schemas.query_select_upAndInsert_schema import QueryResultType
from app.ultils.logger import log_message


class QueryReportBuilder:
    DEFAULT_PREVIEW_LIMIT = 2000
    MAX_COL_WIDTH = 15
    MIN_COL_WIDTH = 2

    # 🎨 cores
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

    # ============================================================
    # BUILD
    # ============================================================
    def build(self) -> List[Dict[str, Any]]:
        estrutura = [
            *self._build_header(),
            *self._build_query_info(),
            *self._build_query_statistics(),
            *self._build_preview_table(),
            self._build_footer(),
        ]

        log_message(f"📊 Relatório construído com {len(estrutura)} elementos")
        return estrutura

    # ============================================================
    # HEADER
    # ============================================================
    def _build_header(self):
        timestamp = datetime.now().strftime("%d/%m/%Y às %H:%M")

        return [
            {
                "type": "header",
                "data": {
                    "title": "Relatório de Execução de Query SQL",
                    "subtitle": f"Gerado em {timestamp}",
                    "logo": True,
                    "title_size": 20,
                    "subtitle_size": 12,
                },
            },
            self._spacer(0.3),
            self._line(self.COLOR_PRIMARY, 2),
            self._spacer(0.4),
            {
                "type": "text",
                "data": {
                    "value": "Este relatório apresenta os resultados de uma consulta SQL executada, incluindo parâmetros, colunas e duração.",
                    "align": "justify",
                    "size": 10,
                },
            },
            self._spacer(0.5),
        ]

    # ============================================================
    # QUERY INFO
    # ============================================================
    def _build_query_info(self):
        status_icon = "✅ Sucesso" if self.query_result.success else "❌ Falhou"

        param_str = (
            "\n".join(f"{k}: {v}" for k, v in self.query_result.params.items())
            if self.query_result.params
            else "Nenhum parâmetro informado"
        )

        rows = [
            ["Status", status_icon],
            ["Query", self.query_result.query or "—"],
            ["Duração (ms)", str(self.query_result.duration_ms)],
            ["Total de Resultados", str(self.query_result.totalResults or "—")],
        ]

        return [
            self._title("📋 Detalhes da Query", self.COLOR_PRIMARY),
            self._spacer(0.2),
            {
                "type": "table",
                "data": {
                    "columns": ["Propriedade", "Valor"],
                    "rows": rows,
                    "colWidths": [5, 11],
                },
                "style": {
                    "backgroundColor": self.BG_LIGHT_BLUE
                },
            },
            self._spacer(0.3),
            self._title("🔧 Parâmetros", self.COLOR_PRIMARY),
            {
                "type": "text",
                "data": {
                    "value": param_str,
                    "size": 9,
                    "color": self.COLOR_TEXT_MUTED,
                },
            },
            self._spacer(0.6),
            self._line(),
        ]

    # ============================================================
    # STATISTICS
    # ============================================================
    def _build_query_statistics(self):
        payload = self.query_result.QueryPayload
        if not payload:
            return []

        rows = [
            ["Tabela Base", payload.baseTable or "—"],
            ["Limite", str(payload.limit or "—")],
            ["Offset", str(payload.offset or "—")],
        ]

        return [
            self._spacer(0.4),
            self._title("📊 Estrutura da Query", self.COLOR_SUCCESS),
            self._spacer(0.3),
            {
                "type": "table",
                "data": {
                    "columns": ["Item", "Valor"],
                    "rows": rows,
                    "colWidths": [5, 11],
                },
            },
            self._spacer(0.8),
        ]

    # ============================================================
    # PREVIEW TABLE
    # ============================================================
    def _build_preview_table(self):
        if not self.query_result.preview:
            return [
                {
                    "type": "text",
                    "data": {
                        "value": "Nenhum dado retornado.",
                        "align": "center",
                        "color": self.COLOR_NEUTRAL,
                    },
                }
            ]

        columns = self.query_result.columns[:10]

        rows = [
            [str(record.get(col, "")) for col in columns]
            for record in self.query_result.preview[:self.preview_limit]
        ]

        width = max(self.MIN_COL_WIDTH, self.MAX_COL_WIDTH / max(1, len(columns)))

        return [
            self._title("📄 Prévia dos Resultados", self.COLOR_INFO),
            self._spacer(0.3),
            {
                "type": "table",
                "data": {
                    "columns": columns,
                    "rows": rows,
                    "colWidths": [width] * len(columns),
                },
            },
            self._spacer(0.6),
        ]

    # ============================================================
    # FOOTER
    # ============================================================
    def _build_footer(self):
        return {
            "type": "footer",
            "data": {
                "left": "OrionForgeNexus (oFn)",
                "center": f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
                "right": "Relatório SQL",
            },
        }

    # ============================================================
    # HELPERS (🔥 ESSENCIAL)
    # ============================================================
    def _spacer(self, height=0.5):
        return {
            "type": "spacer",
            "data": {"height": height},
        }

    def _line(self, color="#cbd5e1", thickness=1):
        return {
            "type": "line",
            "data": {
                "color": color,
                "thickness": thickness,
            },
        }

    def _title(self, text, color):
        return {
            "type": "text",
            "data": {
                "value": text,
                "bold": True,
                "size": 13,
                "color": color,
            },
        }