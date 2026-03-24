"""
Sistema de Teste para Geração de Relatórios de Metadados de Banco de Dados
OrionForgeNexus (oFn)
"""

from datetime import datetime
import json
import uuid
from app.schemas.dbstructure_schema import MetadataTableResponse
from app.ultils.logger import log_message


# =============================================================================
# 🔥 HELPER (PADRÃO DO TEU FRONTEND)
# =============================================================================
def create_section(type_, data=None, style=None):
    return {
        "id": str(uuid.uuid4()),
        "type": type_,
        "data": data or {},
        "style": style or {},
    }


# =============================================================================
# 🧩 BUILDER
# =============================================================================
class ReportStructureBuilder:
    def __init__(self, metadata_list, incluir_estatisticas=True):
        self.metadata_list = metadata_list
        self.incluir_estatisticas = incluir_estatisticas

    # -------------------------------------------------------------------------
    def build(self):
        estrutura = []

        estrutura.extend(self._build_header())

        if self.incluir_estatisticas:
            estrutura.extend(self._build_statistics())

        estrutura.extend(self._build_tables_detail())

        estrutura.append(self._build_footer())

        return estrutura

    # -------------------------------------------------------------------------
    def _build_header(self):
        return [
            create_section("header", {
                "title": "Relatório de Estrutura de Banco de Dados",
                "subtitle": f"Análise de {len(self.metadata_list)} tabelas | Gerado em {datetime.now().strftime('%d/%m/%Y')}",
                "logo": True,
                "title_size": 20,
                "subtitle_size": 12,
            }),

            create_section("spacer", {"height": 0.3}),

            create_section("line", {
                "color": "#1e3a8a",
                "thickness": 2,
            }),

            create_section("spacer", {"height": 0.5}),

            create_section("text", {
                "value": "Este documento apresenta os metadados extraídos automaticamente do banco de dados.",
                "align": "justify",
                "size": 10,
            }),

            create_section("spacer", {"height": 0.5}),
        ]

    # -------------------------------------------------------------------------
    def _build_statistics(self):
        total_colunas = sum(meta["total_colunas"] for meta in self.metadata_list)

        total_fks = sum(
            sum(1 for c in meta["colunas"] if c.get("is_foreign_key"))
            for meta in self.metadata_list
        )

        total_indices = sum(len(meta.get("indices", [])) for meta in self.metadata_list)

        schemas = list(set(meta["schema_name"] for meta in self.metadata_list))

        return [
            create_section("text", {
                "value": "📊 Estatísticas Gerais",
                "bold": True,
                "size": 14,
                "color": "#1e3a8a",
            }),

            create_section("spacer", {"height": 0.3}),

            create_section("table", {
                "columns": ["Métrica", "Valor"],
                "rows": [
                    ["Total de Tabelas", len(self.metadata_list)],
                    ["Total de Colunas", total_colunas],
                    ["Total de Foreign Keys", total_fks],
                    ["Total de Índices", total_indices],
                    ["Schemas", ", ".join(schemas)],
                    ["Média Colunas/Tabela", f"{total_colunas / len(self.metadata_list):.1f}"],
                ],
                "colWidths": [8, 8],
                "header": True,
                "border": True,
            }),

            create_section("spacer", {"height": 0.8}),
            create_section("line", {}),
            create_section("spacer", {"height": 0.5}),
        ]

    # -------------------------------------------------------------------------
    def _build_tables_detail(self):
        estrutura = []

        for idx, meta in enumerate(self.metadata_list, 1):

            estrutura.append(
                create_section("text", {
                    "value": f"{idx}. {meta['schema_name']}.{meta['table_name']}",
                    "bold": True,
                    "size": 13,
                    "color": "#1e3a8a",
                })
            )

            num_registros = meta.get("num_registros", "N/A")
            if isinstance(num_registros, (int, float)):
                num_registros = f"{num_registros:,}"

            estrutura.append(
                create_section("text", {
                    "value": f"Colunas: {meta['total_colunas']} | Registros: {num_registros}",
                    "size": 9,
                    "color": "#64748b",
                })
            )

            estrutura.append(create_section("spacer", {"height": 0.3}))

            columns = ["Campo", "Tipo", "Nulo", "PK", "FK", "Único", "Default"]
            rows = []

            for c in meta["colunas"]:
                rows.append([
                    c["nome"],
                    c["tipo"][:20],
                    "✓" if c.get("is_nullable") else "✗",
                    "✓" if c.get("is_primary_key") else "",
                    c.get("referenced_table", "")[:15],
                    "✓" if c.get("is_unique") else "",
                    str(c.get("default", ""))[:15],
                ])

            estrutura.append(
                create_section("table", {
                    "columns": columns,
                    "rows": rows,
                    "colWidths": [3, 2, 1, 1, 2, 1, 2],
                    "border": True,
                })
            )

            if meta.get("indices"):
                estrutura.append(create_section("spacer", {"height": 0.2}))
                estrutura.append(
                    create_section("text", {
                        "value": f"Índices: {', '.join(meta['indices'][:3])}",
                        "size": 8,
                        "color": "#64748b",
                    })
                )

            estrutura.append(create_section("spacer", {"height": 0.5}))

            if idx % 2 == 0 and idx < len(self.metadata_list):
                estrutura.append(create_section("pagebreak"))

        return estrutura

    # -------------------------------------------------------------------------
    def _build_footer(self):
        return create_section("footer", {
            "left": "OrionForgeNexus (oFn)",
            "center": f"Gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M')}",
            "right": "Documento Confidencial",
        })


# =============================================================================
# 🚀 EXECUÇÃO
# =============================================================================
def executar_request(metadata: list[MetadataTableResponse], logo_path=None):
    log_message("\n🏗️ Construindo estrutura...")

    builder = ReportStructureBuilder(metadata, True)
    estrutura_pdf = builder.build()

    log_message(f"✅ Estrutura criada com {len(estrutura_pdf)} seções")

    try:
        from .gerarpdf import GenericReportGenerator

        generator = GenericReportGenerator(
            logo_path=logo_path,
            company_name="OrionForgeNexus",
            company_abbr="oFn"
        )

        filename = f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        generator.generate_report(filename, estrutura_pdf)

        log_message(f"✅ PDF gerado: {filename}")

    except ImportError:
        log_message("⚠️ gerarpdf não encontrado", "error")

        with open("debug.json", "w", encoding="utf-8") as f:
            json.dump(estrutura_pdf, f, indent=2, ensure_ascii=False)

    return estrutura_pdf