"""
Sistema de Teste para Geração de Relatórios de Metadados de Banco de Dados
OrionForgeNexus (oFn)

Gera dados simulados de estruturas de tabelas e produz relatórios PDF profissionais.

Dependências:
pip install faker reportlab openpyxl
"""

from datetime import datetime
import json
from app.schemas.dbstructure_schema import MetadataTableResponse
from app.ultils.logger import log_message


class ReportStructureBuilder:
    """Constrói estrutura declarativa para geração de PDF"""

    def __init__(self, metadata_list, incluir_estatisticas=True):
        self.metadata_list = metadata_list
        self.incluir_estatisticas = incluir_estatisticas

    def build(self):
        """Constrói estrutura completa do relatório"""
        estrutura = []

        estrutura.extend(self._build_header())

        if self.incluir_estatisticas:
            estrutura.extend(self._build_statistics())

        estrutura.extend(self._build_tables_detail())

        estrutura.append(self._build_footer())

        return estrutura

    def _build_header(self, num_table=None, dataGerado=None):
        """Cabeçalho principal"""
        return [
            {
                "header": {
                    "title": "Relatório de Estrutura de Banco de Dados",
                    "subtitle": f"Análise de {num_table or len(self.metadata_list)} tabelas | Gerado em {dataGerado or datetime.now().strftime('%d/%m/%Y')}",
                    "logo": True,
                    "logo_width": 2.5,
                    "logo_height": 2.5,
                }
            },
            {"spacer": {"height": 0.3}},
            {"line": {"color": "#1e3a8a", "width": 2}},
            {"spacer": {"height": 0.5}},
            {
                "text": {
                    "value": "Este documento apresenta os metadados extraídos automaticamente do banco de dados, incluindo tipos de dados, relacionamentos, índices e estatísticas de cada tabela.",
                    "align": "justify",
                    "size": 10,
                }
            },
            {"spacer": {"height": 0.5}},
        ]

    def _build_statistics(self):
        """Estatísticas gerais"""
        total_colunas = sum(meta["total_colunas"] for meta in self.metadata_list)
        total_fks = sum(
            sum(1 for c in meta["colunas"] if c.get("is_foreign_key"))
            for meta in self.metadata_list
        )
        total_indices = sum(len(meta.get("indices", [])) for meta in self.metadata_list)
        schemas = list(set(meta["schema_name"] for meta in self.metadata_list))

        return [
            {
                "text": {
                    "value": "📊 Estatísticas Gerais",
                    "bold": True,
                    "size": 14,
                    "color": "#1e3a8a",
                }
            },
            {"spacer": {"height": 0.3}},
            {
                "table": {
                    "columns": ["Métrica", "Valor"],
                    "rows": [
                        ["Total de Tabelas", len(self.metadata_list)],
                        ["Total de Colunas", total_colunas],
                        ["Total de Foreign Keys", total_fks],
                        ["Total de Índices", total_indices],
                        ["Schemas Encontrados", ", ".join(schemas)],
                        ["Média de Colunas/Tabela", f"{total_colunas / len(self.metadata_list):.1f}"],
                    ],
                    "col_widths": [8, 8],
                    "header_color": "#059669",
                    "row_colors": ["#ffffff", "#ecfdf5"],
                }
            },
            {"spacer": {"height": 0.8}},
            {"line": {}},
            {"spacer": {"height": 0.5}},
        ]

    def _build_tables_detail(self):
        """Detalhamento das tabelas"""
        estrutura = []
        for idx, meta in enumerate(self.metadata_list, 1):
            estrutura.append(
                {
                    "text": {
                        "value": f"{idx}. Tabela: {meta['schema_name']}.{meta['table_name']}",
                        "bold": True,
                        "size": 13,
                        "color": "#1e3a8a",
                    }
                }
            )

            num_registros = meta.get("num_registros", "N/A")
            if isinstance(num_registros, (int, float)):
                num_registros = f"{num_registros:,}"

            estrutura.append(
                {
                    "text": {
                        "value": f"Colunas: {meta['total_colunas']} | Tamanho: {meta.get('tamanho_estimado', 'N/A')} | Registros: {num_registros}",
                        "size": 9,
                        "color": "#64748b",
                    }
                }
            )

            estrutura.append({"spacer": {"height": 0.3}})

            columns = ["Campo", "Tipo", "Nulo", "PK", "FK", "Único", "Default", "Comentário"]
            rows = []

            for c in meta["colunas"]:
                rows.append(
                    [
                        c["nome"],
                        c["tipo"][:20],
                        "✓" if c.get("is_nullable") else "✗",
                        "✓" if c.get("is_primary_key") else "",
                        c["referenced_table"][:15] if c.get("is_foreign_key") and c.get("referenced_table") else "",
                        "✓" if c.get("is_unique") else "",
                        str(c.get("default", ""))[:15],
                        c.get("comentario", "")[:40],
                    ]
                )

            estrutura.append(
                {
                    "table": {
                        "columns": columns,
                        "rows": rows,
                        "col_widths": [2.5, 2, 1, 0.8, 2, 1, 2, 5],
                        "header_color": "#2563eb",
                        "row_colors": ["#ffffff", "#f8fafc"],
                    }
                }
            )

            if meta.get("indices"):
                estrutura.append({"spacer": {"height": 0.2}})
                estrutura.append(
                    {
                        "text": {
                            "value": f"Índices: {', '.join(meta['indices'][:3])}",
                            "size": 8,
                            "color": "#64748b",
                        }
                    }
                )

            estrutura.append({"spacer": {"height": 0.5}})
            if idx % 2 == 0 and idx < len(self.metadata_list):
                estrutura.append({"pagebreak": {}})

        return estrutura

    def _build_footer(self, data=None):
        """Rodapé"""
        return {
            "footer": {
                "left": "OrionForgeNexus (oFn)",
                "center": f"Gerado automaticamente em {datetime.now().strftime('%d/%m/%Y às %H:%M')}",
                "right": "Documento Interno - Confidencial",
            }
        }


def executar_request(metadata: list[MetadataTableResponse], num_tabelas=5, logo_path=None):
    """Executa geração completa"""
    tabelas = metadata
    log_message("\n🏗️  Construindo estrutura do relatório...")
    builder = ReportStructureBuilder(tabelas, incluir_estatisticas=True)
    estrutura_pdf = builder.build()
    log_message(f"✅ Estrutura criada com {len(estrutura_pdf)} elementos")

    log_message("\n📄 Gerando relatório PDF...")
    try:
        from .gerarpdf import GenericReportGenerator

        report_generator = GenericReportGenerator(
            logo_path=logo_path,
            company_name="OrionForgeNexus",
            company_abbr="oFn"
        )

        filename = f"relatorio_metadados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        report_generator.generate_report(filename, estrutura_pdf)

        log_message(f"\n✅ TESTE CONCLUÍDO COM SUCESSO!")
        log_message(f"📁 Arquivo gerado: {filename}")

    except ImportError:
        log_message("\n⚠️  Módulo 'gerarpdf' não encontrado.", "error")
        json_filename = f"estrutura_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(estrutura_pdf, f, indent=2, ensure_ascii=False)
        log_message(f"📁 Estrutura salva em: {json_filename}", "error")

    return tabelas, estrutura_pdf
