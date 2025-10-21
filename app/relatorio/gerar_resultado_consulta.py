"""
Gerador de Relatórios de Resultados de Query SQL
OrionForgeNexus (oFn)

Gera relatórios PDF baseados em resultados de consultas SQL executadas no sistema.
Suporta exibição de parâmetros, estatísticas, estrutura da query e prévia de resultados.

Dependências:
    pip install reportlab openpyxl faker
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import json

from app.schemas.query_select_upAndInsert_schema import QueryResultType
from app.ultils.logger import log_message


class QueryReportBuilder:
    """
    Constrói estrutura declarativa de relatório para resultados de query SQL.
    
    Attributes:
        query_result: Dicionário com resultados da execução da query
        preview_limit: Número máximo de linhas a exibir na prévia
    """

    # Constantes de configuração
    DEFAULT_PREVIEW_LIMIT = 20
    MAX_COL_WIDTH = 15
    MIN_COL_WIDTH = 2
    
    # Paleta de cores
    COLOR_PRIMARY = "#1e3a8a"
    COLOR_SUCCESS = "#166534"
    COLOR_INFO = "#1d4ed8"
    COLOR_SECONDARY = "#0ea5e9"
    COLOR_GREEN = "#059669"
    COLOR_TEXT_MUTED = "#475569"
    COLOR_NEUTRAL = "#64748b"
    
    # Cores de fundo
    BG_LIGHT_BLUE = "#f0f9ff"
    BG_LIGHT_GREEN = "#ecfdf5"
    BG_VERY_LIGHT_BLUE = "#eff6ff"
    BG_WHITE = "#ffffff"

    def __init__(self, query_result: QueryResultType, preview_limit: int = DEFAULT_PREVIEW_LIMIT):
        """
        Inicializa o construtor de relatórios.
        
        Args:
            query_result: Resultado da execução da query SQL
            preview_limit: Limite de linhas para prévia (padrão: 20)
        """
        self.query_result = query_result or {}
        self.preview_limit = max(1, preview_limit)

    def build(self) -> List[Dict[str, Any]]:
        """
        Monta estrutura completa do relatório.
        
        Returns:
            Lista de elementos que compõem o relatório
        """
        estrutura = []
        estrutura.extend(self._build_header())
        estrutura.extend(self._build_query_info())
        estrutura.extend(self._build_query_statistics())
        estrutura.extend(self._build_preview_table())
        estrutura.append(self._build_footer())
        
        log_message(f"📊 Relatório construído com {len(estrutura)} elementos")
        return estrutura

    # ==============================================================
    # 🔹 SEÇÕES DO RELATÓRIO
    # ==============================================================

    def _build_header(self) -> List[Dict[str, Any]]:
        """
        Constrói o cabeçalho do relatório.
        
        Returns:
            Lista de elementos do cabeçalho
        """
        timestamp = self._format_datetime(datetime.now())
        
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
                    "value": (
                        "Este relatório apresenta os resultados de uma consulta SQL executada, "
                        "incluindo parâmetros, colunas, duração e prévia dos dados retornados."
                    ),
                    "align": "justify",
                    "size": 10,
                }
            },
            {"spacer": {"height": 0.5}},
        ]

    def _build_query_info(self) -> List[Dict[str, Any]]:
        """
        Constrói a seção de informações básicas da query.
        
        Returns:
            Lista de elementos da seção de informações
        """
        status_icon = "✅ Sucesso" if self.query_result.get("success") else "❌ Falhou"
        params = self.query_result.get("params", {})
        param_str = self._format_parameters(params)

        query_info_rows = [
            ["Status", status_icon],
            ["Query", self._safe_get("query", "—")],
            ["Duração (ms)", self._safe_get("duration_ms", "—")],
            ["Total de Resultados", self._safe_get("totalResults", "—")],
        ]

        return [
            self._create_section_title("📋 Detalhes da Query", self.COLOR_PRIMARY),
            {"spacer": {"height": 0.2}},
            {
                "table": {
                    "columns": ["Propriedade", "Valor"],
                    "rows": query_info_rows,
                    "header_color": self.COLOR_SECONDARY,
                    "row_colors": [self.BG_WHITE, self.BG_LIGHT_BLUE],
                    "col_widths": [5, 11],
                }
            },
            {"spacer": {"height": 0.3}},
            {"text": {"value": "🔧 Parâmetros", "bold": True, "size": 12}},
            {
                "text": {
                    "value": param_str,
                    "size": 9,
                    "color": self.COLOR_TEXT_MUTED,
                    "align": "left"
                }
            },
            {"spacer": {"height": 0.6}},
            {"line": {}},
        ]

    def _build_query_statistics(self) -> List[Dict[str, Any]]:
        """
        Constrói a seção de estatísticas e estrutura da query.
        
        Returns:
            Lista de elementos da seção de estatísticas
        """
        query_payload = self.query_result.get("QueryPayload", {}) or {}
        
        joins = query_payload.get("joins", {})
        aliases = query_payload.get("aliaisTables", {})
        base_table = query_payload.get("baseTable", "—")
        
        join_text = self._format_joins(joins)
        alias_text = self._format_aliases(aliases)

        stats_rows = [
            ["Tabela Base", base_table],
            ["Aliases", alias_text],
            ["JOINs", join_text],
            ["DISTINCT", str(query_payload.get("distinct", "—"))],
            ["Limite", str(query_payload.get("limit", "—"))],
            ["Offset", str(query_payload.get("offset", "—"))],
        ]

        return [
            {"spacer": {"height": 0.4}},
            self._create_section_title("📊 Estrutura da Query", self.COLOR_SUCCESS),
            {"spacer": {"height": 0.3}},
            {
                "table": {
                    "columns": ["Item", "Valor"],
                    "rows": stats_rows,
                    "col_widths": [5, 11],
                    "header_color": self.COLOR_GREEN,
                    "row_colors": [self.BG_WHITE, self.BG_LIGHT_GREEN],
                }
            },
            {"spacer": {"height": 0.8}},
        ]

    def _build_preview_table(self) -> List[Dict[str, Any]]:
        """
        Constrói a tabela de prévia dos resultados.
        - Permite até 9 colunas; se houver mais, adiciona "..." ao final.
        - Detecta e trata colunas no formato tableName.colunaName.
        """
        preview = self.query_result.get("preview", []) or []
        columns = self.query_result.get("columns", []) or []

        if not preview:
            return [
                {
                    "text": {
                        "value": "Nenhum dado retornado pela query.",
                        "align": "center",
                        "color": self.COLOR_NEUTRAL
                    }
                }
            ]

        # 🔹 Detecta se há colunas com formato "tabela.coluna"
        formatted_columns = []
        for col in columns:
            if "." in col:
                table, column = col.split(".", 1)
                formatted_columns.append(f"{column} ({table})")  # ex: "nome (clientes)"
            else:
                formatted_columns.append(col)

        # 🔹 Limita colunas a 9 e adiciona indicador de truncamento
        MAX_COLS = 10
        truncated = len(formatted_columns) > MAX_COLS
        visible_columns = formatted_columns[:MAX_COLS]

        if truncated:
            visible_columns.append("...")

        # 🔹 Limita número de linhas (prévia)
        preview_data = preview[:self.preview_limit]

        # 🔹 Ajusta as linhas conforme colunas visíveis
        rows = []
        for record in preview_data:
            row = [str(record.get(col, "")) for col in columns[:MAX_COLS]]
            if truncated:
                row.append("...")
            rows.append(row)

        col_widths = self._calculate_column_widths(visible_columns)

        # 🔹 Adiciona aviso de truncamento
        aviso_texto = ""
        if truncated:
            aviso_texto = (
                f"⚠️ Exibindo apenas {MAX_COLS} de {len(columns)} colunas. "
                "Consulte a versão CSV para o conjunto completo."
            )

        estrutura = [
            self._create_section_title("📄 Prévia dos Resultados", self.COLOR_INFO),
            {"spacer": {"height": 0.3}},
        ]

        if aviso_texto:
            estrutura.append({
                "text": {
                    "value": aviso_texto,
                    "align": "left",
                    "color": self.COLOR_NEUTRAL,
                    "size": 9,
                }
            })
            estrutura.append({"spacer": {"height": 0.2}})

        estrutura.append({
            "table": {
                "columns": visible_columns,
                "rows": rows,
                "col_widths": col_widths,
                "header_color": self.COLOR_INFO,
                "row_colors": [self.BG_WHITE, self.BG_VERY_LIGHT_BLUE],
            }
        })
        estrutura.append({"spacer": {"height": 0.6}})

        return estrutura


    def _build_footer(self) -> Dict[str, Any]:
        """
        Constrói o rodapé do relatório.
        
        Returns:
            Dicionário com configuração do rodapé
        """
        timestamp = self._format_datetime(datetime.now(), include_time=True)
        
        return {
            "footer": {
                "left": "OrionForgeNexus (oFn)",
                "center": f"Gerado automaticamente em {timestamp}",
                "right": "Relatório de Query SQL",
            }
        }

    # ==============================================================
    # 🔧 MÉTODOS AUXILIARES
    # ==============================================================

    def _safe_get(self, key: str, default: str = "—") -> str:
        """
        Obtém valor do resultado de forma segura.
        
        Args:
            key: Chave a buscar
            default: Valor padrão se não encontrado
            
        Returns:
            Valor encontrado ou padrão
        """
        value = self.query_result.get(key, default)
        return str(value) if value is not None else default

    def _format_parameters(self, params: Dict[str, Any]) -> str:
        """
        Formata dicionário de parâmetros para exibição.
        
        Args:
            params: Dicionário de parâmetros
            
        Returns:
            String formatada dos parâmetros
        """
        if not params:
            return "Nenhum parâmetro informado"
        
        return "\n".join([f"{k}: {v}" for k, v in params.items()])

    def _format_joins(self, joins: Dict[str, Any]) -> str:
        """
        Formata informações de JOINs.
        
        Args:
            joins: Dicionário de JOINs
            
        Returns:
            String formatada dos JOINs
        """
        if not joins:
            return "Nenhum JOIN aplicado"
        
        return "\n".join([
            f"{table} ⟶ {info.get('on', '—')}"
            for table, info in joins.items()
        ])

    def _format_aliases(self, aliases: Dict[str, str]) -> str:
        """
        Formata aliases de tabelas.
        
        Args:
            aliases: Dicionário de aliases
            
        Returns:
            String formatada dos aliases
        """
        if not aliases:
            return "—"
        
        return ", ".join([f"{alias} → {table}" for alias, table in aliases.items()])

    def _format_preview_rows(self, preview: List[Dict[str, Any]], columns: List[str]) -> List[List[str]]:
        """
        Formata linhas da prévia de dados.
        
        Args:
            preview: Lista de registros
            columns: Lista de colunas
            
        Returns:
            Lista de listas com valores formatados
        """
        rows = []
        for record in preview:
            row = [str(record.get(col, "")) for col in columns]
            rows.append(row)
        return rows

    def _calculate_column_widths(self, columns: List[str]) -> List[float]:
        """
        Calcula larguras das colunas de forma equilibrada.
        
        Args:
            columns: Lista de colunas
            
        Returns:
            Lista com larguras calculadas
        """
        if not columns:
            return []
        
        num_cols = len(columns)
        width = max(self.MIN_COL_WIDTH, self.MAX_COL_WIDTH / max(1, num_cols))
        return [width] * num_cols

    def _create_section_title(self, text: str, color: str) -> Dict[str, Any]:
        """
        Cria título de seção padronizado.
        
        Args:
            text: Texto do título
            color: Cor do título
            
        Returns:
            Dicionário com configuração do título
        """
        return {
            "text": {
                "value": text,
                "bold": True,
                "size": 13,
                "color": color
            }
        }

    @staticmethod
    def _format_datetime(dt: datetime, include_time: bool = True) -> str:
        """
        Formata data/hora de forma consistente.
        
        Args:
            dt: Objeto datetime
            include_time: Se True, inclui horário
            
        Returns:
            String formatada
        """
        if include_time:
            return dt.strftime("%d/%m/%Y às %H:%M")
        return dt.strftime("%d/%m/%Y %H:%M:%S")


# ==============================================================
# 🚀 FUNÇÃO PRINCIPAL DE GERAÇÃO
# ==============================================================

def gerar_relatorio_query(
    query_result: QueryResultType,
    logo_path: Optional[str] = None,
    preview_limit: int = QueryReportBuilder.DEFAULT_PREVIEW_LIMIT
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Executa a geração de relatório PDF para resultado de query.
    
    Args:
        request: Objeto de requisição
        query_result: Dicionário com resultado da query
        logo_path: Caminho para o logo (opcional)
        preview_limit: Limite de linhas na prévia
        
    Returns:
        Tupla (nome_arquivo, estrutura_pdf)
        
    Raises:
        ValueError: Se query_result estiver vazio
    """
    if not query_result:
        raise ValueError("query_result não pode estar vazio")
    
    log_message("🏗️ Construindo relatório de query...")
    
    builder = QueryReportBuilder(query_result, preview_limit=preview_limit)
    estrutura_pdf = builder.build()
    
    log_message(f"✅ Estrutura declarativa criada ({len(estrutura_pdf)} elementos)")

    try:
        from .gerarpdf import GenericReportGenerator
        
        generator = GenericReportGenerator(
            logo_path=logo_path,
            company_name="OrionForgeNexus",
            company_abbr="oFn"
        )

        filename = _generate_filename("relatorio_query", "pdf")
        generator.generate_report(filename, estrutura_pdf)
        
        log_message(f"✅ PDF gerado com sucesso: {filename}")
        return filename, estrutura_pdf

    except ImportError as e:
        log_message(
            f"⚠️ Módulo gerarpdf não encontrado ({e}) — salvando em JSON",
            "error"
        )
        return _save_debug_json(estrutura_pdf)


def _generate_filename(prefix: str, extension: str) -> str:
    """
    Gera nome de arquivo com timestamp.
    
    Args:
        prefix: Prefixo do arquivo
        extension: Extensão (sem ponto)
        
    Returns:
        Nome do arquivo formatado
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.{extension}"


def _save_debug_json(estrutura: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Salva estrutura em JSON para debug.
    
    Args:
        estrutura: Estrutura do relatório
        
    Returns:
        Tupla (nome_arquivo_json, estrutura)
    """
    json_filename = _generate_filename("query_report_debug", "json")
    
    try:
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(estrutura, f, indent=2, ensure_ascii=False)
        
        log_message(f"💾 Estrutura salva em JSON: {json_filename}")
        return json_filename, estrutura
        
    except IOError as e:
        log_message(f"❌ Erro ao salvar JSON: {e}", "error")
        raise