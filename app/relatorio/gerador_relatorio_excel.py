"""
Gerador de Relatórios Excel
OrionForgeNexus (oFn)

Gera relatórios Excel para estruturas de banco de dados e resultados de queries.

Dependências:
    pip install openpyxl
"""

from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import json

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.drawing.image import Image as XLImage
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils import get_column_letter

from app.ultils.logger import log_message


# ============================================================
# 🎨 CONSTANTES DE ESTILO
# ============================================================

class ExcelStyles:
    """Definições de estilos para Excel"""
    
    # Cores
    COLOR_PRIMARY = "1e3a8a"
    COLOR_SECONDARY = "0ea5e9"
    COLOR_SUCCESS = "059669"
    COLOR_INFO = "1d4ed8"
    COLOR_TEXT_PRIMARY = "475569"
    COLOR_TEXT_SECONDARY = "64748b"
    COLOR_WHITE = "FFFFFF"
    COLOR_BORDER = "cbd5e1"
    COLOR_ALTERNATE_ROW = "f8fafc"
    COLOR_LIGHT_BLUE = "f0f9ff"
    COLOR_LIGHT_GREEN = "ecfdf5"
    
    # Fontes
    FONT_TITLE = Font(bold=True, size=16, color=COLOR_PRIMARY)
    FONT_SUBTITLE = Font(bold=True, size=13, color=COLOR_TEXT_PRIMARY)
    FONT_INFO = Font(size=10, color=COLOR_TEXT_SECONDARY)
    FONT_HEADER = Font(bold=True, color=COLOR_WHITE, size=11)
    FONT_HEADER_SECONDARY = Font(bold=True, color=COLOR_WHITE, size=10)
    FONT_DATA = Font(size=10, color=COLOR_TEXT_PRIMARY)
    FONT_SECTION = Font(bold=True, size=12, color=COLOR_PRIMARY)
    
    # Preenchimentos
    FILL_HEADER = PatternFill(start_color=COLOR_PRIMARY, end_color=COLOR_PRIMARY, fill_type="solid")
    FILL_HEADER_SECONDARY = PatternFill(start_color=COLOR_SECONDARY, end_color=COLOR_SECONDARY, fill_type="solid")
    FILL_HEADER_SUCCESS = PatternFill(start_color=COLOR_SUCCESS, end_color=COLOR_SUCCESS, fill_type="solid")
    FILL_HEADER_INFO = PatternFill(start_color=COLOR_INFO, end_color=COLOR_INFO, fill_type="solid")
    FILL_ALTERNATE = PatternFill(start_color=COLOR_ALTERNATE_ROW, end_color=COLOR_ALTERNATE_ROW, fill_type="solid")
    FILL_LIGHT_BLUE = PatternFill(start_color=COLOR_LIGHT_BLUE, end_color=COLOR_LIGHT_BLUE, fill_type="solid")
    FILL_LIGHT_GREEN = PatternFill(start_color=COLOR_LIGHT_GREEN, end_color=COLOR_LIGHT_GREEN, fill_type="solid")
    
    # Bordas
    BORDER_THIN = Border(
        left=Side(style="thin", color=COLOR_BORDER),
        right=Side(style="thin", color=COLOR_BORDER),
        top=Side(style="thin", color=COLOR_BORDER),
        bottom=Side(style="thin", color=COLOR_BORDER),
    )
    
    # Alinhamentos
    ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ALIGN_RIGHT = Alignment(horizontal="right", vertical="center")


# ============================================================
# 🏗️ CLASSE PRINCIPAL
# ============================================================

class ExcelReportGenerator:
    """Gerador de relatórios Excel com template OrionForgeNexus"""
    
    # Constantes
    DEFAULT_COMPANY_NAME = "OrionForgeNexus"
    DEFAULT_COMPANY_ABBR = "oFn"
    LOGO_SIZE = (80, 80)
    MAX_COLUMN_WIDTH = 60
    MIN_COLUMN_WIDTH = 12
    DEFAULT_COLUMN_WIDTH = 15
    
    def __init__(
        self,
        logo_path: Optional[str] = None,
        company_name: str = DEFAULT_COMPANY_NAME,
        company_abbr: str = DEFAULT_COMPANY_ABBR
    ):
        """
        Inicializa o gerador de relatórios Excel
        
        Args:
            logo_path: Caminho para o arquivo do logo (opcional)
            company_name: Nome da empresa
            company_abbr: Abreviação da empresa
        """
        self.logo_path = self._validate_logo_path(logo_path)
        self.company_name = company_name
        self.company_abbr = company_abbr
    
    @staticmethod
    def _validate_logo_path(logo_path: Optional[str]) -> Optional[Path]:
        """Valida o caminho do logo"""
        if not logo_path:
            return None
        
        path = Path(logo_path)
        if path.exists() and path.is_file():
            return path
        
        log_message(f"⚠️ Logo não encontrado: {logo_path}", "warning")
        return None
    
        # ============================================================
    # 🧾 CABEÇALHO PADRÃO
    # ============================================================

    def _add_header(self, ws: Worksheet, titulo: str, total_cols: int = 8) -> int:
        """
        Adiciona o cabeçalho padrão do relatório com título, data e logo (se disponível).

        Args:
            ws: Planilha do Excel (Worksheet)
            titulo: Texto principal do cabeçalho
            total_cols: Número total de colunas da planilha

        Returns:
            Próxima linha disponível após o cabeçalho
        """
        # 🔹 Mescla células do título
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=total_cols)
        cell = ws.cell(row=1, column=1)
        cell.value = titulo
        cell.font = ExcelStyles.FONT_TITLE
        cell.alignment = ExcelStyles.ALIGN_CENTER

        # 🔹 Linha com data e empresa
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=total_cols)
        date_text = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        ws.cell(row=2, column=1, value=f"Gerado em {date_text} - {self.company_name} ({self.company_abbr})").font = ExcelStyles.FONT_INFO
        ws.cell(row=2, column=1).alignment = ExcelStyles.ALIGN_CENTER

        # 🔹 Adiciona logo (opcional)
        if self.logo_path and Path(self.logo_path).exists():
            try:
                img = XLImage(str(self.logo_path))
                img.width, img.height = self.LOGO_SIZE
                ws.add_image(img, "A1")
            except Exception as e:
                log_message(f"⚠️ Falha ao carregar logo: {e}", "warning")

        # 🔹 Retorna próxima linha
        return 4
        # ============================================================
    # 🧩 AJUSTE AUTOMÁTICO DE COLUNAS
    # ============================================================

    def _auto_adjust_columns(self, ws: Worksheet, total_cols: int) -> None:
        """
        Ajusta automaticamente a largura das colunas com base no conteúdo.

        Args:
            ws: Planilha do Excel
            total_cols: Número total de colunas a ajustar
        """
        for col_idx in range(1, total_cols + 1):
            col_letter = get_column_letter(col_idx)
            max_length = 0

            # Calcula o comprimento máximo do conteúdo na coluna
            for cell in ws[col_letter]:
                try:
                    cell_value = str(cell.value) if cell.value is not None else ""
                    max_length = max(max_length, len(cell_value))
                except Exception:
                    pass

            # Limita o tamanho entre o mínimo e o máximo permitido
            adjusted_width = max(self.MIN_COLUMN_WIDTH, min(max_length + 2, self.MAX_COLUMN_WIDTH))
            ws.column_dimensions[col_letter].width = adjusted_width

    def _add_query_results_sheet(self, ws: Worksheet, query_result: Dict[str, Any]) -> None:
        """
        Adiciona aba com resultados da query.
        Exibe até 10 colunas; se houver mais, adiciona "..." no final.
        """
        columns = query_result.get("columns", []) or []
        preview = query_result.get("preview", []) or []

        if not columns:
            ws["A1"] = "Nenhum dado retornado pela query."
            ws["A1"].font = ExcelStyles.FONT_INFO
            return

        # Limita colunas exibidas
        MAX_COLS = 10
        truncated = len(columns) > MAX_COLS
        visible_columns = columns[:MAX_COLS]
        if truncated:
            visible_columns.append("...")

        current_row = 1
        ws.merge_cells(f"A{current_row}:{get_column_letter(len(visible_columns))}{current_row}")
        ws[f"A{current_row}"] = "📄 Prévia dos Resultados"
        ws[f"A{current_row}"].font = ExcelStyles.FONT_SECTION
        ws[f"A{current_row}"].alignment = ExcelStyles.ALIGN_LEFT
        current_row += 2

        # Cabeçalho
        for col_idx, col_name in enumerate(visible_columns, 1):
            cell = ws.cell(row=current_row, column=col_idx, value=col_name)
            cell.fill = ExcelStyles.FILL_HEADER_INFO
            cell.font = ExcelStyles.FONT_HEADER_SECONDARY
            cell.alignment = ExcelStyles.ALIGN_CENTER
            cell.border = ExcelStyles.BORDER_THIN
        current_row += 1

        # Linhas de dados
        for record in preview:
            row_data = [record.get(col, "") for col in columns[:MAX_COLS]]
            if truncated:
                row_data.append("...")
            for col_idx, value in enumerate(row_data, 1):
                cell = ws.cell(row=current_row, column=col_idx, value=str(value))
                cell.font = ExcelStyles.FONT_DATA
                cell.alignment = ExcelStyles.ALIGN_LEFT
                cell.border = ExcelStyles.BORDER_THIN
                if current_row % 2 == 0:
                    cell.fill = ExcelStyles.FILL_ALTERNATE
            current_row += 1

        # Mensagem de aviso se truncado
        if truncated:
            current_row += 1
            ws.merge_cells(f"A{current_row}:{get_column_letter(len(visible_columns))}{current_row}")
            cell = ws[f"A{current_row}"]
            cell.value = f"⚠️ Exibindo apenas {MAX_COLS} de {len(columns)} colunas. Consulte a versão CSV ou completa no sistema."
            cell.font = ExcelStyles.FONT_INFO
            cell.alignment = ExcelStyles.ALIGN_LEFT

        # Ajusta colunas automaticamente
        self._auto_adjust_columns(ws, len(visible_columns))

    
    # ============================================================
    # 📊 RELATÓRIO DE ESTRUTURA DE BANCO
    # ============================================================
    
    def generate_structure_report(
        self,
        filename: str,
        metadata_list: List[Dict[str, Any]]
    ) -> None:
        """
        Gera relatório Excel de estrutura de banco de dados
        
        Args:
            filename: Nome do arquivo de saída
            metadata_list: Lista de metadados das tabelas
        """
        if not metadata_list:
            raise ValueError("Lista de metadados não pode estar vazia")
        
        log_message("📊 Gerando relatório Excel de estrutura...")
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Estrutura do Banco"
        
        current_row = self._add_header(ws, "Relatório de Estrutura do Banco de Dados", 8)
        
        for idx, metadata in enumerate(metadata_list, 1):
            current_row = self._add_table_structure(ws, metadata, current_row, idx)
            current_row += 2  # Espaço entre tabelas
        
        self._auto_adjust_columns(ws, 8)
        
        wb.save(filename)
        log_message(f"✅ Relatório Excel de estrutura gerado: {filename}")
    
    def _add_table_structure(
        self,
        ws: Worksheet,
        metadata: Dict[str, Any],
        start_row: int,
        table_number: int
    ) -> int:
        """Adiciona estrutura de uma tabela ao relatório"""
        # Título da tabela
        ws.merge_cells(f"A{start_row}:H{start_row}")
        cell = ws[f"A{start_row}"]
        table_name = f"{metadata.get('schema_name', '')}.{metadata.get('table_name', '')}"
        cell.value = f"Tabela {table_number}: {table_name}"
        cell.font = ExcelStyles.FONT_SECTION
        cell.alignment = ExcelStyles.ALIGN_LEFT
        start_row += 1
        
        # Informações gerais
        info_data = [
            ["Schema", metadata.get("schema_name", "—")],
            ["Tabela", metadata.get("table_name", "—")],
            ["Total de Colunas", metadata.get("total_colunas", 0)],
            ["Executado em", metadata.get("executado_em", "—")],
        ]
        
        for row_data in info_data:
            for col_idx, value in enumerate(row_data, 1):
                cell = ws.cell(row=start_row, column=col_idx, value=value)
                cell.border = ExcelStyles.BORDER_THIN
                cell.alignment = ExcelStyles.ALIGN_LEFT
                if col_idx == 1:
                    cell.font = Font(bold=True, size=10)
                    cell.fill = ExcelStyles.FILL_LIGHT_BLUE
                else:
                    cell.fill = ExcelStyles.FILL_ALTERNATE
            start_row += 1
        
        start_row += 1
        
        # Cabeçalho das colunas
        columns = ["Nome", "Tipo", "Nullable", "PK", "FK", "Unique", "Default", "Comentário"]
        for col_idx, col_name in enumerate(columns, 1):
            cell = ws.cell(row=start_row, column=col_idx, value=col_name)
            cell.fill = ExcelStyles.FILL_HEADER_INFO
            cell.font = ExcelStyles.FONT_HEADER_SECONDARY
            cell.alignment = ExcelStyles.ALIGN_CENTER
            cell.border = ExcelStyles.BORDER_THIN
        start_row += 1
        
        # Dados das colunas
        colunas = metadata.get("colunas", [])
        for coluna in colunas:
            row_data = [
                coluna.get("nome", ""),
                coluna.get("tipo", ""),
                "Sim" if coluna.get("is_nullable") else "Não",
                "✓" if coluna.get("is_primary_key") else "",
                "✓" if coluna.get("is_foreign_key") else "",
                "✓" if coluna.get("is_unique") else "",
                coluna.get("default", ""),
                coluna.get("comentario", ""),
            ]
            
            for col_idx, value in enumerate(row_data, 1):
                cell = ws.cell(row=start_row, column=col_idx, value=value)
                cell.border = ExcelStyles.BORDER_THIN
                cell.alignment = ExcelStyles.ALIGN_LEFT if col_idx in [1, 2, 8] else ExcelStyles.ALIGN_CENTER
                cell.font = ExcelStyles.FONT_DATA
                if start_row % 2 == 0:
                    cell.fill = ExcelStyles.FILL_ALTERNATE
            
            start_row += 1
        
        return start_row
    
    # ============================================================
    # 📋 RELATÓRIO DE QUERY
    # ============================================================
    
    def generate_query_report(
        self,
        filename: str,
        query_result: Dict[str, Any]
    ) -> None:
        """
        Gera relatório Excel de resultado de query
        
        Args:
            filename: Nome do arquivo de saída
            query_result: Resultado da query SQL
        """
        if not query_result:
            raise ValueError("Resultado da query não pode estar vazio")
        
        log_message("📋 Gerando relatório Excel de query...")
        
        wb = Workbook()
        
        # Aba 1: Informações da Query
        ws_info = wb.active
        ws_info.title = "Informações"
        self._add_query_info_sheet(ws_info, query_result)
        
        # Aba 2: Resultados
        ws_results = wb.create_sheet("Resultados")
        self._add_query_results_sheet(ws_results, query_result)
        
        wb.save(filename)
        log_message(f"✅ Relatório Excel de query gerado: {filename}")
    
    def _add_query_info_sheet(self, ws: Worksheet, query_result: Dict[str, Any]) -> None:
        """Adiciona aba com informações da query"""
        current_row = self._add_header(ws, "Relatório de Execução de Query SQL", 4)
        
        # Status
        ws[f"A{current_row}"] = "Status da Execução"
        ws[f"A{current_row}"].font = ExcelStyles.FONT_SECTION
        current_row += 1
        
        status_data = [
            ["Status", "✅ Sucesso" if query_result.get("success") else "❌ Falhou"],
            ["Duração (ms)", query_result.get("duration_ms", "—")],
            ["Total de Resultados", query_result.get("totalResults", "—")],
            ["Prévia Carregada", len(query_result.get("preview", []))],
        ]
        
        for row_data in status_data:
            for col_idx, value in enumerate(row_data, 1):
                cell = ws.cell(row=current_row, column=col_idx, value=value)
                cell.border = ExcelStyles.BORDER_THIN
                cell.alignment = ExcelStyles.ALIGN_LEFT
                if col_idx == 1:
                    cell.font = Font(bold=True, size=10)
                    cell.fill = ExcelStyles.FILL_LIGHT_BLUE
                else:
                    cell.fill = ExcelStyles.FILL_ALTERNATE
            current_row += 1
        
        current_row += 2
        
        # Query
        ws[f"A{current_row}"] = "Query SQL"