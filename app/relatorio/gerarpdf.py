"""
Gerador de Relatórios Genérico - OrionForgeNexus (oFn)
-------------------------------------------------------
Sistema 100% declarativo que recebe estrutura JSON e gera PDF automaticamente.
Rodapé fixo na parte inferior de cada página.

Estrutura suportada:
--------------------
report_data = [
    {"header": {"title": "...", "subtitle": "...", "logo": True/False}},
    {"text": {"value": "...", "align": "left/center/right", "size": 11, "bold": True/False}},
    {"spacer": {"height": 1}},  # em centímetros
    {"table": {
        "columns": ["Col1", "Col2"],
        "rows": [[val1, val2], ...],
        "col_widths": [5, 10]  # opcional, em cm
    }},
    {"image": {"path": "imagem.png", "width": 10, "height": 5, "align": "center"}},
    {"list": {"items": ["Item 1", "Item 2"], "bullet": "•"}},
    {"line": {"color": "#cccccc", "width": 1}},  # linha horizontal
    {"pagebreak": {}},
    {"footer": {"left": "...", "center": "...", "right": "..."}}
]
"""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import Paragraph, Spacer, Table, TableStyle, Image, PageBreak, HRFlowable
from reportlab.lib.units import cm
from reportlab.platypus.frames import Frame
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from datetime import datetime
import os
from app.ultils.logger import log_message



class GenericReportGenerator:
    """Gerador ultra-genérico de relatórios PDF com rodapé fixo"""
    
    def __init__(self, logo_path=None, company_name="OrionForgeNexus", company_abbr="oFn"):
        self.logo_path = logo_path
        self.company_name = company_name
        self.company_abbr = company_abbr
        self.styles = getSampleStyleSheet()
        self.elements = []
        self.footer_data = None
        
    # ========================================
    # 🎨 RODAPÉ FIXO
    # ========================================
    def _footer(self, canvas, doc):
        """Desenha rodapé fixo no final de cada página"""
        canvas.saveState()
        
        # Configurações de posição
        page_width, page_height = A4
        footer_y = 1.5 * cm
        
        # Linha superior do rodapé
        canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
        canvas.setLineWidth(0.5)
        canvas.line(2*cm, footer_y + 0.5*cm, page_width - 2*cm, footer_y + 0.5*cm)
        
        # Texto do rodapé
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#94a3b8"))
        
        if self.footer_data:
            # Rodapé customizado (esquerda, centro, direita)
            left_text = self.footer_data.get("left", "")
            center_text = self.footer_data.get("center", "")
            right_text = self.footer_data.get("right", "")
            
            if left_text:
                canvas.drawString(2*cm, footer_y, left_text)
            if center_text:
                canvas.drawCentredString(page_width / 2, footer_y, center_text)
            if right_text:
                canvas.drawRightString(page_width - 2*cm, footer_y, right_text)
        else:
            # Rodapé padrão
            default_footer = f"© {datetime.now().year} {self.company_name} ({self.company_abbr}) - Gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M')}"
            canvas.drawCentredString(page_width / 2, footer_y, default_footer)
        
        # Número da página
        canvas.setFont("Helvetica-Bold", 8)
        page_num = f"Página {doc.page}"
        canvas.drawRightString(page_width - 2*cm, footer_y - 0.4*cm, page_num)
        
        canvas.restoreState()
    
    # ========================================
    # 🔨 CONSTRUTORES DE ELEMENTOS
    # ========================================
    
    def _add_header(self, data: dict):
        """Adiciona cabeçalho com título e subtítulo"""
        title = data.get("title", "Sem título")
        subtitle = data.get("subtitle", "")
        show_logo = data.get("logo", True)
        
        # Estilo do título
        style_title = ParagraphStyle(
            "HeaderTitle",
            parent=self.styles["Heading1"],
            fontSize=data.get("title_size", 20),
            textColor=colors.HexColor(data.get("title_color", "#1e3a8a")),
            alignment=TA_CENTER,
            spaceAfter=10,
            fontName="Helvetica-Bold"
        )
        
        # Estilo do subtítulo
        style_subtitle = ParagraphStyle(
            "HeaderSubtitle",
            parent=self.styles["Heading2"],
            fontSize=data.get("subtitle_size", 12),
            textColor=colors.HexColor(data.get("subtitle_color", "#475569")),
            alignment=TA_CENTER,
            spaceAfter=20,
        )
        
        # Logo
        if show_logo and self.logo_path and os.path.exists(self.logo_path):
            try:
                logo_width = data.get("logo_width", 3)
                logo_height = data.get("logo_height", 3)
                img = Image(self.logo_path, width=logo_width*cm, height=logo_height*cm)
                img.hAlign = "CENTER"
                self.elements.append(img)
                self.elements.append(Spacer(1, 0.5*cm))
            except Exception as e:
                log_message(f"⚠️ Erro ao carregar logo: {e}","error")
        
        # Adiciona título e subtítulo
        self.elements.append(Paragraph(title, style_title))
        if subtitle:
            self.elements.append(Paragraph(subtitle, style_subtitle))
    
    def _add_text(self, data: dict):
        """Adiciona parágrafo de texto"""
        value = data.get("value", "")
        align = data.get("align", "left").lower()
        size = data.get("size", 11)
        bold = data.get("bold", False)
        color = data.get("color", "#334155")
        
        alignment_map = {
            "left": TA_LEFT,
            "center": TA_CENTER,
            "right": TA_RIGHT,
            "justify": TA_JUSTIFY
        }
        alignment = alignment_map.get(align, TA_LEFT)
        
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        
        style_text = ParagraphStyle(
            "CustomText",
            parent=self.styles["Normal"],
            fontSize=size,
            textColor=colors.HexColor(color),
            alignment=alignment,
            spaceAfter=10,
            fontName=font_name
        )
        
        self.elements.append(Paragraph(value, style_text))
    
    def _add_spacer(self, data: dict):
        """Adiciona espaço vertical"""
        height = data.get("height", 1)
        self.elements.append(Spacer(1, height * cm))
    
    def _add_table(self, data: dict):
        """Adiciona tabela formatada com quebra automática de texto"""
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        col_widths = data.get("col_widths", None)
        
        if not columns or not rows:
            return
        
        # Converte dados para Paragraph para permitir quebra de linha
        def wrap_text(text, max_width=None):
            """Converte texto em Paragraph para quebra automática"""
            if text is None or text == "":
                return Paragraph("", self.styles["Normal"])
            
            style = ParagraphStyle(
                'CellText',
                parent=self.styles['Normal'],
                fontSize=9,
                leading=11,
                alignment=TA_CENTER,
                wordWrap='CJK'  # Quebra de linha melhorada
            )
            return Paragraph(str(text), style)
        
        # Converte cabeçalho
        header_style = ParagraphStyle(
            'HeaderCell',
            parent=self.styles['Normal'],
            fontSize=10,
            fontName='Helvetica-Bold',
            leading=12,
            alignment=TA_CENTER,
            textColor=colors.white
        )
        header_row = [Paragraph(str(col), header_style) for col in columns]
        
        # Converte linhas de dados
        wrapped_rows = []
        for row in rows:
            wrapped_row = [wrap_text(cell) for cell in row]
            wrapped_rows.append(wrapped_row)
        
        table_data = [header_row] + wrapped_rows
        
        # Largura das colunas
        if col_widths:
            col_widths = [w * cm for w in col_widths]
        else:
            available_width = (A4[0] - 4*cm)
            col_widths = [available_width / len(columns)] * len(columns)
        
        # Cria tabela com altura mínima de linha
        table = Table(
            table_data, 
            colWidths=col_widths, 
            hAlign="CENTER",
            repeatRows=1  # Repete cabeçalho em páginas seguintes
        )
        
        # Cores customizáveis
        header_color = data.get("header_color", "#1e3a8a")
        row_colors = data.get("row_colors", ["#ffffff", "#f8fafc"])
        
        table.setStyle(TableStyle([
            # Cabeçalho
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(header_color)),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
            ("TOPPADDING", (0, 0), (-1, 0), 10),
            
            # Corpo
            ("BACKGROUND", (0, 1), (-1, -1), colors.white),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor(c) for c in row_colors]),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 1), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
            
            # CRÍTICO: Permitir quebra de linha automática
            ("WORDWRAP", (0, 0), (-1, -1), True),
        ]))
        
        self.elements.append(table)
        self.elements.append(Spacer(1, 0.5*cm))
    
    def _add_image(self, data: dict):
        """Adiciona imagem"""
        path = data.get("path")
        if not path or not os.path.exists(path):
            return
        
        try:
            width = data.get("width", 10) * cm
            height = data.get("height", 5) * cm
            align = data.get("align", "center").upper()
            
            img = Image(path, width=width, height=height)
            img.hAlign = align
            self.elements.append(img)
            self.elements.append(Spacer(1, 0.3*cm))
        except Exception as e:
            log_message(f"⚠️ Erro ao carregar imagem: {e}","error")
    
    def _add_list(self, data: dict):
        """Adiciona lista com marcadores"""
        items = data.get("items", [])
        bullet = data.get("bullet", "•")
        
        style_list = ParagraphStyle(
            "ListItem",
            parent=self.styles["Normal"],
            fontSize=11,
            textColor=colors.HexColor("#334155"),
            leftIndent=20,
            spaceAfter=5
        )
        
        for item in items:
            text = f"{bullet} {item}"
            self.elements.append(Paragraph(text, style_list))
        
        self.elements.append(Spacer(1, 0.3*cm))
    
    def _add_line(self, data: dict):
        """Adiciona linha horizontal"""
        color = data.get("color", "#cbd5e1")
        width = data.get("width", 1)
        
        line = HRFlowable(
            width="100%",
            thickness=width,
            color=colors.HexColor(color),
            spaceBefore=5,
            spaceAfter=5
        )
        self.elements.append(line)
    
    def _add_pagebreak(self):
        """Adiciona quebra de página"""
        self.elements.append(PageBreak())
    
    def _set_footer(self, data: dict):
        """Define dados do rodapé fixo"""
        self.footer_data = data
    
    # ========================================
    # 📄 GERAÇÃO DO RELATÓRIO
    # ========================================
    
    def generate_report(self, filename: str, structure: list, orientation="portrait"):
        """
        Gera relatório PDF baseado na estrutura declarativa
        
        Args:
            filename (str): Nome do arquivo de saída
            structure (list): Lista de dicionários com elementos do relatório
            orientation (str): "portrait" ou "landscape"
        """
        # Define orientação
        pagesize = landscape(A4) if orientation == "landscape" else A4
        
        # Cria documento com rodapé fixo
        doc = BaseDocTemplate(
            filename,
            pagesize=pagesize,
            rightMargin=2*cm,
            leftMargin=2*cm,
            topMargin=2*cm,
            bottomMargin=3*cm  # Espaço para o rodapé
        )
        
        # Frame principal (área de conteúdo)
        frame = Frame(
            doc.leftMargin,
            doc.bottomMargin,
            doc.width,
            doc.height,
            id='normal'
        )
        
        # Template de página com rodapé
        template = PageTemplate(id='main', frames=frame, onPage=self._footer)
        doc.addPageTemplates([template])
        
        # Processa estrutura
        for section in structure:
            if "header" in section:
                self._add_header(section["header"])
            elif "text" in section:
                self._add_text(section["text"])
            elif "spacer" in section:
                self._add_spacer(section["spacer"])
            elif "table" in section:
                self._add_table(section["table"])
            elif "image" in section:
                self._add_image(section["image"])
            elif "list" in section:
                self._add_list(section["list"])
            elif "line" in section:
                self._add_line(section["line"])
            elif "pagebreak" in section:
                self._add_pagebreak()
            elif "footer" in section:
                self._set_footer(section["footer"])
        
        # Gera PDF
        doc.build(self.elements)
        log_message(f"✅ Relatório gerado com sucesso: {filename}")


# # ========================================
# # 🧪 EXEMPLOS DE USO
# # ========================================

# if __name__ == "__main__":
    
#     # Exemplo 1: Relatório simples
#     print("📄 Gerando Exemplo 1: Relatório Simples")
#     report_simple = [
#         {"header": {"title": "Relatório de Vendas", "subtitle": "Outubro 2025"}},
#         {"text": {"value": "Este relatório apresenta as vendas realizadas no mês.", "align": "center"}},
#         {"spacer": {"height": 0.5}},
#         {"table": {
#             "columns": ["ID", "Cliente", "Valor", "Status"],
#             "rows": [
#                 [1, "Maria Silva", "R$ 350,00", "Concluído"],
#                 [2, "João Pedro", "R$ 200,00", "Pendente"],
#                 [3, "Ana Paula", "R$ 580,00", "Concluído"]
#             ]
#         }},
#         {"footer": {"center": f"© {datetime.now().year} OrionForgeNexus - Relatório gerado automaticamente"}}
#     ]
    
#     generator1 = GenericReportGenerator()
#     generator1.generate_report("exemplo1_simples.pdf", report_simple)
    
    
#     # Exemplo 2: Relatório completo com todos os elementos
#     print("\n📄 Gerando Exemplo 2: Relatório Completo")
#     report_complete = [
#         {"header": {"title": "Relatório Executivo Completo", "subtitle": "Análise Trimestral - Q4 2025", "logo": False}},
        
#         {"text": {"value": "Sumário Executivo", "align": "left", "size": 14, "bold": True, "color": "#1e3a8a"}},
#         {"line": {"color": "#1e3a8a", "width": 2}},
#         {"spacer": {"height": 0.3}},
        
#         {"text": {
#             "value": "Este documento apresenta uma análise detalhada do desempenho da empresa no último trimestre, incluindo métricas financeiras, operacionais e estratégicas.",
#             "align": "justify"
#         }},
        
#         {"spacer": {"height": 0.5}},
#         {"text": {"value": "Principais Indicadores:", "bold": True}},
#         {"list": {
#             "items": [
#                 "Receita total: R$ 1.250.000,00 (↑ 15% vs Q3)",
#                 "Novos clientes: 47 empresas",
#                 "Taxa de satisfação: 94,5%",
#                 "Projetos concluídos: 23"
#             ],
#             "bullet": "✓"
#         }},
        
#         {"spacer": {"height": 0.5}},
#         {"text": {"value": "Detalhamento por Departamento", "size": 13, "bold": True, "color": "#475569"}},
#         {"line": {}},
        
#         {"table": {
#             "columns": ["Departamento", "Meta", "Realizado", "% Atingido"],
#             "rows": [
#                 ["Vendas", "R$ 800k", "R$ 850k", "106%"],
#                 ["Marketing", "R$ 200k", "R$ 195k", "97%"],
#                 ["Operações", "R$ 250k", "R$ 205k", "82%"]
#             ],
#             "header_color": "#059669",
#             "row_colors": ["#ffffff", "#ecfdf5"]
#         }},
        
#         {"pagebreak": {}},
        
#         {"text": {"value": "Próximas Ações Estratégicas", "size": 14, "bold": True, "align": "center"}},
#         {"spacer": {"height": 0.3}},
#         {"list": {
#             "items": [
#                 "Expandir equipe de desenvolvimento em 20%",
#                 "Lançar novo produto no Q1 2026",
#                 "Implementar sistema de CRM integrado",
#                 "Reforçar presença em redes sociais"
#             ]
#         }},
        
#         {"spacer": {"height": 1}},
#         {"text": {
#             "value": "Relatório aprovado pela diretoria em 18/10/2025",
#             "align": "right",
#             "size": 9,
#             "color": "#64748b"
#         }},
        
#         {"footer": {
#             "left": "OrionForgeNexus (oFn)",
#             "center": "Documento Confidencial",
#             "right": f"Gerado em {datetime.now().strftime('%d/%m/%Y')}"
#         }}
#     ]
    
#     generator2 = GenericReportGenerator()
#     generator2.generate_report("exemplo2_completo.pdf", report_complete)
    
    
#     # Exemplo 3: Relatório em modo paisagem
#     print("\n📄 Gerando Exemplo 3: Relatório Paisagem (Landscape)")
#     report_landscape = [
#         {"header": {"title": "Dashboard de Projetos 2025"}},
#         {"table": {
#             "columns": ["ID", "Projeto", "Cliente", "Início", "Fim", "Status", "Orçamento", "Gasto", "Margem"],
#             "rows": [
#                 [1, "Sistema ERP", "Empresa A", "01/01", "30/06", "Concluído", "R$ 50k", "R$ 45k", "10%"],
#                 [2, "App Mobile", "Empresa B", "15/02", "15/08", "Em Andamento", "R$ 30k", "R$ 22k", "27%"],
#                 [3, "Site Institucional", "Empresa C", "10/03", "10/05", "Concluído", "R$ 15k", "R$ 14k", "7%"],
#                 [4, "Consultoria TI", "Empresa D", "01/04", "01/12", "Em Andamento", "R$ 80k", "R$ 60k", "25%"]
#             ],
#             "col_widths": [1.5, 4, 3.5, 2, 2, 3, 2.5, 2.5, 2]
#         }},
#         {"footer": {"center": "OrionForgeNexus - Gestão de Projetos"}}
#     ]
    
#     generator3 = GenericReportGenerator()
#     generator3.generate_report("exemplo3_landscape.pdf", report_landscape, orientation="landscape")
    
    
#     print("\n✅ Todos os exemplos foram gerados com sucesso!")
#     print("📁 Arquivos criados:")
#     print("   • exemplo1_simples.pdf")
#     print("   • exemplo2_completo.pdf")
#     print("   • exemplo3_landscape.pdf")