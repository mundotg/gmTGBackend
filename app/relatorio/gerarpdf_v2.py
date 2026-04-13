"""
Gerador de Relatórios Genérico - OrionForgeNexus (oFn)
-------------------------------------------------------
Agora compatível com estrutura do frontend:

{
  "type": "text",
  "data": {...},
  "style": {...}
}
"""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, HRFlowable
)
from reportlab.lib.units import cm
from reportlab.platypus.frames import Frame
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
import os

# ============================================================================
# CORE
# ============================================================================

class GenericReportGenerator:

    def __init__(self, logo_path=None, company_name="OrionForgeNexus", company_abbr="oFn"):
        self.logo_path = logo_path
        self.company_name = company_name
        self.company_abbr = company_abbr
        self.styles = getSampleStyleSheet()
        self.elements = []
        self.footer_data = None

    # ============================================================================
    # FOOTER FIXO
    # ============================================================================
    def _footer(self, canvas, doc):
        canvas.saveState()

        page_width, _ = A4
        footer_y = 1.5 * cm

        canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
        canvas.setLineWidth(0.5)
        canvas.line(2*cm, footer_y + 0.5*cm, page_width - 2*cm, footer_y + 0.5*cm)

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#94a3b8"))

        if self.footer_data:
            canvas.drawString(2*cm, footer_y, self.footer_data.get("left", ""))
            canvas.drawCentredString(page_width / 2, footer_y, self.footer_data.get("center", ""))
            canvas.drawRightString(page_width - 2*cm, footer_y, self.footer_data.get("right", ""))
        else:
            default = f"{self.company_name} ({self.company_abbr})"
            canvas.drawCentredString(page_width / 2, footer_y, default)

        canvas.setFont("Helvetica-Bold", 8)
        canvas.drawRightString(page_width - 2*cm, footer_y - 0.4*cm, f"Página {doc.page}")

        canvas.restoreState()

    # ============================================================================
    # STYLE HELPER
    # ============================================================================
    def _apply_style_spacing(self, style: dict):
        if not style:
            return

        mt = style.get("marginTop", 0)
        mb = style.get("marginBottom", 0)

        if mt:
            self.elements.append(Spacer(1, mt * cm))
        if mb:
            self.elements.append(Spacer(1, mb * cm))

    # ============================================================================
    # ELEMENTOS
    # ============================================================================

    def _add_header(self, data: dict, style: dict):
        title = data.get("title", "")
        subtitle = data.get("subtitle", "")

        style_title = ParagraphStyle(
            "HeaderTitle",
            parent=self.styles["Heading1"],
            fontSize=data.get("title_size", 20),
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1e3a8a")
        )

        style_sub = ParagraphStyle(
            "HeaderSub",
            parent=self.styles["Normal"],
            fontSize=data.get("subtitle_size", 12),
            alignment=TA_CENTER,
            textColor=colors.HexColor("#64748b")
        )

        self._apply_style_spacing(style)

        self.elements.append(Paragraph(title, style_title))
        if subtitle:
            self.elements.append(Paragraph(subtitle, style_sub))

    def _add_text(self, data: dict, style: dict):
        value = data.get("value", "")

        align = style.get("align", data.get("align", "left"))

        alignment_map = {
            "left": TA_LEFT,
            "center": TA_CENTER,
            "right": TA_RIGHT,
            "justify": TA_JUSTIFY
        }

        style_text = ParagraphStyle(
            "Text",
            parent=self.styles["Normal"],
            fontSize=data.get("size", 11),
            textColor=colors.HexColor(data.get("color", "#334155")),
            alignment=alignment_map.get(align, TA_LEFT),
        )

        self._apply_style_spacing(style)

        self.elements.append(Paragraph(value, style_text))

    def _add_table(self, data: dict, style: dict):
        columns = data.get("columns", [])
        rows = data.get("rows", [])

        if not columns or not rows:
            return

        col_widths = data.get("colWidths")

        if col_widths:
            col_widths = [w * cm for w in col_widths]

        table_data = [columns] + rows

        table = Table(table_data, colWidths=col_widths, repeatRows=1)

        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ]))

        self._apply_style_spacing(style)

        self.elements.append(table)
        self.elements.append(Spacer(1, 0.5 * cm))

    def _add_image(self, data: dict, style: dict):
        path = data.get("path")
        if not path or not os.path.exists(path):
            return

        width = data.get("width", 10) * cm
        height = data.get("height", 5) * cm

        align = style.get("align", "center").upper()

        img = Image(path, width=width, height=height)
        img.hAlign = align

        self._apply_style_spacing(style)

        self.elements.append(img)

    def _add_list(self, data: dict, style: dict):
        items = data.get("items", [])
        bullet = data.get("bullet", "•")

        for item in items:
            self.elements.append(Paragraph(f"{bullet} {item}", self.styles["Normal"]))

        self._apply_style_spacing(style)

    def _add_line(self, data: dict, style: dict):
        color = data.get("color", "#cbd5e1")
        thickness = data.get("thickness", 1)

        line = HRFlowable(
            width="100%",
            thickness=thickness,
            color=colors.HexColor(color)
        )

        self._apply_style_spacing(style)

        self.elements.append(line)

    def _add_spacer(self, data: dict):
        height = data.get("height", 1)
        self.elements.append(Spacer(1, height * cm))

    def _add_pagebreak(self):
        self.elements.append(PageBreak())

    def _set_footer(self, data: dict):
        self.footer_data = data

    # ============================================================================
    # GENERATE
    # ============================================================================
    def generate_report(self, filename: str, structure: list, orientation="portrait"):

        pagesize = landscape(A4) if orientation == "landscape" else A4

        doc = BaseDocTemplate(
            filename,
            pagesize=pagesize,
            rightMargin=2*cm,
            leftMargin=2*cm,
            topMargin=2*cm,
            bottomMargin=3*cm
        )

        frame = Frame(
            doc.leftMargin,
            doc.bottomMargin,
            doc.width,
            doc.height,
            id='main'
        )

        template = PageTemplate(id='template', frames=frame, onPage=self._footer)
        doc.addPageTemplates([template])

        # 🔥 PARSER ALINHADO COM FRONTEND
        for section in structure:
            section_type = section.get("type")
            data = section.get("data", {})
            style = section.get("style", {})

            if section_type == "header":
                self._add_header(data, style)

            elif section_type == "text":
                self._add_text(data, style)

            elif section_type == "table":
                self._add_table(data, style)

            elif section_type == "image":
                self._add_image(data, style)

            elif section_type == "list":
                self._add_list(data, style)

            elif section_type == "line":
                self._add_line(data, style)

            elif section_type == "spacer":
                self._add_spacer(data)

            elif section_type == "pagebreak":
                self._add_pagebreak()

            elif section_type == "footer":
                self._set_footer(data)

        doc.build(self.elements)