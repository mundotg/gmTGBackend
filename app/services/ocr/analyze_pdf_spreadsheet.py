import io
import asyncio
import traceback
import fitz  # Import do PyMuPDF
from typing import AsyncGenerator, Dict, Any

from app.services.ocr._OCR_CACHE import get_cached_result, set_cached_result
from app.services.ocr.newpaddleORC import analyze_image
from app.ultils.logger import log_message


async def _process_extracted_image(
    image_bytes: bytes, page_num: int, img_index: int
) -> AsyncGenerator[str, None]:
    """
    Componente separado para processar os bytes de uma imagem específica via OCR.
    Retorna um gerador assíncrono com as linhas de texto detectadas.
    """
    cache_key = f"page_{page_num}_img_{img_index}"
    cached = get_cached_result(image_bytes, cache_key)

    if cached:
        lines = cached.get("lines", [])
    else:
        result = analyze_image(image_bytes)

        if isinstance(result, dict) and result.get("error"):
            yield f"[Erro no OCR da imagem {img_index}]\n"
            return

        set_cached_result(image_bytes, cache_key, result)
        lines = result.get("lines", [])

    for line in lines:
        yield line + "\n"
        await asyncio.sleep(0.005)


# ==========================================
# GERADOR: EXTRAÇÃO REAL DE PDF (PyMuPDF)
# ==========================================


async def generate_pdf_text(file_bytes: bytes) -> AsyncGenerator[str, None]:
    """
    Lê o texto nativo do PDF e extrai imagens embutidas para análise OCR.
    """
    if not file_bytes:
        yield "[Erro: PDF vazio]\n"
        return

    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")

        if doc.is_encrypted:
            # yield "[Erro: PDF protegido]\n"
            return

        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            yield f"\n--- Página {page_num + 1} ---\n"

            # 1. Extrair e enviar o texto nativo da página
            text = page.get_text().strip()
            if text:
                yield text + "\n"
                await asyncio.sleep(0.01)

            # 2. Detectar e processar imagens embutidas na página
            images = page.get_images(full=True)

            if images:
                # yield f"\n[Processando {len(images)} imagem(ns) encontrada(s)...]\n"

                for img_index, img in enumerate(images):
                    xref = img[0]  # Referência cruzada da imagem no PDF
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]

                    # Chama a função auxiliar para fazer o OCR da imagem específica
                    async for ocr_line in _process_extracted_image(
                        image_bytes, page_num, img_index
                    ):
                        yield ocr_line

            await asyncio.sleep(0.05)

        doc.close()
        yield "\n[Fim do Documento]\n"

    except Exception as e:
        log_message(f"Erro PDF OCR: {str(e)}\n{traceback.format_exc()}", "error")
        yield "[Erro ao processar PDF]\n"


# ==========================================
# GERADOR: EXTRAÇÃO REAL DE PLANILHA (Pandas)
# ==========================================
async def generate_spreadsheet_text(file_bytes: bytes, filename: str = "arquivo.xlsx"):
    """
    Usa o pandas para ler as linhas, retornando-as uma a uma via stream.
    """
    if not file_bytes:
        yield "[Erro: O ficheiro de planilha enviado está vazio ou corrompido.]\n"
        return

    try:
        # Verifica se é CSV ou Excel para usar o motor certo do pandas
        if filename.lower().endswith(".csv"):
            # Lida com CSVs (tenta detetar o separador automaticamente)
            df = pd.read_csv(io.BytesIO(file_bytes), sep=None, engine="python")
        else:
            # Lida com .xlsx e .xls (usa openpyxl ou xlrd automaticamente)
            df = pd.read_excel(io.BytesIO(file_bytes))

        # Validação: Planilha vazia (sem colunas ou sem linhas)
        if df.empty:
            yield "[Aviso: A planilha está vazia ou não contém dados legíveis.]\n"
            return

        # 1. Enviar o Cabeçalho da planilha
        headers = " | ".join(str(col).strip() for col in df.columns)
        yield f"CABEÇALHOS: {headers}\n{'-'*40}\n"
        await asyncio.sleep(0.05)

        # 2. Iterar linha a linha real (o iterrows já faz isso)
        for row_number, (_, row) in enumerate(df.iterrows(), start=1):
            # Pega nos valores, ignora os nulos (pd.notna) e junta-os numa string
            linha_texto = " | ".join(
                [str(val).strip() for val in row.values if pd.notna(val)]
            )

            if linha_texto.strip():
                yield f"Linha {row_number}: {linha_texto}\n"

            # Pausa a cada linha para fazer o stream contínuo no frontend
            await asyncio.sleep(0.01)

        yield "\n[Fim da Planilha]\n"

    except Exception as e:
        log_message(
            f"Erro ao ler Planilha: {str(e)}\n{traceback.format_exc()}", level="error"
        )
        yield "[Erro crítico ao processar a planilha. Verifique se a formatação está correta e não está corrompida.]\n"
