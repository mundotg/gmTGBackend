import io
import asyncio
import traceback
import fitz  # Import do PyMuPDF
import pandas as pd

from app.ultils.logger import log_message


# ==========================================
# GERADOR: EXTRAÇÃO REAL DE PDF (PyMuPDF)
# ==========================================
# ==========================================
# GERADOR: EXTRAÇÃO REAL DE PDF (PyMuPDF)
# ==========================================
async def generate_pdf_text(file_bytes: bytes):
    """
    Usa o PyMuPDF (fitz) para extrair o texto, forçando a leitura estrita
    da esquerda para a direita, linha por linha (ideal para Tabelas).
    """
    if not file_bytes:
        yield "[Erro: O ficheiro PDF enviado está vazio ou corrompido.]\n"
        return

    try:
        # Abre o PDF diretamente da memória (bytes)
        doc = fitz.open(stream=file_bytes, filetype="pdf")

        # Validação: PDF protegido por palavra-passe
        if doc.is_encrypted:
            yield "[Erro: O documento PDF está protegido por palavra-passe e não pode ser lido.]\n"
            doc.close()
            return

        # Validação: PDF sem páginas
        if len(doc) == 0:
            yield "[Aviso: O documento PDF não contém nenhuma página.]\n"
            doc.close()
            return

        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            yield f"--- Página {page_num + 1} ---\n"

            # Extrai todas as palavras individuais e as suas coordenadas geográficas no PDF
            # Retorna: (x0, y0, x1, y1, "texto", block_no, line_no, word_no)
            words = page.get_text("words")

            if words:
                # ---------------------------------------------------------
                # ALGORITMO DE AGRUPAMENTO VISUAL HORIZONTAL (Tabelas/Linhas)
                # ---------------------------------------------------------
                # 1. Agrupamos pela posição Vertical (Eixo Y).
                # Usamos o centro da palavra (y0 + y1)/2 e dividimos por 5.
                # Isto agrupa palavras que sofram ligeiros desalinhamentos de 1 a 4 pixels na mesma linha.
                # 2. Ordenamos pela posição Horizontal (Eixo X) para ler da esquerda para a direita.
                words.sort(key=lambda w: (round((w[1] + w[3]) / 2 / 5), w[0]))

                current_line = []
                current_y = None

                for w in words:
                    text = w[4]
                    y = round((w[1] + w[3]) / 2 / 5)

                    if current_y is None or y == current_y:
                        current_line.append(text)
                        current_y = y
                    else:
                        # O Eixo Y mudou, logo mudámos de linha. Emitimos a linha anterior!
                        linha_texto = " ".join(current_line).strip()
                        if linha_texto:
                            yield f"{linha_texto}\n"
                            await asyncio.sleep(
                                0.005
                            )  # Pausa para o stream fluir suavemente

                        # Começa a construir a nova linha
                        current_line = [text]
                        current_y = y

                # Garante que a última linha processada também é enviada
                if current_line:
                    linha_texto = " ".join(current_line).strip()
                    if linha_texto:
                        yield f"{linha_texto}\n"

            # Pausa ligeiramente maior ao mudar de página
            await asyncio.sleep(0.05)

        doc.close()
        yield "\n[Fim do Documento]\n"

    except Exception as e:
        log_message(
            f"Erro ao ler PDF: {str(e)}\n{traceback.format_exc()}", level="error"
        )
        yield "[Erro crítico ao processar o PDF. Por favor, verifique se o ficheiro é válido.]\n"


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
