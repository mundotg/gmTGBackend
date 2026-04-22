import traceback
import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.ocr._OCR_CACHE import get_cached_result, set_cached_result
from app.services.ocr.analyze_pdf_spreadsheet import (
    generate_pdf_text,
    generate_spreadsheet_text,
)
from app.services.ocr.newpaddleORC import analyze_image
from app.services.ocr.ultils.extractors import extract_angolan_id_data
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/ocr", tags=["ocr"])


# ==========================================
# FUNÇÃO AUXILIAR: Simula Stream da Cache
# ==========================================
async def stream_from_cache(cached_text: str):
    """Lê o texto inteiro da cache e envia-o em pequenos blocos (stream)"""
    lines = cached_text.splitlines()
    for line in lines:
        yield f"{line}\n"
        await asyncio.sleep(0.005)  # Pausa mínima para o frontend animar fluidamente


import json  # 👈 Não te esqueças de importar o json no topo do ficheiro
from typing import Optional
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    UploadFile,
    File,
    Form,
)  # 👈 Importa o Form


# ==========================================
# ROTA 1: IMAGEM (Processamento normal - JSON)
# ==========================================
@router.post("/analyze-image")
async def analyze_image_route(
    image: UploadFile = File(...),
    formulario: Optional[str] = Form(None),  # 👈 Recebe como string via Form-Data
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        if not image.content_type or not image.content_type.startswith("image/"):
            raise HTTPException(
                status_code=400, detail="Arquivo enviado não é uma imagem válida."
            )

        image_bytes = await image.read()

        if not image_bytes:
            raise HTTPException(status_code=400, detail="Imagem vazia ou corrompida.")

        filename = image.filename or "imagem.jpg"

        # Converte a string JSON do frontend de volta para Dicionário/Lista no Python
        # Converte a string JSON do frontend de volta para Lista no Python
        campos_formulario = []  # 👈 Muda de {} para []
        if formulario:
            try:
                campos_formulario = json.loads(formulario)
            except json.JSONDecodeError:
                pass  # Ignora se vier mal formatado

        # 1. VERIFICAR CACHE
        cached_data = get_cached_result(image_bytes, filename)
        if cached_data and cached_data.get("text"):
            # Passamos os campos do formulário para o extrator
            dados_extraidos = extract_angolan_id_data(
                cached_data.get("text", ""), campos_formulario
            )
            return {
                "success": True,
                "text": cached_data.get("text", ""),
                "lines": cached_data.get("lines", []),
                "file": cached_data.get("file"),
                "extracted_data": dados_extraidos,
            }

        # 2. PROCESSAR IMAGEM SE NÃO ESTIVER NA CACHE
        result = analyze_image(image_bytes)

        if isinstance(result, dict) and result.get("error"):
            raise HTTPException(
                status_code=422,
                detail={"error": result.get("error"), "message": result.get("message")},
            )

        # 3. GUARDAR NA CACHE E EXTRAIR DADOS
        set_cached_result(image_bytes, filename, result)

        # Passamos os campos do formulário para o extrator
        dados_extraidos = extract_angolan_id_data(
            result.get("text", ""), campos_formulario
        )

        return {
            "success": True,
            "text": result.get("text", ""),
            "lines": result.get("lines", []),
            "file": result.get("file"),
            "extracted_data": dados_extraidos,
        }

    except HTTPException:
        raise
    except Exception as e:
        error_trace = traceback.format_exc()
        log_message(
            f"Erro na rota analyze-image: {str(e)}\n{error_trace}", level="error"
        )
        raise HTTPException(
            status_code=500, detail={"error": type(e).__name__, "message": str(e)}
        )


# ==========================================
# ROTA 2: PDF (Streaming)
# ==========================================
@router.post("/analyze-pdf")
async def analyze_pdf_route(
    pdf: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        if pdf.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="Arquivo não é um PDF válido.")

        file_bytes = await pdf.read()

        if not file_bytes:
            raise HTTPException(
                status_code=400, detail="Arquivo PDF vazio ou corrompido."
            )

        filename = pdf.filename or "documento.pdf"

        # 1. VERIFICAR CACHE
        cached_data = None  # get_cached_result(file_bytes, filename)
        if cached_data and cached_data.get("text"):
            # Se encontrou, devolvemos o stream artificial a partir da cache
            return StreamingResponse(
                stream_from_cache(cached_data["text"]),
                media_type="text/plain; charset=utf-8",
            )

        # 2. SE NÃO ESTIVER NA CACHE, FAZ O STREAM NORMAL MAS INTERCEPTA PARA GUARDAR
        async def cached_pdf_generator():
            full_text_array = []

            # Executa o gerador original
            async for chunk in generate_pdf_text(file_bytes):
                full_text_array.append(chunk)
                yield chunk

            # No fim do stream, junta tudo e guarda na cache
            final_text = "".join(full_text_array)

            result_dict = {
                "text": final_text,
                "lines": final_text.splitlines(),
                "file": None,
            }
            set_cached_result(file_bytes, filename, result_dict)

        return StreamingResponse(
            cached_pdf_generator(), media_type="text/plain; charset=utf-8"
        )

    except HTTPException:
        raise
    except Exception as e:
        error_trace = traceback.format_exc()
        log_message(f"Erro na rota analyze-pdf: {str(e)}\n{error_trace}", level="error")
        raise HTTPException(
            status_code=500, detail="Erro interno ao tentar processar o PDF."
        )


# ==========================================
# ROTA 3: PLANILHA (Streaming)
# ==========================================
@router.post("/analyze-spreadsheet")
async def analyze_spreadsheet_route(
    spreadsheet: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    valid_types = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "text/csv",
    }

    try:
        filename = spreadsheet.filename or ""

        if (
            spreadsheet.content_type not in valid_types
            and not filename.lower().endswith((".xls", ".xlsx", ".csv"))
        ):
            raise HTTPException(
                status_code=400, detail="Arquivo não é uma planilha válida."
            )

        file_bytes = await spreadsheet.read()

        if not file_bytes:
            raise HTTPException(status_code=400, detail="Planilha vazia ou corrompida.")

        # 1. VERIFICAR CACHE
        cached_data = get_cached_result(file_bytes, filename)
        if cached_data and cached_data.get("text"):
            # Se encontrou, devolvemos o stream artificial a partir da cache
            return StreamingResponse(
                stream_from_cache(cached_data["text"]),
                media_type="text/plain; charset=utf-8",
            )

        # 2. SE NÃO ESTIVER NA CACHE, FAZ O STREAM NORMAL MAS INTERCEPTA PARA GUARDAR
        async def cached_spreadsheet_generator():
            full_text_array = []

            # Executa o gerador original
            async for chunk in generate_spreadsheet_text(file_bytes, filename):
                full_text_array.append(chunk)
                yield chunk

            # No fim do stream, junta tudo e guarda na cache
            final_text = "".join(full_text_array)

            result_dict = {
                "text": final_text,
                "lines": final_text.splitlines(),
                "file": None,
            }
            set_cached_result(file_bytes, filename, result_dict)

        return StreamingResponse(
            cached_spreadsheet_generator(),
            media_type="text/plain; charset=utf-8",
        )

    except HTTPException:
        raise
    except Exception as e:
        error_trace = traceback.format_exc()
        log_message(
            f"Erro na rota analyze-spreadsheet: {str(e)}\n{error_trace}", level="error"
        )
        raise HTTPException(
            status_code=500, detail="Erro interno ao tentar processar a planilha."
        )
