import traceback
import asyncio
import json
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks, status
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.ocr._OCR_CACHE import get_cached_result, set_cached_result
from app.services.ocr.analyze_pdf_spreadsheet import (
    generate_pdf_text,
    generate_spreadsheet_text,
)
from app.services.ocr.newpaddleORC import analyze_image
from app.services.ocr.ocr_job_manager import ocr_job_manager, OCRJobStatus
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


def _processar_ocr_em_background(
    job_id: str,
    image_bytes: bytes,
    filename: str,
    campos_formulario: list,
) -> None:
    """
    Worker desacoplado em background para processamento de OCR (FinOps - Compute).
    Executa a inferência de PaddleOCR sem prender conexões HTTP síncronas.
    """
    try:
        log_message(f"⚙️ [OCR Background] Iniciando processamento do job {job_id} ({filename})")
        result = analyze_image(image_bytes)

        if isinstance(result, dict) and result.get("error"):
            ocr_job_manager.update_job_failure(
                job_id, result.get("message") or "Erro na inferência do OCR"
            )
            return

        set_cached_result(image_bytes, filename, result)
        dados_extraidos = extract_angolan_id_data(
            result.get("text", ""), campos_formulario
        )

        payload_final = {
            "success": True,
            "text": result.get("text", ""),
            "lines": result.get("lines", []),
            "file": result.get("file"),
            "extracted_data": dados_extraidos,
        }
        ocr_job_manager.update_job_success(job_id, payload_final)
        log_message(f"✅ [OCR Background] Job {job_id} finalizado com sucesso.")

    except Exception as exc:
        trace = traceback.format_exc()
        log_message(f"❌ [OCR Background] Erro no job {job_id}: {exc}\n{trace}", "error")
        ocr_job_manager.update_job_failure(job_id, str(exc))


# ==========================================
# ROTA 1A: IMAGEM ASSÍNCRONA (Background Tasks)
# ==========================================
@router.post("/analyze-image-async", status_code=status.HTTP_202_ACCEPTED)
async def analyze_image_async_route(
    background_tasks: BackgroundTasks,
    image: UploadFile = File(...),
    formulario: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    ⚡ Execução Desacoplada de OCR:
    Agenda a inferência pesada na CPU em segundo plano, liberando a API imediatamente.
    O cliente recebe um `job_id` para consulta posterior em GET /ocr/jobs/{job_id}.
    """
    try:
        if not image.content_type or not image.content_type.startswith("image/"):
            raise HTTPException(
                status_code=400, detail="Arquivo enviado não é uma imagem válida."
            )

        image_bytes = await image.read()
        if not image_bytes:
            raise HTTPException(status_code=400, detail="Imagem vazia ou corrompida.")

        filename = image.filename or "imagem.jpg"
        campos_formulario = []
        if formulario:
            try:
                campos_formulario = json.loads(formulario)
            except json.JSONDecodeError:
                pass

        # 1. Se já estiver em cache, devolve imediatamente
        cached_data = get_cached_result(image_bytes, filename)
        if cached_data and cached_data.get("text"):
            dados_extraidos = extract_angolan_id_data(
                cached_data.get("text", ""), campos_formulario
            )
            return {
                "success": True,
                "cached": True,
                "status": OCRJobStatus.COMPLETED,
                "text": cached_data.get("text", ""),
                "lines": cached_data.get("lines", []),
                "file": cached_data.get("file"),
                "extracted_data": dados_extraidos,
            }

        # 2. Cria o Job e despacha para BackgroundTasks (FinOps Compute)
        job_id = ocr_job_manager.create_job(filename)
        background_tasks.add_task(
            _processar_ocr_em_background,
            job_id,
            image_bytes,
            filename,
            campos_formulario,
        )

        return {
            "success": True,
            "cached": False,
            "job_id": job_id,
            "status": OCRJobStatus.PROCESSING,
            "message": "Processamento de OCR agendado em background com sucesso.",
            "check_status_url": f"/ocr/jobs/{job_id}",
        }

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"Erro ao agendar analyze-image-async: {e}", level="error")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ROTA 1B: CONSULTA DE STATUS DO JOB DE OCR
# ==========================================
@router.get("/jobs/{job_id}")
async def get_ocr_job_status(job_id: str, user_id: int = Depends(get_current_user_id)):
    """Consulta o progresso e o resultado de uma tarefa de OCR assíncrona."""
    job = ocr_job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job de OCR não encontrado ou expirado.")

    if job["status"] == OCRJobStatus.COMPLETED:
        return {"success": True, **job}
    elif job["status"] == OCRJobStatus.FAILED:
        return JSONResponse(
            status_code=422,
            content={"success": False, **job},
        )
    return {"success": True, **job}


# ==========================================
# ROTA 1: IMAGEM (Síncrona Otimizada com ThreadPool)
# ==========================================
@router.post("/analyze-image")
async def analyze_image_route(
    image: UploadFile = File(...),
    formulario: Optional[str] = Form(None),
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
        campos_formulario = []
        if formulario:
            try:
                campos_formulario = json.loads(formulario)
            except json.JSONDecodeError:
                pass

        # 1. VERIFICAR CACHE
        cached_data = get_cached_result(image_bytes, filename)
        if cached_data and cached_data.get("text"):
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

        # 2. PROCESSAR IMAGEM FORA DO EVENT LOOP (run_in_threadpool previne saturação da API)
        result = await run_in_threadpool(analyze_image, image_bytes)

        if isinstance(result, dict) and result.get("error"):
            raise HTTPException(
                status_code=422,
                detail={"error": result.get("error"), "message": result.get("message")},
            )

        # 3. GUARDAR NA CACHE E EXTRAIR DADOS
        set_cached_result(image_bytes, filename, result)
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
        cached_data = get_cached_result(file_bytes, filename)
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
