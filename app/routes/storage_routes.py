import traceback

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import os
from datetime import date as dt_date

from app.cruds.user_crud import get_user_by_id
from app.models.clouds_models import (
    ALLOWED_EXTENSIONS,
    FileModel,
    LogAction,
    LogCloud,
    LogStatus,
    NetworkMetric,
    RequestUsage,
    StorageUsage,
)
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from cloud.config import StorageService
from app.database import get_db

router = APIRouter(prefix="/storage", tags=["Storage"])
storage = StorageService()

MAX_FILE_SIZE = 10 * 1024 * 1024


# 🔒 utils
def validate_filename(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Tipo inválido")


async def validate_file_size(file: UploadFile):
    content = await file.read()
    size = len(content)

    if size > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="Ficheiro muito grande")

    file.file.seek(0)
    return size


# 🧠 helpers métricas
def check_request_limit(db, user_id):
    today = dt_date.today()
    user = get_user_by_id(
        db, user_id
    )  # Garante que o usuário existe antes de verificar o storage
    usage = db.query(RequestUsage).filter_by(user_id=user_id, date=today).first()

    if not usage:
        usage = RequestUsage(user_id=user_id, date=today, request_count=0)
        db.add(usage)

    if usage.request_count >= user.plan.max_requests_per_day:
        raise HTTPException(status_code=429, detail="Limite de requests atingido")

    usage.request_count += 1
    db.commit()


def check_storage_limit(db, user_id, file_size):
    user = get_user_by_id(db, user_id)
    usage = db.query(StorageUsage).filter_by(user_id=user_id).first()

    if not usage:
        usage = StorageUsage(user_id=user_id, used_bytes=0)
        db.add(usage)

    max_bytes = user.plan.max_storage_mb * 1024 * 1024

    if usage.used_bytes + file_size > max_bytes:
        raise HTTPException(status_code=403, detail="Limite de storage atingido")

    usage.used_bytes += file_size
    db.commit()


def add_network(db, user_id, ingress=0, egress=0):
    today = dt_date.today()

    metric = db.query(NetworkMetric).filter_by(user_id=user_id, date=today).first()

    if not metric:
        metric = NetworkMetric(user_id=user_id, date=today)
        db.add(metric)

    metric.ingress_bytes += ingress
    metric.egress_bytes += egress

    db.commit()


def log_db(db, user_id, action, filename, status, message=None):
    log = LogCloud(
        user_id=int(user_id),
        action=action,
        filename=filename,
        status=status,
        message=message,
    )
    db.add(log)
    db.commit()


def ok(data=None, message="Sucesso"):
    return {"success": True, "message": message, "data": data}


def fail(message="Erro", code=400):
    raise HTTPException(status_code=code, detail=message)


# 📤 UPLOAD
@router.post("/upload")
async def upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    filename = file.filename if file else "unknown"

    try:
        if not file:
            fail("Ficheiro não enviado")

        validate_filename(filename)
        size = await validate_file_size(file)

        check_request_limit(db, user_id)
        check_storage_limit(db, user_id, size)

        storage.upload_file(file.file, filename)

        db.add(
            FileModel(
                user_id=user_id,
                filename=filename,
                path=filename,
                size_bytes=size,
            )
        )
        db.commit()

        add_network(db, user_id, ingress=size)

        log_db(db, user_id, LogAction.UPLOAD, filename, LogStatus.SUCCESS)

        return ok({"file": filename, "size": size}, "Upload realizado")

    except HTTPException as e:
        db.rollback()
        log_message(
            message=f"Erro no upload: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.UPLOAD, filename, LogStatus.ERROR, str(e.detail))
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro interno no upload: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.UPLOAD, filename, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro interno no upload")


from fastapi import Query


@router.get("/filespage")
async def list_files_page(
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    try:
        check_request_limit(db, user_id)

        offset = (page - 1) * limit

        query = db.query(FileModel).filter_by(user_id=user_id, is_deleted=False)

        total = query.count()

        files = (
            query.order_by(FileModel.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

        data = [
            {
                "id": str(f.id),
                "filename": f.filename,
                "size_bytes": f.size_bytes,
                "mime_type": f.mime_type,
                "created_at": f.created_at,
            }
            for f in files
        ]

        return ok(
            {
                "items": data,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "pages": (total + limit - 1) // limit,
                },
            }
        )

    except Exception as e:
        log_message(
            message=f"Erro ao listar ficheiros: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, None, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao listar ficheiros")


# 📂 LIST
@router.get("/files")
def list_files(
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        check_request_limit(db, user_id)

        files = db.query(FileModel).filter_by(user_id=user_id).all()

        return ok([f.filename for f in files])

    except Exception as e:
        log_message(
            message=f"Erro ao listar ficheiros: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, None, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao listar ficheiros")


# 🗑️ DELETE
@router.delete("/delete/{filename}")
def delete_file(
    filename: str,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = db.query(FileModel).filter_by(user_id=user_id, filename=filename).first()

        if not file:
            fail("Ficheiro não encontrado", 404)

        storage.delete_file(filename)

        usage = db.query(StorageUsage).filter_by(user_id=user_id).first()
        if usage:
            usage.used_bytes = max(0, usage.used_bytes - file.size_bytes)

        db.delete(file)
        db.commit()

        log_db(db, user_id, LogAction.DELETE, filename, LogStatus.SUCCESS)

        return ok(message="Ficheiro removido")

    except HTTPException as e:
        db.rollback()
        log_message(
            message=f"Erro ao apagar ficheiro: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DELETE, filename, LogStatus.ERROR, str(e.detail))
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao apagar ficheiro: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DELETE, filename, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao apagar ficheiro")


# 🔗 URL (download)
@router.get("/url/{filename}")
def get_url(
    filename: str,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
    expires: int = 3600,
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = db.query(FileModel).filter_by(user_id=user_id, filename=filename).first()

        if not file:
            fail("Ficheiro não encontrado", 404)

        url = storage.generate_url(filename, expires)

        add_network(db, user_id, egress=file.size_bytes)

        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.SUCCESS)

        return ok({"url": url, "expires": expires})

    except HTTPException as e:
        log_message(
            message=str(e), level="error", source="storage_routes.py", user=user_id
        )
        log_db(
            db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, str(e.detail)
        )
        raise

    except Exception as e:
        log_message(
            message=f"Erro ao gerar URL: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao gerar URL")


@router.get("/download/{filename}")
def download_file(
    filename: str,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = db.query(FileModel).filter_by(user_id=user_id, filename=filename).first()

        if not file:
            fail("Ficheiro não encontrado", 404)

        stream = storage.get_file_stream(filename)

        add_network(db, user_id, egress=file.size_bytes)
        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.SUCCESS)

        return StreamingResponse(
            stream,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(file.size_bytes),  # 🔥 MUITO IMPORTANTE
            },
        )

    except HTTPException as e:
        db.rollback()
        log_message(
            message=str(e), level="error", source="storage_routes.py", user=user_id
        )
        log_db(
            db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, str(e.detail)
        )
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao baixar ficheiro: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro no download")


@router.get("/stats")
def get_stats(
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        user = get_user_by_id(db, user_id)
        storage_data = db.query(StorageUsage).filter_by(user_id=user_id).first()
        requests_data = db.query(RequestUsage).filter_by(user_id=user_id).first()
        network = db.query(NetworkMetric).filter_by(user_id=user_id).first()

        return ok(
            {
                "plan": {
                    "name": user.plan.name,
                    "max_storage_mb": user.plan.max_storage_mb,
                    "max_requests": user.plan.max_requests_per_day,
                },
                "usage": {
                    "storage_bytes": storage_data.used_bytes if storage_data else 0,
                    "requests": requests_data.request_count if requests_data else 0,
                    "ingress": network.ingress_bytes if network else 0,
                    "egress": network.egress_bytes if network else 0,
                },
            }
        )

    except Exception as e:
        log_message(
            message=f"Erro ao obter métricas: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, None, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao obter métricas")
