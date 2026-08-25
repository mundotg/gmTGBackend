import mimetypes
import traceback
import unicodedata
import uuid
from urllib.parse import quote

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session
import os
from datetime import date as dt_date

from typing import List, Optional
from uuid import UUID

from app.cruds.user_crud import get_user_by_id
from app.models.clouds_models import (
    ALLOWED_EXTENSIONS,
    FileModel,
    LogAction,
    LogCloud,
    LogStatus,
    NetworkMetric,
    RequestUsage,
    StorageFolder,
    StorageUsage,
)
from app.schemas.storage_folder_schema import (
    FileMove,
    FolderCreate,
    FolderOut,
    FolderPasswordUpdate,
    FolderTreeNode,
    FolderUnlock,
    FolderUpdate,
)
from app.services import storage_folder_service as folder_service
from app.config.dotenv import get_env_int
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from cloud.config import StorageService
from app.config.redis import read_cache, write_cache, delete_cache
from app.database import get_db

router = APIRouter(prefix="/storage", tags=["Storage"])
storage = StorageService()

# Limite por ficheiro, configurável. Estava fixo em 10 MB no código, o que dava
# 413 em qualquer upload maior sem forma de ajustar.
MAX_FILE_MB = get_env_int("STORAGE_MAX_FILE_MB", 100)
MAX_FILE_SIZE = MAX_FILE_MB * 1024 * 1024

# ── Cache Redis (desempenho): stats e listagem por utilizador ──
# A listagem é paginada; usa-se uma "versão" por utilizador para invalidar
# TODAS as páginas de uma vez (bump em upload/delete), em vez de apagar chaves
# uma a uma.
STATS_TTL = 30
FILES_TTL = 20


def _stats_key(user_id) -> str:
    return f"storage:stats:{user_id}"


def _files_ver(user_id) -> int:
    try:
        return int(read_cache(f"storage:ver:{user_id}") or 0)
    except Exception:  # noqa: BLE001
        return 0


def _files_key(user_id, page, limit, folder_id=None) -> str:
    escopo = folder_id or "root"
    return f"storage:files:{user_id}:v{_files_ver(user_id)}:{escopo}:{page}:{limit}"


def invalidate_storage_cache(user_id) -> None:
    """Invalida stats + todas as páginas da listagem (via bump de versão)."""
    try:
        delete_cache(_stats_key(user_id))
        write_cache(f"storage:ver:{user_id}", _files_ver(user_id) + 1)
    except Exception as e:  # noqa: BLE001
        log_message(f"[STORAGE] falha ao invalidar cache: {e}", "warning")


# 🔒 utils
def validate_filename(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Tipo inválido")


def build_object_key(user_id, filename: str) -> str:
    """Chave do objeto no bucket: `{user_id}/{uuid}.ext`.

    O nome original vai para a coluna `filename` (só para exibição). Guardar o
    nome cru como chave fazia o upload de um utilizador **sobrescrever** o
    ficheiro homónimo de outro — o namespace do bucket é global.
    """
    ext = os.path.splitext(filename)[1].lower()
    return f"{int(user_id)}/{uuid.uuid4().hex}{ext}"


def content_disposition(filename: str) -> str:
    """Cabeçalho com o nome do ficheiro em ASCII + UTF-8 (RFC 5987).

    O Starlette codifica cabeçalhos em latin-1: um nome com emoji, travessão ou
    aspas curvas rebentava o download com 500 antes de enviar um único byte.
    """
    ascii_name = (
        unicodedata.normalize("NFKD", filename)
        .encode("ascii", "ignore")
        .decode("ascii")
        .replace('"', "")
        .strip()
    )

    # nome inteiramente não-ASCII (ex.: 日本語.csv) sobraria como ".csv"
    stem, ext = os.path.splitext(ascii_name)
    if not stem.strip():
        ascii_name = f"download{ext}"

    return (
        f'attachment; filename="{ascii_name}"; '
        f"filename*=UTF-8''{quote(filename, safe='')}"
    )


def get_owned_file(
    db: Session,
    user_id,
    filename: str,
    folder_id=None,
) -> FileModel:
    """Ficheiro ativo do utilizador, ou 404.

    É aqui que a posse é garantida — e também que a senha da pasta é exigida,
    para que nenhuma rota de conteúdo possa esquecer-se de a verificar.
    """
    query = db.query(FileModel).filter_by(
        user_id=user_id, filename=filename, is_deleted=False
    )
    if folder_id is not None:
        query = query.filter(FileModel.folder_id == folder_id)

    file = query.order_by(FileModel.created_at.desc()).first()

    if not file:
        fail("Ficheiro não encontrado", 404)

    folder_service.assert_file_access(db, user_id, file)
    return file


GENERIC_TYPES = {"", "binary/octet-stream", "application/octet-stream"}


def resolve_content_type(file: FileModel, storage_type) -> str:
    """Melhor tipo disponível: storage → BD → extensão do nome original.

    Objetos enviados antes de o upload gravar o `ContentType` voltam do MinIO
    como `binary/octet-stream`; nesses casos o nome do ficheiro sabe mais.
    """
    if storage_type and storage_type.lower() not in GENERIC_TYPES:
        return storage_type

    return (
        file.mime_type
        or mimetypes.guess_type(file.filename)[0]
        or "application/octet-stream"
    )


def get_plan(user):
    """Plano do utilizador, com erro de negócio em vez de AttributeError/500."""
    if user is None:
        raise HTTPException(status_code=404, detail="Utilizador não encontrado")

    if user.plan is None:
        raise HTTPException(
            status_code=403, detail="Utilizador sem plano associado"
        )

    return user.plan


async def validate_file_size(file: UploadFile) -> int:
    """Tamanho do upload sem o carregar para memória.

    A versão anterior fazia `await file.read()` só para medir: um ficheiro de
    500 MB passava a ocupar 500 MB de RAM (e outra vez no upload). O Starlette
    já escreve o corpo num ficheiro temporário, portanto basta procurar o fim.
    """
    f = file.file
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)

    if size == 0:
        raise HTTPException(status_code=400, detail="Ficheiro vazio")

    if size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"Ficheiro muito grande: máximo {MAX_FILE_MB} MB por ficheiro.",
        )

    return size


# 🧠 helpers métricas
def check_request_limit(db, user_id):
    today = dt_date.today()
    user = get_user_by_id(
        db, user_id
    )  # Garante que o usuário existe antes de verificar o storage
    plan = get_plan(user)
    usage = db.query(RequestUsage).filter_by(user_id=user_id, date=today).first()

    if not usage:
        usage = RequestUsage(user_id=user_id, date=today, request_count=0)
        db.add(usage)

    if usage.request_count >= plan.max_requests_per_day:
        db.rollback()  # não deixa a linha nova pendurada na sessão
        raise HTTPException(status_code=429, detail="Limite de requests atingido")

    usage.request_count += 1
    db.commit()


def assert_storage_limit(db, user_id, file_size):
    """Valida a quota **sem** a debitar — ver `credit_storage`."""
    user = get_user_by_id(db, user_id)
    plan = get_plan(user)
    usage = db.query(StorageUsage).filter_by(user_id=user_id).first()

    used = usage.used_bytes if usage else 0
    max_bytes = plan.max_storage_mb * 1024 * 1024

    if used + file_size > max_bytes:
        raise HTTPException(status_code=403, detail="Limite de storage atingido")


def credit_storage(db, user_id, file_size):
    """Debita a quota **depois** do upload ter sucesso, sem commit próprio.

    A versão anterior debitava e fazia commit antes do upload: se o upload
    falhasse, o `rollback` do handler não desfazia o commit já feito e a quota
    ficava inflacionada para sempre.
    """
    usage = db.query(StorageUsage).filter_by(user_id=user_id).first()

    if not usage:
        usage = StorageUsage(user_id=user_id, used_bytes=0)
        db.add(usage)

    usage.used_bytes += file_size


def resolve_target_folder(db: Session, user_id, folder_id) -> Optional[StorageFolder]:
    """Pasta de destino validada e desbloqueada, ou None para a raiz."""
    if not folder_id:
        return None

    folder = folder_service.get_folder_or_404(db, user_id, folder_id)
    folder_service.assert_folder_access(db, user_id, folder)
    return folder


def add_network(db, user_id, ingress=0, egress=0):
    today = dt_date.today()

    metric = db.query(NetworkMetric).filter_by(user_id=user_id, date=today).first()

    if not metric:
        # Os contadores têm de vir a 0 explícitos: `default=0` na coluna só é
        # aplicado no INSERT, e aqui somamos ao objeto antes do flush — ficava
        # None e rebentava com TypeError no primeiro upload de cada dia.
        metric = NetworkMetric(
            user_id=user_id, date=today, ingress_bytes=0, egress_bytes=0
        )
        db.add(metric)

    metric.ingress_bytes = (metric.ingress_bytes or 0) + ingress
    metric.egress_bytes = (metric.egress_bytes or 0) + egress

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
    folder_id: Optional[UUID] = Query(
        None, description="Pasta de destino; omitir envia para a raiz."
    ),
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
        assert_storage_limit(db, user_id, size)

        # Valida a pasta e exige que esteja desbloqueada.
        destino = resolve_target_folder(db, user_id, folder_id)

        key = build_object_key(user_id, filename)
        mime_type = file.content_type or mimetypes.guess_type(filename)[0]

        storage.upload_file(file.file, key, content_type=mime_type)

        credit_storage(db, user_id, size)

        db.add(
            FileModel(
                user_id=user_id,
                folder_id=destino.id if destino else None,
                filename=filename,
                path=key,
                size_bytes=size,
                mime_type=mime_type,
            )
        )
        db.commit()

        add_network(db, user_id, ingress=size)

        log_db(db, user_id, LogAction.UPLOAD, filename, LogStatus.SUCCESS)
        invalidate_storage_cache(user_id)  # listagem/stats mudaram

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


@router.get("/filespage")
async def list_files_page(
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    folder_id: Optional[UUID] = Query(
        None, description="Pasta a listar; omitir lista a raiz."
    ),
):
    try:
        check_request_limit(db, user_id)

        # Listar dentro de uma pasta bloqueada devolve 423 (e não a lista).
        resolve_target_folder(db, user_id, folder_id)

        cache_key = _files_key(user_id, page, limit, folder_id)
        cached = read_cache(cache_key)
        if cached:
            return ok(cached)

        offset = (page - 1) * limit

        query = db.query(FileModel).filter_by(
            user_id=user_id, is_deleted=False, folder_id=folder_id
        )

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
                "created_at": f.created_at.isoformat() if f.created_at else None,
            }
            for f in files
        ]

        payload = {
            "items": data,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": (total + limit - 1) // limit,
            },
        }
        write_cache(cache_key, payload, ttl=FILES_TTL)
        return ok(payload)

    except HTTPException:
        # Sem isto, o 423 da pasta bloqueada (e o 429 do limite de pedidos)
        # eram apanhados abaixo e devolvidos como 500.
        raise

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

        files = (
            db.query(FileModel).filter_by(user_id=user_id, is_deleted=False).all()
        )
        acessiveis = folder_service.accessible_folder_ids(db, user_id)

        # Nesta listagem plana, ficheiros em pastas bloqueadas ficam de fora —
        # nem o nome deve escapar antes de a senha ser dada.
        return ok(
            [
                f.filename
                for f in files
                if f.folder_id is None or f.folder_id in acessiveis
            ]
        )

    except HTTPException:
        raise

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
    folder_id: Optional[UUID] = Query(
        None, description="Desambigua nomes repetidos em pastas diferentes."
    ),
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = get_owned_file(db, user_id, filename, folder_id)

        # O objeto só sai do bucket se mais nenhum registo ativo o referenciar
        # (ex.: uma migração que apontou duas linhas para a mesma key legada).
        outros = (
            db.query(FileModel)
            .filter(
                FileModel.path == file.path,
                FileModel.id != file.id,
                FileModel.is_deleted.is_(False),
            )
            .count()
        )

        if not outros:
            storage.delete_file(file.path)

        usage = db.query(StorageUsage).filter_by(user_id=user_id).first()
        if usage:
            usage.used_bytes = max(0, usage.used_bytes - file.size_bytes)

        file.is_deleted = True  # soft delete, coerente com a coluna do modelo
        db.commit()

        log_db(db, user_id, LogAction.DELETE, filename, LogStatus.SUCCESS)
        invalidate_storage_cache(user_id)  # listagem/stats mudaram

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
    request: Request,
    folder_id: Optional[UUID] = Query(
        None, description="Desambigua nomes repetidos em pastas diferentes."
    ),
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
    expires: int = 3600,
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = get_owned_file(db, user_id, filename, folder_id)

        # Se o objeto só existir no cache local, devolve a rota autenticada de
        # download em vez de uma URL pré-assinada que daria 404 no browser.
        fallback_url = (
            f"{str(request.base_url).rstrip('/')}"
            f"/storage/download/{quote(file.filename, safe='')}"
        )

        url = storage.generate_url(file.path, expires, fallback_url=fallback_url)

        add_network(db, user_id, egress=file.size_bytes)

        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.SUCCESS)

        return ok({"url": url, "expires": expires})

    except FileNotFoundError:
        log_db(
            db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, "não encontrado"
        )
        raise HTTPException(404, "Ficheiro não encontrado no storage")

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
    folder_id: Optional[UUID] = Query(
        None, description="Desambigua nomes repetidos em pastas diferentes."
    ),
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        validate_filename(filename)
        check_request_limit(db, user_id)

        file = get_owned_file(db, user_id, filename, folder_id)

        # tamanho e tipo vêm da fonte real (bucket ou cache), não da BD
        stream, size, content_type = storage.get_file_stream(file.path)

        add_network(db, user_id, egress=size or file.size_bytes)
        log_db(db, user_id, LogAction.DOWNLOAD, filename, LogStatus.SUCCESS)

        headers = {"Content-Disposition": content_disposition(file.filename)}
        if size:
            headers["Content-Length"] = str(size)

        return StreamingResponse(
            stream,
            media_type=resolve_content_type(file, content_type),
            headers=headers,
        )

    except FileNotFoundError:
        db.rollback()
        log_db(
            db, user_id, LogAction.DOWNLOAD, filename, LogStatus.ERROR, "não encontrado"
        )
        raise HTTPException(404, "Ficheiro não encontrado no storage")

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
        cached = read_cache(_stats_key(user_id))
        if cached:
            return ok(cached)

        user = get_user_by_id(db, user_id)
        storage_data = db.query(StorageUsage).filter_by(user_id=user_id).first()
        requests_data = db.query(RequestUsage).filter_by(user_id=user_id).first()
        network = db.query(NetworkMetric).filter_by(user_id=user_id).first()

        payload = {
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
        write_cache(_stats_key(user_id), payload, ttl=STATS_TTL)
        return ok(payload)

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            message=f"Erro ao obter métricas: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        log_db(db, user_id, LogAction.DOWNLOAD, None, LogStatus.ERROR, str(e))
        raise HTTPException(500, "Erro ao obter métricas")


@router.patch("/files/{file_id}/move", response_model=None)
def move_file(
    file_id: UUID,
    payload: FileMove,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    """Move um ficheiro entre pastas (usado pelo arrastar-e-largar).

    Exige acesso à pasta de **origem** e à de **destino**: arrastar não pode ser
    um atalho para tirar um ficheiro de dentro de uma pasta protegida, nem para
    o meter numa sem dar a senha.

    Só mexe em metadados — a chave no bucket não muda.
    """
    try:
        check_request_limit(db, user_id)

        file = (
            db.query(FileModel)
            .filter_by(id=file_id, user_id=user_id, is_deleted=False)
            .first()
        )
        if not file:
            fail("Ficheiro não encontrado", 404)

        folder_service.assert_file_access(db, user_id, file)  # origem

        destino = (
            None
            if payload.move_to_root
            else resolve_target_folder(db, user_id, payload.folder_id)
        )

        if (destino.id if destino else None) == file.folder_id:
            return ok({"moved": False}, "O ficheiro já está nessa pasta")

        file.folder_id = destino.id if destino else None
        db.commit()

        invalidate_storage_cache(user_id)
        return ok(
            {"moved": True, "folder_id": str(destino.id) if destino else None},
            f"'{file.filename}' movido para {destino.name if destino else 'a raiz'}",
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao mover ficheiro: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao mover ficheiro")


# ══════════════════════════════ 📁 PASTAS ══════════════════════════════
# A senha de uma pasta protege toda a subárvore. O desbloqueio dura 30 min
# (Redis) e é revogado assim que a senha muda — ver storage_folder_service.


@router.post("/folders", response_model=None)
def create_folder(
    payload: FolderCreate,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        check_request_limit(db, user_id)

        # Criar dentro de uma pasta bloqueada exige tê-la desbloqueado.
        resolve_target_folder(db, user_id, payload.parent_id)

        folder_service.assert_depth_ok(db, payload.parent_id)
        folder_service.assert_name_free(
            db, user_id, payload.parent_id, payload.name
        )

        folder = StorageFolder(
            user_id=user_id,
            parent_id=payload.parent_id,
            name=payload.name,
        )

        if payload.password:
            folder_service.set_password(db, folder, payload.password)

        db.add(folder)
        db.commit()
        db.refresh(folder)

        # Quem cria a pasta com senha não deve ter de a escrever logo a seguir.
        if folder.is_locked:
            folder_service.mark_unlocked(user_id, folder.id)

        invalidate_storage_cache(user_id)
        return ok(FolderOut.model_validate(folder).model_dump(mode="json"), "Pasta criada")

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao criar pasta: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao criar pasta")


@router.get("/folders", response_model=None)
def list_folders(
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    """Árvore completa. Pastas bloqueadas vêm sem filhos nem contagem —
    o cadeado é visível, o conteúdo não."""
    try:
        check_request_limit(db, user_id)

        pastas = (
            db.query(StorageFolder)
            .filter_by(user_id=user_id, is_deleted=False)
            .order_by(StorageFolder.name)
            .all()
        )
        acessiveis = folder_service.accessible_folder_ids(db, user_id)

        contagens = dict(
            db.query(FileModel.folder_id, func.count(FileModel.id))
            .filter(FileModel.user_id == user_id, FileModel.is_deleted.is_(False))
            .group_by(FileModel.folder_id)
            .all()
        )

        por_parent: dict = {}
        for f in pastas:
            por_parent.setdefault(f.parent_id, []).append(f)

        def _montar(parent_id, profundidade=0) -> List[FolderTreeNode]:
            if profundidade > folder_service.MAX_DEPTH:
                return []

            nos = []
            for f in por_parent.get(parent_id, []):
                aberta = f.id in acessiveis
                nos.append(
                    FolderTreeNode(
                        id=f.id,
                        name=f.name,
                        parent_id=f.parent_id,
                        is_locked=f.is_locked,
                        is_unlocked=aberta,
                        created_at=f.created_at,
                        children=_montar(f.id, profundidade + 1) if aberta else [],
                        file_count=contagens.get(f.id, 0) if aberta else 0,
                    )
                )
            return nos

        return ok(
            {
                "folders": [n.model_dump(mode="json") for n in _montar(None)],
                "root_file_count": contagens.get(None, 0),
            }
        )

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            message=f"Erro ao listar pastas: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao listar pastas")


@router.post("/folders/{folder_id}/unlock", response_model=None)
def unlock_folder(
    folder_id: UUID,
    payload: FolderUnlock,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        check_request_limit(db, user_id)
        folder = folder_service.get_folder_or_404(db, user_id, folder_id)

        # A cadeia acima tem de estar aberta ANTES de qualquer outra coisa:
        # caso contrário bastaria a senha da subpasta (ou o facto de ela não
        # ter senha) para contornar a proteção de cima.
        if folder.parent_id:
            pai = folder_service.get_folder_or_404(db, user_id, folder.parent_id)
            folder_service.assert_folder_access(db, user_id, pai)

        if not folder.is_locked:
            return ok({"unlocked": True}, "A pasta não tem senha")

        if not folder_service.check_password(folder, payload.password):
            log_message(
                f"⚠️ Senha errada na pasta '{folder.name}' (user {user_id})",
                "warning",
            )
            raise HTTPException(status_code=403, detail="Senha incorreta")

        folder_service.mark_unlocked(user_id, folder.id)
        return ok(
            {"unlocked": True, "expires_in": folder_service.UNLOCK_TTL},
            "Pasta desbloqueada",
        )

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            message=f"Erro ao desbloquear pasta: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao desbloquear pasta")


@router.post("/folders/{folder_id}/lock", response_model=None)
def lock_folder(
    folder_id: UUID,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    """Revoga o desbloqueio antes de expirar (útil em máquina partilhada)."""
    folder = folder_service.get_folder_or_404(db, user_id, folder_id)
    folder_service.clear_unlock(user_id, folder.id)
    return ok({"locked": True}, "Pasta bloqueada")


@router.put("/folders/{folder_id}/password", response_model=None)
def update_folder_password(
    folder_id: UUID,
    payload: FolderPasswordUpdate,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    try:
        check_request_limit(db, user_id)
        folder = folder_service.get_folder_or_404(db, user_id, folder_id)

        # Ter uma senha já definida obriga a provar que se sabe qual é.
        if folder.is_locked and not folder_service.check_password(
            folder, payload.current_password or ""
        ):
            raise HTTPException(status_code=403, detail="Senha atual incorreta")

        folder_service.set_password(db, folder, payload.new_password)
        db.commit()

        if folder.is_locked:
            folder_service.mark_unlocked(user_id, folder.id)

        return ok(
            {"is_locked": folder.is_locked},
            "Senha definida" if folder.is_locked else "Senha removida",
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao alterar senha da pasta: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao alterar a senha da pasta")


@router.patch("/folders/{folder_id}", response_model=None)
def update_folder(
    folder_id: UUID,
    payload: FolderUpdate,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    """Renomeia e/ou move a pasta."""
    try:
        check_request_limit(db, user_id)
        folder = folder_service.get_folder_or_404(db, user_id, folder_id)
        folder_service.assert_folder_access(db, user_id, folder)

        novo_parent = folder.parent_id
        if payload.move_to_root:
            novo_parent = None
        elif payload.parent_id is not None:
            resolve_target_folder(db, user_id, payload.parent_id)
            folder_service.assert_no_cycle(db, folder, payload.parent_id)
            folder_service.assert_depth_ok(db, payload.parent_id)
            novo_parent = payload.parent_id

        novo_nome = payload.name or folder.name

        if novo_nome != folder.name or novo_parent != folder.parent_id:
            folder_service.assert_name_free(
                db, user_id, novo_parent, novo_nome, ignore_id=folder.id
            )

        folder.name = novo_nome
        folder.parent_id = novo_parent
        db.commit()
        db.refresh(folder)

        invalidate_storage_cache(user_id)
        return ok(FolderOut.model_validate(folder).model_dump(mode="json"), "Pasta atualizada")

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao atualizar pasta: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao atualizar pasta")


@router.delete("/folders/{folder_id}", response_model=None)
def delete_folder(
    folder_id: UUID,
    db: Session = Depends(get_db),
    user_id=Depends(get_current_user_id),
):
    """Apaga a pasta e tudo abaixo (soft delete, tal como nos ficheiros).

    Os objetos ficam no bucket: a exclusão é reversível na BD e os ficheiros
    podem ser recuperados. Só o `DELETE /storage/delete/{filename}` remove
    mesmo do storage.
    """
    try:
        check_request_limit(db, user_id)
        folder = folder_service.get_folder_or_404(db, user_id, folder_id)
        folder_service.assert_folder_access(db, user_id, folder)

        ids = folder_service.descendant_ids(db, folder)

        ficheiros = (
            db.query(FileModel)
            .filter(
                FileModel.user_id == user_id,
                FileModel.folder_id.in_(ids),
                FileModel.is_deleted.is_(False),
            )
            .all()
        )

        usage = db.query(StorageUsage).filter_by(user_id=user_id).first()
        for f in ficheiros:
            f.is_deleted = True
            if usage:
                usage.used_bytes = max(0, usage.used_bytes - f.size_bytes)

        db.query(StorageFolder).filter(StorageFolder.id.in_(ids)).update(
            {"is_deleted": True}, synchronize_session=False
        )
        db.commit()

        for fid in ids:
            folder_service.clear_unlock(user_id, fid)

        invalidate_storage_cache(user_id)
        log_db(db, user_id, LogAction.DELETE, folder.name, LogStatus.SUCCESS)

        return ok(
            {"folders": len(ids), "files": len(ficheiros)},
            f"Pasta '{folder.name}' removida",
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        log_message(
            message=f"Erro ao apagar pasta: {str(e)}{traceback.format_exc()}",
            level="error",
            source="storage_routes.py",
            user=user_id,
        )
        raise HTTPException(500, "Erro ao apagar pasta")
