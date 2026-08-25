"""Pastas do storage: árvore, senha e regras de acesso.

A senha é **controlo de acesso**: guarda-se o hash bcrypt e valida-se a cada
pedido. O conteúdo não é cifrado, portanto uma senha esquecida não perde
ficheiros — o dono pode redefini-la.

Uma pasta com senha protege **toda a subárvore**: para tocar num ficheiro é
preciso ter desbloqueado todas as pastas com senha no caminho até à raiz. Isto
evita o buraco óbvio de proteger a pasta mas deixar a subpasta aberta.

O desbloqueio vive no Redis com TTL (`storage:unlock:{user}:{folder}`) em vez de
um cookie ou JWT: expira sozinho, e mudar a senha revoga-o de imediato.
"""

from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.auth import hash_password, verify_password
from app.config.redis import delete_cache, read_cache, write_cache
from app.models.clouds_models import FileModel, StorageFolder

# Tempo que um desbloqueio dura sem nova interação.
UNLOCK_TTL = 30 * 60

# Profundidade máxima da árvore — trava ciclos e aninhamentos absurdos.
MAX_DEPTH = 32


# ──────────────────────────── desbloqueio ────────────────────────────


def _unlock_key(user_id, folder_id) -> str:
    return f"storage:unlock:{user_id}:{folder_id}"


def mark_unlocked(user_id, folder_id) -> None:
    write_cache(_unlock_key(user_id, folder_id), True, ttl=UNLOCK_TTL)


def clear_unlock(user_id, folder_id) -> None:
    delete_cache(_unlock_key(user_id, folder_id))


def is_unlocked(user_id, folder_id) -> bool:
    """Falha **fechada**: sem Redis, a pasta fica bloqueada.

    `read_cache` engole os erros e devolve None, portanto uma falha de cache
    nega o acesso em vez de o conceder.
    """
    return bool(read_cache(_unlock_key(user_id, folder_id)))


# ──────────────────────────── árvore ────────────────────────────


def get_folder_or_404(db: Session, user_id, folder_id) -> StorageFolder:
    folder = (
        db.query(StorageFolder)
        .filter_by(id=folder_id, user_id=user_id, is_deleted=False)
        .first()
    )
    if not folder:
        raise HTTPException(status_code=404, detail="Pasta não encontrada")
    return folder


def get_ancestors(db: Session, folder: StorageFolder) -> List[StorageFolder]:
    """Da pasta até à raiz, inclusive. Protegido contra ciclos."""
    cadeia: List[StorageFolder] = []
    atual: Optional[StorageFolder] = folder
    vistos = set()

    while atual is not None and len(cadeia) < MAX_DEPTH:
        if atual.id in vistos:  # ciclo: para em vez de rodar para sempre
            break
        vistos.add(atual.id)
        cadeia.append(atual)

        if atual.parent_id is None:
            break
        atual = (
            db.query(StorageFolder)
            .filter_by(id=atual.parent_id, is_deleted=False)
            .first()
        )

    return cadeia


def locked_ancestors(
    db: Session, user_id, folder: StorageFolder
) -> List[StorageFolder]:
    """Pastas com senha, no caminho até à raiz, ainda por desbloquear."""
    return [
        f
        for f in get_ancestors(db, folder)
        if f.is_locked and not is_unlocked(user_id, f.id)
    ]


def assert_folder_access(db: Session, user_id, folder: Optional[StorageFolder]) -> None:
    """423 se alguma pasta do caminho estiver bloqueada."""
    if folder is None:  # raiz nunca tem senha
        return

    bloqueadas = locked_ancestors(db, user_id, folder)
    if bloqueadas:
        alvo = bloqueadas[-1]  # a mais próxima da raiz é a que interessa pedir
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={
                "message": f"A pasta '{alvo.name}' está protegida por senha.",
                "folder_id": str(alvo.id),
                "folder_name": alvo.name,
            },
        )


def assert_file_access(db: Session, user_id, file: FileModel) -> None:
    """Aplica a regra da pasta ao ficheiro (raiz = sem restrição)."""
    if not file.folder_id:
        return
    folder = (
        db.query(StorageFolder)
        .filter_by(id=file.folder_id, is_deleted=False)
        .first()
    )
    assert_folder_access(db, user_id, folder)


def accessible_folder_ids(db: Session, user_id) -> set:
    """Ids das pastas do utilizador que estão acessíveis agora.

    Serve para filtrar listagens: os ficheiros dentro de pastas bloqueadas não
    aparecem sequer, em vez de aparecerem e falharem no download.
    """
    pastas = (
        db.query(StorageFolder).filter_by(user_id=user_id, is_deleted=False).all()
    )
    por_id = {f.id: f for f in pastas}

    # Pasta bloqueada por si mesma ou por um ascendente.
    bloqueada_cache: dict = {}

    def _bloqueada(f: StorageFolder, profundidade: int = 0) -> bool:
        if f.id in bloqueada_cache:
            return bloqueada_cache[f.id]
        if profundidade > MAX_DEPTH:
            return True

        resultado = f.is_locked and not is_unlocked(user_id, f.id)
        if not resultado and f.parent_id and f.parent_id in por_id:
            resultado = _bloqueada(por_id[f.parent_id], profundidade + 1)

        bloqueada_cache[f.id] = resultado
        return resultado

    return {f.id for f in pastas if not _bloqueada(f)}


# ──────────────────────────── senha ────────────────────────────


def set_password(db: Session, folder: StorageFolder, nova: Optional[str]) -> None:
    """Define ou remove a senha. `None`/vazio remove.

    Qualquer alteração revoga o desbloqueio em curso: quem já lá estava tem de
    voltar a autenticar-se com a senha nova.
    """
    if nova:
        if len(nova) < 4:
            raise HTTPException(
                status_code=400, detail="A senha da pasta deve ter pelo menos 4 caracteres."
            )
        folder.password_hash = hash_password(nova)
    else:
        folder.password_hash = None

    clear_unlock(folder.user_id, folder.id)


def check_password(folder: StorageFolder, senha: str) -> bool:
    if not folder.password_hash:
        return True
    try:
        return verify_password(senha or "", folder.password_hash)
    except Exception:  # noqa: BLE001 — hash corrompido não deve dar 500
        return False


# ──────────────────────────── criação/movimentação ────────────────────────────


def assert_no_cycle(db: Session, folder: StorageFolder, novo_parent_id) -> None:
    """Impede mover uma pasta para dentro de si própria ou de um descendente."""
    if novo_parent_id is None:
        return
    if str(novo_parent_id) == str(folder.id):
        raise HTTPException(
            status_code=400, detail="Uma pasta não pode ser a sua própria pasta-mãe."
        )

    destino = get_folder_or_404(db, folder.user_id, novo_parent_id)
    for ancestral in get_ancestors(db, destino):
        if ancestral.id == folder.id:
            raise HTTPException(
                status_code=400,
                detail="Não é possível mover uma pasta para dentro de si própria.",
            )


def assert_depth_ok(db: Session, parent_id) -> None:
    if parent_id is None:
        return
    parent = db.query(StorageFolder).filter_by(id=parent_id).first()
    if parent and len(get_ancestors(db, parent)) >= MAX_DEPTH:
        raise HTTPException(
            status_code=400, detail=f"Profundidade máxima de {MAX_DEPTH} pastas atingida."
        )


def assert_name_free(
    db: Session, user_id, parent_id, name: str, ignore_id: Optional[UUID] = None
) -> None:
    q = db.query(StorageFolder).filter_by(
        user_id=user_id, parent_id=parent_id, name=name, is_deleted=False
    )
    if ignore_id:
        q = q.filter(StorageFolder.id != ignore_id)

    if q.first():
        raise HTTPException(
            status_code=409, detail=f"Já existe uma pasta '{name}' neste local."
        )


def descendant_ids(db: Session, folder: StorageFolder) -> List[UUID]:
    """Ids da pasta e de tudo abaixo dela (para apagar em cascata)."""
    todos = [folder.id]
    fila = [folder.id]
    profundidade = 0

    while fila and profundidade < MAX_DEPTH:
        filhos = (
            db.query(StorageFolder.id)
            .filter(
                StorageFolder.parent_id.in_(fila),
                StorageFolder.is_deleted.is_(False),
            )
            .all()
        )
        fila = [f.id for f in filhos if f.id not in todos]
        todos.extend(fila)
        profundidade += 1

    return todos
