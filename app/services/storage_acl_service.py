"""Controlo de Acesso e Permissões Granulares (ACL + Links Partilhados)
para o módulo MustaInf Cloud Storage (`/storage`).

Níveis de permissão (Roles):
- `viewer`     (nível 10): Apenas leitura de metadados (vê na lista, não pode baixar nem alterar).
- `downloader` (nível 20): Visualização + Download (individual ou ZIP), sem alterar/apagar.
- `editor`     (nível 30): Visualização + Download + Upload na pasta + Mover/Renomear.
- `owner`      (nível 100): Controlo total (apagar, gerir senhas, gerir permissões e links).

Suporta:
1. **Herança em Árvore**: Permissões atribuídas a uma pasta propagam-se às subpastas e ficheiros.
2. **Escopo de Visibilidade**: `private` (apenas dono e convidados) ou `company` (membros da mesma empresa).
3. **Links de Partilha Temporários**: Com expiração em horas, limite máximo de downloads e senha opcional.
"""

from __future__ import annotations

import json
import os
import secrets
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Literal, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.auth import hash_password, verify_password
from app.config.redis import delete_cache, read_cache, write_cache
from app.cruds.user_crud import get_user_by_email, get_user_by_id
from app.models.clouds_models import FileModel, StorageFolder
from app.models import user_model
from app.services.storage_folder_service import get_ancestors

TargetType = Literal["file", "folder"]
AccessRole = Literal["viewer", "downloader", "editor", "owner"]
VisibilityScope = Literal["private", "company"]

ROLE_LEVELS: Dict[str, int] = {
    "none": 0,
    "viewer": 10,
    "downloader": 20,
    "editor": 30,
    "owner": 100,
}

ROLE_LABELS: Dict[str, str] = {
    "viewer": "Leitor (Apenas Ver)",
    "downloader": "Transferência (Ver e Baixar)",
    "editor": "Editor (Upload e Organizar)",
    "owner": "Proprietário (Controlo Total)",
}

_ACL_STORE_PATH = os.path.join("./storage_cache", ".acl_registry.json")
_lock = threading.Lock()

# Estrutura persistida:
# {
#   "policies": {
#       "file:<uuid>": {
#           "target_type": "file",
#           "target_id": "<uuid>",
#           "owner_id": 1,
#           "visibility": "private" | "company",
#           "company_role": "viewer" | "downloader" | "editor",
#           "grants": {
#               "<user_id>": {
#                   "user_id": 2,
#                   "email": "ana@empresa.ao",
#                   "nome": "Ana",
#                   "role": "downloader",
#                   "granted_at": "2026-10-29T..."
#               }
#           },
#           "rate_limit_override_mb_s": null
#       }
#   },
#   "share_links": {
#       "<token>": {
#           "token": "<token>",
#           "target_type": "file" | "folder",
#           "target_id": "<uuid>",
#           "owner_id": 1,
#           "role": "downloader" | "viewer",
#           "expires_at": "2026-10-30T..." | null,
#           "max_downloads": 10 | null,
#           "downloads_count": 0,
#           "password_hash": "..." | null,
#           "created_at": "2026-10-29T..."
#       }
#   }
# }
_state: Dict[str, dict] = {"policies": {}, "share_links": {}}
_loaded = False


def _ensure_loaded() -> None:
    global _loaded, _state
    if _loaded:
        return
    os.makedirs(os.path.dirname(_ACL_STORE_PATH), exist_ok=True)
    cached = read_cache("storage:acl:global_state")
    if isinstance(cached, dict) and "policies" in cached:
        _state = cached
        _loaded = True
        return
    if os.path.exists(_ACL_STORE_PATH):
        try:
            with open(_ACL_STORE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _state["policies"] = data.get("policies", {})
                    _state["share_links"] = data.get("share_links", {})
        except Exception:
            pass
    _loaded = True


def _persist() -> None:
    os.makedirs(os.path.dirname(_ACL_STORE_PATH), exist_ok=True)
    write_cache("storage:acl:global_state", _state, ttl=86400 * 90)
    try:
        tmp = f"{_ACL_STORE_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, _ACL_STORE_PATH)
    except Exception:
        pass


def _policy_key(target_type: str, target_id: object) -> str:
    return f"{target_type}:{str(target_id)}"


def _default_policy(target_type: str, target_id: object, owner_id: int) -> dict:
    return {
        "target_type": target_type,
        "target_id": str(target_id),
        "owner_id": int(owner_id),
        "visibility": "private",
        "company_role": "downloader",
        "grants": {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def get_or_init_policy(target_type: TargetType, target_id: object, owner_id: int) -> dict:
    key = _policy_key(target_type, target_id)
    with _lock:
        _ensure_loaded()
        pol = _state["policies"].get(key)
        if not pol:
            pol = _default_policy(target_type, target_id, owner_id)
            _state["policies"][key] = pol
        return pol


def resolve_target_owner(
    db: Session, target_type: TargetType, target_id: object
) -> Tuple[int, str, Optional[FileModel], Optional[StorageFolder]]:
    """Localiza o ficheiro ou pasta e devolve `(owner_id, display_name, file_obj, folder_obj)`."""
    if target_type == "file":
        f = db.query(FileModel).filter_by(id=target_id, is_deleted=False).first()
        if not f:
            raise HTTPException(status_code=404, detail="Ficheiro não encontrado.")
        return int(f.user_id), f.filename, f, None
    elif target_type == "folder":
        folder = db.query(StorageFolder).filter_by(id=target_id, is_deleted=False).first()
        if not folder:
            raise HTTPException(status_code=404, detail="Pasta não encontrada.")
        return int(folder.user_id), folder.name, None, folder
    else:
        raise HTTPException(status_code=400, detail="Tipo de alvo inválido (use 'file' ou 'folder').")


def _single_policy_role_for_user(db: Session, policy: dict, user_id: int) -> Optional[str]:
    if int(policy.get("owner_id", -1)) == int(user_id):
        return "owner"

    grants = policy.get("grants", {})
    grant = grants.get(str(user_id))
    if grant and grant.get("role") in ROLE_LEVELS:
        return grant["role"]

    if policy.get("visibility") == "company":
        owner = db.query(user_model.User).filter(user_model.User.id == int(policy["owner_id"])).first()
        actor = db.query(user_model.User).filter(user_model.User.id == int(user_id)).first()
        if owner and actor and owner.empresa_id and owner.empresa_id == actor.empresa_id:
            return policy.get("company_role", "viewer")

    return None


def get_effective_file_role(db: Session, user_id: int, file_obj: FileModel) -> str:
    """Calcula a permissão efetiva (`owner`, `editor`, `downloader`, `viewer`, `none`)
    de um utilizador sobre um `FileModel`, considerando herança das pastas ascendentes.
    """
    if int(file_obj.user_id) == int(user_id):
        return "owner"

    with _lock:
        _ensure_loaded()
        best_role = "none"

        file_pol = _state["policies"].get(_policy_key("file", file_obj.id))
        if file_pol:
            r = _single_policy_role_for_user(db, file_pol, int(user_id))
            if r and ROLE_LEVELS.get(r, 0) > ROLE_LEVELS[best_role]:
                best_role = r

        if file_obj.folder_id:
            folder = (
                db.query(StorageFolder)
                .filter_by(id=file_obj.folder_id, is_deleted=False)
                .first()
            )
            if folder:
                for anc in get_ancestors(db, folder):
                    f_pol = _state["policies"].get(_policy_key("folder", anc.id))
                    if f_pol:
                        r = _single_policy_role_for_user(db, f_pol, int(user_id))
                        if r and ROLE_LEVELS.get(r, 0) > ROLE_LEVELS[best_role]:
                            best_role = r

        return best_role


def get_effective_folder_role(db: Session, user_id: int, folder: StorageFolder) -> str:
    """Calcula a permissão efetiva de um utilizador sobre uma `StorageFolder`."""
    if int(folder.user_id) == int(user_id):
        return "owner"

    with _lock:
        _ensure_loaded()
        best_role = "none"
        for anc in get_ancestors(db, folder):
            f_pol = _state["policies"].get(_policy_key("folder", anc.id))
            if f_pol:
                r = _single_policy_role_for_user(db, f_pol, int(user_id))
                if r and ROLE_LEVELS.get(r, 0) > ROLE_LEVELS[best_role]:
                    best_role = r
        return best_role


def assert_permission(actual_role: str, minimum_role: AccessRole, action_label: str = "aceder a este recurso") -> None:
    if ROLE_LEVELS.get(actual_role, 0) < ROLE_LEVELS.get(minimum_role, 100):
        if actual_role == "none":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Acesso negado: não tem permissão para {action_label}.",
            )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Permissão insuficiente para {action_label}. "
                f"O seu nível atual é '{ROLE_LABELS.get(actual_role, actual_role)}', "
                f"mas esta operação requer pelo menos '{ROLE_LABELS.get(minimum_role, minimum_role)}'."
            ),
        )


# ──────────────────────────── Gestão de ACL (Grant / Revoke / Visibility) ────────────────────────────


def get_permissions_details(
    db: Session, actor_id: int, target_type: TargetType, target_id: object
) -> dict:
    owner_id, name, file_obj, folder_obj = resolve_target_owner(db, target_type, target_id)
    effective_role = (
        get_effective_file_role(db, actor_id, file_obj)
        if file_obj
        else get_effective_folder_role(db, actor_id, folder_obj)  # type: ignore[arg-type]
    )
    assert_permission(effective_role, "viewer", "consultar permissões")

    pol = get_or_init_policy(target_type, target_id, owner_id)
    with _lock:
        _ensure_loaded()
        links = [
            {
                "token": lk["token"],
                "role": lk["role"],
                "expires_at": lk.get("expires_at"),
                "max_downloads": lk.get("max_downloads"),
                "downloads_count": lk.get("downloads_count", 0),
                "has_password": bool(lk.get("password_hash")),
                "created_at": lk.get("created_at"),
            }
            for lk in _state["share_links"].values()
            if lk["target_type"] == target_type and str(lk["target_id"]) == str(target_id)
        ]

    return {
        "target_type": target_type,
        "target_id": str(target_id),
        "target_name": name,
        "owner_id": owner_id,
        "my_role": effective_role,
        "visibility": pol.get("visibility", "private"),
        "company_role": pol.get("company_role", "downloader"),
        "grants": list(pol.get("grants", {}).values()),
        "share_links": links,
    }


def grant_user_permission(
    db: Session,
    actor_id: int,
    target_type: TargetType,
    target_id: object,
    *,
    grantee_email: Optional[str] = None,
    grantee_user_id: Optional[int] = None,
    role: str = "downloader",
    visibility: Optional[VisibilityScope] = None,
    company_role: Optional[str] = None,
) -> dict:
    owner_id, name, _, _ = resolve_target_owner(db, target_type, target_id)
    if int(actor_id) != int(owner_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas o proprietário pode alterar as permissões de acesso.",
        )

    if role not in ("viewer", "downloader", "editor"):
        raise HTTPException(
            status_code=400,
            detail="Papel inválido. Escolha entre 'viewer', 'downloader' ou 'editor'.",
        )

    pol = get_or_init_policy(target_type, target_id, owner_id)

    with _lock:
        if visibility in ("private", "company"):
            pol["visibility"] = visibility
        if company_role in ("viewer", "downloader", "editor"):
            pol["company_role"] = company_role

        if grantee_email or grantee_user_id is not None:
            target_user = None
            if grantee_email:
                target_user = get_user_by_email(db, grantee_email.strip())
            elif grantee_user_id is not None:
                target_user = get_user_by_id(db, str(grantee_user_id))

            if not target_user:
                raise HTTPException(
                    status_code=404,
                    detail=f"Utilizador '{grantee_email or grantee_user_id}' não encontrado.",
                )

            if int(target_user.id) == int(owner_id):
                raise HTTPException(
                    status_code=400,
                    detail="O proprietário já possui acesso total a este recurso.",
                )

            pol.setdefault("grants", {})[str(target_user.id)] = {
                "user_id": int(target_user.id),
                "email": target_user.email,
                "nome": getattr(target_user, "nome", target_user.email),
                "role": role,
                "role_label": ROLE_LABELS.get(role, role),
                "granted_at": datetime.now(timezone.utc).isoformat(),
            }

        pol["updated_at"] = datetime.now(timezone.utc).isoformat()
        _persist()

    return get_permissions_details(db, actor_id, target_type, target_id)


def revoke_user_permission(
    db: Session,
    actor_id: int,
    target_type: TargetType,
    target_id: object,
    grantee_user_id: int,
) -> dict:
    owner_id, _, _, _ = resolve_target_owner(db, target_type, target_id)
    if int(actor_id) != int(owner_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas o proprietário pode revogar permissões.",
        )

    pol = get_or_init_policy(target_type, target_id, owner_id)
    with _lock:
        pol.get("grants", {}).pop(str(grantee_user_id), None)
        pol["updated_at"] = datetime.now(timezone.utc).isoformat()
        _persist()

    return get_permissions_details(db, actor_id, target_type, target_id)


# ──────────────────────────── Links de Partilha (Share Links) ────────────────────────────


def create_share_link(
    db: Session,
    actor_id: int,
    target_type: TargetType,
    target_id: object,
    *,
    role: str = "downloader",
    expires_in_hours: Optional[int] = 72,
    max_downloads: Optional[int] = None,
    password: Optional[str] = None,
) -> dict:
    owner_id, name, _, _ = resolve_target_owner(db, target_type, target_id)
    if int(actor_id) != int(owner_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas o proprietário pode gerar links de partilha.",
        )

    if role not in ("viewer", "downloader"):
        raise HTTPException(status_code=400, detail="O link apenas pode ter perfil 'viewer' ou 'downloader'.")

    token = secrets.token_urlsafe(18)
    now = datetime.now(timezone.utc)
    expires_at = (
        (now + timedelta(hours=max(1, int(expires_in_hours)))).isoformat()
        if expires_in_hours
        else None
    )

    entry = {
        "token": token,
        "target_type": target_type,
        "target_id": str(target_id),
        "target_name": name,
        "owner_id": owner_id,
        "role": role,
        "expires_at": expires_at,
        "max_downloads": int(max_downloads) if max_downloads and int(max_downloads) > 0 else None,
        "downloads_count": 0,
        "password_hash": hash_password(password) if password and password.strip() else None,
        "created_at": now.isoformat(),
    }

    with _lock:
        _ensure_loaded()
        _state["share_links"][token] = entry
        _persist()

    return {
        "token": token,
        "target_type": target_type,
        "target_id": str(target_id),
        "target_name": name,
        "role": role,
        "expires_at": expires_at,
        "max_downloads": entry["max_downloads"],
        "downloads_count": 0,
        "has_password": bool(entry["password_hash"]),
        "created_at": entry["created_at"],
    }


def delete_share_link(
    db: Session, actor_id: int, target_type: TargetType, target_id: object, token: str
) -> dict:
    owner_id, _, _, _ = resolve_target_owner(db, target_type, target_id)
    if int(actor_id) != int(owner_id):
        raise HTTPException(status_code=403, detail="Apenas o proprietário pode revogar links.")

    with _lock:
        _ensure_loaded()
        lk = _state["share_links"].get(token)
        if lk and str(lk.get("target_id")) == str(target_id):
            _state["share_links"].pop(token, None)
            _persist()

    return get_permissions_details(db, actor_id, target_type, target_id)


def validate_and_consume_share_link(
    db: Session,
    token: str,
    *,
    password: Optional[str] = None,
    consume_download: bool = False,
) -> dict:
    """Valida um link de partilha (expiração, limite de downloads, senha) e,
    se `consume_download=True`, exige perfil `downloader` e incrementa o contador.
    """
    with _lock:
        _ensure_loaded()
        lk = _state["share_links"].get(token)
        if not lk:
            raise HTTPException(status_code=404, detail="Link de partilha inválido ou revogado.")

        if lk.get("expires_at"):
            exp = datetime.fromisoformat(lk["expires_at"])
            if datetime.now(timezone.utc) > exp:
                raise HTTPException(status_code=410, detail="Este link de partilha expirou.")

        if lk.get("password_hash"):
            if not password or not verify_password(password, lk["password_hash"]):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Este link está protegido por senha. Introduza a senha correta.",
                )

        if consume_download:
            if lk.get("role") == "viewer":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Este link permite apenas visualização de metadados (download desativado pelo proprietário).",
                )
            max_dl = lk.get("max_downloads")
            used_dl = int(lk.get("downloads_count", 0))
            if max_dl is not None and used_dl >= int(max_dl):
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Limite máximo de downloads deste link ({max_dl}) já foi atingido.",
                )
            lk["downloads_count"] = used_dl + 1
            _persist()

        return dict(lk)


def list_shared_with_user(db: Session, user_id: int) -> dict:
    """Lista todos os ficheiros e pastas partilhados com `user_id` (via ACL direta ou empresa)."""
    with _lock:
        _ensure_loaded()
        policies_snapshot = list(_state["policies"].values())

    shared_files = []
    shared_folders = []
    seen_files = set()

    for pol in policies_snapshot:
        if int(pol.get("owner_id", -1)) == int(user_id):
            continue
        role = _single_policy_role_for_user(db, pol, int(user_id))
        if not role:
            continue

        t_type = pol.get("target_type")
        t_id = pol.get("target_id")

        if t_type == "file":
            f = db.query(FileModel).filter_by(id=t_id, is_deleted=False).first()
            if f and str(f.id) not in seen_files:
                seen_files.add(str(f.id))
                owner = db.query(user_model.User).filter(user_model.User.id == f.user_id).first()
                shared_files.append(
                    {
                        "id": str(f.id),
                        "filename": f.filename,
                        "size_bytes": f.size_bytes,
                        "mime_type": f.mime_type,
                        "folder_id": str(f.folder_id) if f.folder_id else None,
                        "created_at": f.created_at.isoformat() if f.created_at else None,
                        "owner_id": f.user_id,
                        "owner_email": owner.email if owner else None,
                        "owner_name": owner.nome if owner else "Utilizador",
                        "my_role": role,
                        "my_role_label": ROLE_LABELS.get(role, role),
                    }
                )
        elif t_type == "folder":
            folder = db.query(StorageFolder).filter_by(id=t_id, is_deleted=False).first()
            if folder:
                owner = db.query(user_model.User).filter(user_model.User.id == folder.user_id).first()
                shared_folders.append(
                    {
                        "id": str(folder.id),
                        "name": folder.name,
                        "owner_id": folder.user_id,
                        "owner_email": owner.email if owner else None,
                        "owner_name": owner.nome if owner else "Utilizador",
                        "my_role": role,
                        "my_role_label": ROLE_LABELS.get(role, role),
                    }
                )
                # Também inclui ficheiros dentro desta pasta partilhada
                files_in_folder = (
                    db.query(FileModel)
                    .filter_by(folder_id=folder.id, is_deleted=False)
                    .all()
                )
                for ff in files_in_folder:
                    if str(ff.id) not in seen_files:
                        seen_files.add(str(ff.id))
                        shared_files.append(
                            {
                                "id": str(ff.id),
                                "filename": ff.filename,
                                "size_bytes": ff.size_bytes,
                                "mime_type": ff.mime_type,
                                "folder_id": str(ff.folder_id) if ff.folder_id else None,
                                "created_at": ff.created_at.isoformat() if ff.created_at else None,
                                "owner_id": ff.user_id,
                                "owner_email": owner.email if owner else None,
                                "owner_name": owner.nome if owner else "Utilizador",
                                "my_role": role,
                                "my_role_label": ROLE_LABELS.get(role, role),
                            }
                        )

    return {
        "files": shared_files,
        "folders": shared_folders,
    }


def reset_acl_state_for_tests() -> None:
    """Limpa estado de ACL (usado em testes unitários)."""
    global _state, _loaded
    with _lock:
        _state = {"policies": {}, "share_links": {}}
        _loaded = True
        delete_cache("storage:acl:global_state")
        if os.path.exists(_ACL_STORE_PATH):
            try:
                os.remove(_ACL_STORE_PATH)
            except OSError:
                pass

