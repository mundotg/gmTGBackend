"""
Move os ficheiros já existentes para chaves separadas por utilizador.

O esquema antigo usava o nome do ficheiro como chave no bucket, num namespace
global: dois utilizadores com `relatorio.pdf` partilhavam o mesmo objeto — o
upload de um sobrescrevia o do outro e o download devolvia o ficheiro errado.

O esquema novo é `{user_id}/{uuid}.ext`, guardado em `files.path`. A aplicação
lê sempre `path`, por isso funciona antes e depois desta migração; o script
existe para arrumar o que já lá está.

Uso:
    # 1. ver o que seria alterado, sem tocar em nada
    python -m scripts.migrate_storage_keys --dry-run

    # 2. aplicar
    python -m scripts.migrate_storage_keys

Antes de aplicar: faz backup da base de dados e do bucket (ou de
STORAGE_LOCAL_PATH). Ficheiros que não forem encontrados nem na cloud nem no
cache local são deixados intactos e reportados no fim.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from collections import defaultdict
from typing import Dict, List

from botocore.exceptions import ClientError, EndpointConnectionError

from app.database import SessionLocal
from app.models.clouds_models import FileModel
from app.routes.storage_routes import build_object_key
from cloud.config import StorageService


def _ja_migrado(file: FileModel) -> bool:
    """True se a key já tem o prefixo do dono."""
    return bool(file.path) and file.path.startswith(f"{file.user_id}/")


def _copiar_na_cloud(storage: StorageService, old_key: str, new_key: str) -> bool:
    if not storage.bucket:
        return False
    try:
        storage.s3.copy_object(
            Bucket=storage.bucket,
            CopySource={"Bucket": storage.bucket, "Key": old_key},
            Key=new_key,
        )
        return True
    except (ClientError, EndpointConnectionError):
        return False


def _copiar_local(storage: StorageService, old_key: str, new_key: str) -> bool:
    origem = storage._get_local_path(old_key)
    if not os.path.exists(origem):
        return False

    destino = storage._get_local_path(new_key)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    shutil.copy2(origem, destino)
    return True


def _remover_original(storage: StorageService, old_key: str) -> None:
    if storage.bucket:
        try:
            storage.s3.delete_object(Bucket=storage.bucket, Key=old_key)
        except Exception:  # noqa: BLE001
            pass

    local = storage._get_local_path(old_key)
    if os.path.exists(local):
        try:
            os.remove(local)
        except Exception:  # noqa: BLE001
            pass


def migrate(dry_run: bool = False) -> int:
    session = SessionLocal()
    storage = StorageService()

    migrados = 0
    ja_ok = 0
    nao_encontrados: List[str] = []

    try:
        ficheiros = session.query(FileModel).all()
        print(f"🔍 {len(ficheiros)} ficheiros registados.\n")

        pendentes = []
        for f in ficheiros:
            if _ja_migrado(f):
                ja_ok += 1
            else:
                pendentes.append(f)

        # Várias linhas podem apontar para a MESMA key legada (foi esse o bug).
        # Cada uma recebe a sua cópia; o original só é removido no fim.
        por_key: Dict[str, List[FileModel]] = defaultdict(list)
        for f in pendentes:
            por_key[f.path or f.filename].append(f)

        for old_key, grupo in por_key.items():
            if len(grupo) > 1:
                print(
                    f"  ⚠️  '{old_key}' era partilhado por {len(grupo)} registos "
                    f"(users: {sorted({g.user_id for g in grupo})}) — a duplicar."
                )

            todos_ok = True

            for f in grupo:
                new_key = build_object_key(f.user_id, f.filename)

                if dry_run:
                    print(f"  🧪 user={f.user_id} '{old_key}' → '{new_key}'")
                    migrados += 1
                    continue

                copiado = _copiar_na_cloud(storage, old_key, new_key)
                copiado = _copiar_local(storage, old_key, new_key) or copiado

                if not copiado:
                    todos_ok = False
                    nao_encontrados.append(f"user={f.user_id} '{old_key}'")
                    print(f"  ❌ user={f.user_id} '{old_key}' não encontrado")
                    continue

                f.path = new_key
                migrados += 1
                print(f"  ✅ user={f.user_id} '{old_key}' → '{new_key}'")

            if not dry_run and todos_ok:
                _remover_original(storage, old_key)

        if dry_run:
            session.rollback()
            print("\n🧪 DRY-RUN — nada foi gravado nem copiado.")
        else:
            session.commit()
            print("\n💾 Alterações gravadas.")

        print(f"   migrados={migrados}  já ok={ja_ok}  falhas={len(nao_encontrados)}")

        if nao_encontrados:
            print("\n⚠️  Sem ficheiro na cloud nem no cache local (path intacto):")
            for item in nao_encontrados:
                print(f"     - {item}")

        return 1 if nao_encontrados else 0

    except Exception as exc:  # noqa: BLE001
        session.rollback()
        print(f"❌ Migração abortada: {exc}")
        return 1

    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="mostra o que seria alterado sem gravar",
    )
    args = parser.parse_args()

    sys.exit(migrate(dry_run=args.dry_run))
