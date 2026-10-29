r"""
Publica todos os pacotes `.zip` do MustaLink Agent (`musta-link-agent/dist/`)
para o bucket MinIO configurado no `.env` (`STORAGE_ENDPOINT`, `STORAGE_BUCKET`,
`MUSTA_LINK_MINIO_PREFIX`).

Uso:
    .\venv\Scripts\python.exe -m scripts.upload_mustalink_to_minio
"""

from __future__ import annotations

import sys
from pathlib import Path

from app.services.agent_tunnel_manager import (
    get_minio_releases_base_url,
    get_minio_releases_prefix,
    get_releases_dist_dir,
)
from cloud.config import StorageService


def upload_releases() -> int:
    dist_dir: Path = get_releases_dist_dir()
    if not dist_dir.is_dir():
        print(f"❌ Pasta de releases não encontrada: {dist_dir}")
        return 1

    storage = StorageService()
    prefix = get_minio_releases_prefix()
    base_url = get_minio_releases_base_url()

    files_to_upload = sorted(
        [
            p
            for p in dist_dir.iterdir()
            if p.is_file() and (p.suffix == ".zip" or p.name in ("releases.json", "SHA256SUMS.txt"))
        ]
    )

    if not files_to_upload:
        print(f"⚠️ Nenhum pacote .zip encontrado em {dist_dir}")
        return 1

    print(f"🚀 A enviar {len(files_to_upload)} ficheiros para MinIO (bucket={storage.bucket}, prefix={prefix})...\n")

    for file_path in files_to_upload:
        key = f"{prefix}/{file_path.name}"
        content_type = (
            "application/zip"
            if file_path.suffix == ".zip"
            else "application/json"
            if file_path.suffix == ".json"
            else "text/plain"
        )
        with open(file_path, "rb") as fh:
            storage.upload_file(fh, key=key, content_type=content_type)
        print(f"  ✅ {file_path.name} -> {base_url}/{file_path.name}")

    print("\n🎉 Todos os pacotes MustaLink foram sincronizados com sucesso!")
    return 0


if __name__ == "__main__":
    sys.exit(upload_releases())
