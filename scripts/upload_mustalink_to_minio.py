from __future__ import annotations

import sys
from app.config.dotenv import load_env
load_env()

from app.services.agent_tunnel_manager import sync_agent_releases_with_storage


def upload_releases() -> int:
    """Publica ou atualiza todos os pacotes do MustaLink Agent no MinIO/S3."""
    result = sync_agent_releases_with_storage(force_upload=True)
    if result.get("status") in ("synced", "up_to_date"):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(upload_releases())
