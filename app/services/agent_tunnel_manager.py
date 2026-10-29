from __future__ import annotations

import asyncio
import json
import re
import secrets
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from fastapi import WebSocket

from app.auth import create_token, decode_token
from app.config.dotenv import get_env
from app.database import SessionLocal
from app.models.user_model import User
from app.services.crypto_utils import aes_decrypt, aes_encrypt
from app.ultils.logger import log_message

# Protocolo binário multiplexado MustaLink v1
# Cabeçalho fixo de 9 bytes: [FrameType: 1B][ConnID: 4B BigEndian][PayloadLen: 4B BigEndian]
FRAME_OPEN = 1
FRAME_OPEN_ACK = 2
FRAME_DATA = 3
FRAME_CLOSE = 4
FRAME_ERROR = 5

HEADER_STRUCT = struct.Struct("!BII")
MAX_PAYLOAD_SIZE = 64 * 1024
MAX_CONCURRENT_STREAMS = 64
MAX_STREAM_QUEUE_SIZE = 256
OPEN_ACK_TIMEOUT = 7.0
PAIRING_CODE_TTL_SECONDS = 600  # 10 minutos

_AGENT_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")
_PAIRING_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

ALLOWED_OS = {"windows", "linux", "darwin"}
ALLOWED_ARCH = {"amd64", "arm64"}


@dataclass
class PairingCodeEntry:
    code: str
    user_id: int
    agent_id: str
    target_db: str
    expires_days: int
    expires_at: datetime


def normalize_agent_id(raw: Optional[str]) -> str:
    if not raw or not raw.strip():
        return "default"
    cleaned = _AGENT_ID_RE.sub("", raw.strip())[:64]
    return cleaned or "default"


def create_agent_tunnel_token(
    user_id: int,
    agent_id: str = "default",
    expires_days: int = 30,
) -> str:
    """
    Emite um token JWT dedicado (`typ="agent_tunnel"`) com identificador único (`jti`)
    para autenticar o MustaLink Agent. Reativa o `agent_id` caso tenha sido revogado antes.
    """
    norm_agent_id = normalize_agent_id(agent_id)
    jti = secrets.token_hex(12)
    agent_tunnel_registry.unrevoke_agent(user_id, norm_agent_id)

    payload = {
        "sub": aes_encrypt(str(user_id)),
        "typ": "agent_tunnel",
        "agent_id": norm_agent_id,
        "jti": jti,
    }
    return create_token(payload, timedelta(days=max(1, min(expires_days, 365))))


def authenticate_agent_websocket(websocket: WebSocket) -> Optional[Tuple[int, str, str]]:
    """
    Valida estritamente a autenticação do agente via cabeçalho `Authorization: Bearer <token>`.
    Exige:
      1. Cabeçalho Authorization Bearer (não aceita token na URL nem cookies de browser).
      2. Token JWT assinado válido com `typ == "agent_tunnel"`.
      3. Correspondência exata entre o `agent_id` do token e o header `X-Musta-Agent-ID`.
      4. Token/agente não revogado no Control Plane.
      5. Utilizador existente e com conta ativa (`is_active == True`) na base de dados.
    Devolve `(user_id, agent_id, jti)` ou `None`.
    """
    auth_header = (websocket.headers.get("authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None

    payload = decode_token(token)
    if not isinstance(payload, dict):
        return None

    # Exige token específico de agente (recusa tokens normais de sessão web)
    if payload.get("typ") != "agent_tunnel":
        return None

    encrypted_sub = payload.get("sub")
    if not encrypted_sub:
        return None

    try:
        user_id = int(aes_decrypt(str(encrypted_sub)))
    except Exception:
        return None

    token_agent_id = normalize_agent_id(str(payload.get("agent_id") or "default"))
    header_agent_id = websocket.headers.get("x-musta-agent-id")
    if header_agent_id and normalize_agent_id(header_agent_id) != token_agent_id:
        return None

    jti = str(payload.get("jti") or "")
    if agent_tunnel_registry.is_revoked(user_id, token_agent_id, jti):
        return None

    # Confirma na base de dados que o utilizador existe e está ativo
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.is_active:
            return None
    finally:
        db.close()

    return user_id, token_agent_id, jti


class AgentTunnelSession:
    """
    Representa um túnel reverso ativo de um `musta-link-agent`.
    Abre um listener TCP efémero apenas em 127.0.0.1 e multiplexa múltiplas
    conexões simultâneas sobre o único WebSocket usando `conn_id`.
    """

    def __init__(
        self,
        user_id: int,
        agent_id: str,
        target_db: str,
        websocket: WebSocket,
        token_jti: str = "",
        platform: str = "",
        version: str = "1.0.0",
    ) -> None:
        self.user_id = user_id
        self.agent_id = agent_id
        self.target_db = target_db
        self.ws = websocket
        self.token_jti = token_jti
        self.platform = platform
        self.version = version
        self.connected_at = datetime.now(timezone.utc)
        self.local_port: int = 0
        self.total_connections_handled: int = 0

        self._server: Optional[asyncio.AbstractServer] = None
        self._next_conn_id: int = 1
        self._ws_send_lock = asyncio.Lock()
        self._queues: Dict[int, asyncio.Queue[Optional[bytes]]] = {}
        self._open_events: Dict[int, asyncio.Event] = {}
        self._open_errors: Dict[int, str] = {}
        self._closed = False

    @property
    def active_streams(self) -> int:
        return len(self._queues)

    async def start_local_listener(self) -> int:
        """Inicia um socket TCP local (127.0.0.1:0) exclusivo para este túnel."""
        self._server = await asyncio.start_server(
            self._handle_local_tcp_client,
            host="127.0.0.1",
            port=0,
        )
        sockets = self._server.sockets or []
        if not sockets:
            raise RuntimeError("Não foi possível alocar porta local para o túnel do agente.")
        self.local_port = int(sockets[0].getsockname()[1])
        return self.local_port

    async def send_frame(self, frame_type: int, conn_id: int, payload: bytes = b"") -> None:
        header = HEADER_STRUCT.pack(frame_type, conn_id, len(payload))
        async with self._ws_send_lock:
            await self.ws.send_bytes(header + payload)

    async def receive_loop(self) -> None:
        """Processa os frames binários vindos do agente Go."""
        try:
            while not self._closed:
                data = await self.ws.receive_bytes()
                if len(data) < HEADER_STRUCT.size:
                    continue

                frame_type, conn_id, payload_len = HEADER_STRUCT.unpack_from(data, 0)
                if payload_len > MAX_PAYLOAD_SIZE or len(data) - HEADER_STRUCT.size != payload_len:
                    continue
                payload = data[HEADER_STRUCT.size :]

                if frame_type == FRAME_OPEN_ACK:
                    if ev := self._open_events.get(conn_id):
                        ev.set()
                elif frame_type == FRAME_ERROR:
                    self._open_errors[conn_id] = payload.decode("utf-8", errors="replace")
                    if ev := self._open_events.get(conn_id):
                        ev.set()
                elif frame_type == FRAME_DATA:
                    if q := self._queues.get(conn_id):
                        try:
                            q.put_nowait(payload)
                        except asyncio.QueueFull:
                            # Backpressure: encerra a stream se o consumidor local não acompanhar
                            try:
                                q.get_nowait()
                                q.put_nowait(None)
                            except Exception:
                                pass
                elif frame_type == FRAME_CLOSE:
                    if q := self._queues.get(conn_id):
                        try:
                            q.put_nowait(None)
                        except asyncio.QueueFull:
                            pass
        finally:
            await self.close()

    async def _handle_local_tcp_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Executado sempre que o SQLAlchemy/driver do backend se conecta à porta
        de loopback deste agente. Cria uma sessão multiplexada isolada (`conn_id`).
        """
        if self._closed or len(self._queues) >= MAX_CONCURRENT_STREAMS:
            writer.close()
            return

        conn_id = self._next_conn_id
        self._next_conn_id = (self._next_conn_id % 2_147_483_647) + 1
        self.total_connections_handled += 1

        queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=MAX_STREAM_QUEUE_SIZE)
        open_ev = asyncio.Event()
        self._queues[conn_id] = queue
        self._open_events[conn_id] = open_ev

        try:
            # 1. Solicita ao agente Go a abertura da conexão TCP com a BD local
            await self.send_frame(FRAME_OPEN, conn_id)
            await asyncio.wait_for(open_ev.wait(), timeout=OPEN_ACK_TIMEOUT)

            if conn_id in self._open_errors:
                err_msg = self._open_errors.pop(conn_id)
                raise ConnectionError(f"Agente recusou conexão local: {err_msg}")

            # 2. Encaminhamento bidirecional concorrente
            async def tcp_to_ws() -> None:
                try:
                    while not self._closed:
                        chunk = await reader.read(MAX_PAYLOAD_SIZE)
                        if not chunk:
                            break
                        await self.send_frame(FRAME_DATA, conn_id, chunk)
                finally:
                    if not self._closed:
                        try:
                            await self.send_frame(FRAME_CLOSE, conn_id)
                        except Exception:
                            pass

            async def ws_to_tcp() -> None:
                while not self._closed:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    writer.write(chunk)
                    await writer.drain()

            t1 = asyncio.create_task(tcp_to_ws())
            t2 = asyncio.create_task(ws_to_tcp())
            done, pending = await asyncio.wait(
                [t1, t2],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
        except Exception as exc:
            log_message(
                f"⚠️ [MustaLink] Sessão #{conn_id} (user={self.user_id}, agent={self.agent_id}) terminou: {exc}",
                level="warning",
            )
        finally:
            self._queues.pop(conn_id, None)
            self._open_events.pop(conn_id, None)
            self._open_errors.pop(conn_id, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None

        for ev in self._open_events.values():
            ev.set()
        for q in self._queues.values():
            try:
                q.put_nowait(None)
            except Exception:
                pass
        self._queues.clear()

        try:
            await self.ws.close()
        except Exception:
            pass

    def to_status_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "target_db": self.target_db,
            "platform": self.platform,
            "version": self.version,
            "connected_at": self.connected_at.isoformat(),
            "active_streams": self.active_streams,
            "total_connections_handled": self.total_connections_handled,
            "virtual_host": f"agent://{self.agent_id}",
            "local_relay_port": self.local_port,
        }


class AgentTunnelRegistry:
    """
    Control Plane em memória dos túneis ativos, códigos de emparelhamento e credenciais revogadas.
    Garante isolamento estrito entre utilizadores/tenants.
    """

    def __init__(self) -> None:
        self._sessions: Dict[Tuple[int, str], AgentTunnelSession] = {}
        self._revoked_agents: Set[Tuple[int, str]] = set()
        self._revoked_jtis: Set[str] = set()
        self._pairing_codes: Dict[str, PairingCodeEntry] = {}

    async def register(self, session: AgentTunnelSession) -> None:
        key = (session.user_id, session.agent_id)
        old = self._sessions.get(key)
        if old is not None and old is not session:
            await old.close()
        self._sessions[key] = session

    async def unregister(self, session: AgentTunnelSession) -> None:
        key = (session.user_id, session.agent_id)
        if self._sessions.get(key) is session:
            self._sessions.pop(key, None)
        await session.close()

    def get_session(self, user_id: Optional[int], agent_id: str = "default") -> Optional[AgentTunnelSession]:
        """
        Obtém a sessão ativa do agente APENAS se pertencer ao `user_id` indicado.
        Nunca faz fallback para sessões de outros utilizadores (isolamento estrito de tenant).
        """
        if user_id is None:
            return None

        norm_id = normalize_agent_id(agent_id)
        sess = self._sessions.get((user_id, norm_id))
        if sess and not sess._closed:
            return sess

        # Se o utilizador pediu "default" e tem exatamente 1 agente próprio conectado, usa esse
        if norm_id == "default":
            user_sessions = [
                s for (uid, _), s in self._sessions.items() if uid == user_id and not s._closed
            ]
            if len(user_sessions) == 1:
                return user_sessions[0]
        return None

    async def revoke_agent(self, user_id: int, agent_id: str) -> bool:
        """
        Revoga o agente `(user_id, agent_id)` e encerra imediatamente qualquer túnel ativo.
        """
        norm_id = normalize_agent_id(agent_id)
        key = (user_id, norm_id)
        self._revoked_agents.add(key)
        sess = self._sessions.pop(key, None)
        if sess is not None:
            if sess.token_jti:
                self._revoked_jtis.add(sess.token_jti)
            await sess.close()
            return True
        return False

    def unrevoke_agent(self, user_id: int, agent_id: str) -> None:
        norm_id = normalize_agent_id(agent_id)
        self._revoked_agents.discard((user_id, norm_id))

    def is_revoked(self, user_id: int, agent_id: str, jti: str = "") -> bool:
        norm_id = normalize_agent_id(agent_id)
        if (user_id, norm_id) in self._revoked_agents:
            return True
        if jti and jti in self._revoked_jtis:
            return True
        return False

    def create_pairing_code(
        self,
        user_id: int,
        agent_id: str = "luanda-pg-01",
        target_db: str = "127.0.0.1:5432",
        expires_days: int = 30,
    ) -> PairingCodeEntry:
        """Gera um código temporário único (XXXX-XXXX) válido por 10 minutos."""
        now = datetime.now(timezone.utc)
        # Limpa códigos expirados
        expired = [k for k, v in self._pairing_codes.items() if v.expires_at < now]
        for k in expired:
            self._pairing_codes.pop(k, None)

        part1 = "".join(secrets.choice(_PAIRING_ALPHABET) for _ in range(4))
        part2 = "".join(secrets.choice(_PAIRING_ALPHABET) for _ in range(4))
        code = f"{part1}-{part2}"

        entry = PairingCodeEntry(
            code=code,
            user_id=user_id,
            agent_id=normalize_agent_id(agent_id),
            target_db=(target_db or "127.0.0.1:5432").strip()[:128],
            expires_days=max(1, min(expires_days, 365)),
            expires_at=now + timedelta(seconds=PAIRING_CODE_TTL_SECONDS),
        )
        self._pairing_codes[code] = entry
        return entry

    def consume_pairing_code(
        self,
        raw_code: str,
        override_agent_id: Optional[str] = None,
        override_target_db: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Valida e consome (uso único) um código de emparelhamento `XXXX-XXXX`,
        emitindo um token dedicado para o agente.
        """
        norm_code = (raw_code or "").strip().upper()
        entry = self._pairing_codes.pop(norm_code, None)
        if not entry:
            return None

        if datetime.now(timezone.utc) > entry.expires_at:
            return None

        final_agent_id = entry.agent_id
        if override_agent_id and override_agent_id.strip() and override_agent_id.strip() != "default":
            final_agent_id = normalize_agent_id(override_agent_id)

        final_target_db = entry.target_db
        if override_target_db and override_target_db.strip():
            final_target_db = override_target_db.strip()[:128]

        token = create_agent_tunnel_token(
            user_id=entry.user_id,
            agent_id=final_agent_id,
            expires_days=entry.expires_days,
        )
        return {
            "user_id": entry.user_id,
            "agent_id": final_agent_id,
            "target_db": final_target_db,
            "virtual_host": f"agent://{final_agent_id}",
            "token": token,
        }

    def list_for_user(self, user_id: int) -> list[dict[str, Any]]:
        return [
            s.to_status_dict()
            for (uid, _), s in self._sessions.items()
            if uid == user_id and not s._closed
        ]


agent_tunnel_registry = AgentTunnelRegistry()


def parse_agent_host(host: str) -> Optional[str]:
    """
    Deteta se o campo `host` da conexão aponta para um MustaLink Agent.
    Formatos suportados:
      - "agent" ou "musta-agent"          -> agent_id = "default"
      - "agent://<agent_id>"              -> agent_id = "<agent_id>"
      - "musta-agent://<agent_id>"        -> agent_id = "<agent_id>"
      - "agent:<agent_id>"                -> agent_id = "<agent_id>"
    Devolve o `agent_id` normalizado ou None se for um host normal.
    """
    raw = (host or "").strip()
    lower = raw.lower()
    if lower in ("agent", "musta-agent", "tunnel", "musta-tunnel"):
        return "default"
    for prefix in ("agent://", "musta-agent://", "tunnel://", "agent:", "musta-agent:"):
        if lower.startswith(prefix):
            rest = raw[len(prefix) :].strip().strip("/")
            return normalize_agent_id(rest)
    return None


def get_minio_releases_prefix() -> str:
    """Prefixo dos pacotes MustaLink dentro do bucket MinIO, configurado no `.env`."""
    prefix = (get_env("MUSTA_LINK_MINIO_PREFIX", "releases/musta-link") or "releases/musta-link").strip()
    return prefix.strip("/")


def get_minio_releases_base_url() -> str:
    """
    Obtém a URL base pública do MinIO para os pacotes do MustaLink a partir do `.env`:
      1. `MUSTA_LINK_MINIO_BASE_URL` (ex: https://s3.mustainfo.cloud/meu-bucket/releases/musta-link)
      2. Ou construída a partir de `STORAGE_PUBLIC_ENDPOINT` / `STORAGE_ENDPOINT` + `STORAGE_BUCKET` + `MUSTA_LINK_MINIO_PREFIX`.
    """
    explicit_url = (get_env("MUSTA_LINK_MINIO_BASE_URL", "") or "").strip().rstrip("/")
    if explicit_url:
        return explicit_url

    endpoint = (
        get_env("STORAGE_PUBLIC_ENDPOINT")
        or get_env("STORAGE_ENDPOINT")
        or "https://s3.mustainfo.cloud"
    ).strip().rstrip("/")
    bucket = (get_env("STORAGE_BUCKET", "meu-bucket") or "meu-bucket").strip().strip("/")
    prefix = get_minio_releases_prefix()
    return f"{endpoint}/{bucket}/{prefix}"


def get_release_minio_url(os_name: str, arch: str) -> Optional[str]:
    """
    Devolve a URL do pacote `.zip` no MinIO para `(os_name, arch)`.
    Se o objeto existir no bucket MinIO e `StorageService` estiver ativo, gera URL
    pré-assinada; caso contrário devolve a URL direta configurada no `.env`.
    """
    clean_os = (os_name or "").strip().lower()
    clean_arch = (arch or "").strip().lower()
    if clean_os not in ALLOWED_OS or clean_arch not in ALLOWED_ARCH:
        return None

    filename = f"musta-link-{clean_os}-{clean_arch}.zip"
    base_url = get_minio_releases_base_url()
    direct_url = f"{base_url}/{filename}"
    key = f"{get_minio_releases_prefix()}/{filename}"

    try:
        from cloud.config import StorageService

        storage = StorageService()
        if storage.bucket and storage.s3:
            presigned = storage.generate_url(key, expires=3600, fallback_url=direct_url)
            if presigned:
                return presigned
    except Exception:
        pass

    return direct_url


def get_releases_dist_dir() -> Path:
    """
    Localiza a pasta onde residem os pacotes `.zip` compilados do MustaLink Agent.
    Em produção pode ser definida via variável de ambiente `MUSTA_AGENT_DIST_DIR`.
    """
    env_dir = (get_env("MUSTA_AGENT_DIST_DIR", "") or "").strip()
    if env_dir:
        return Path(env_dir).resolve()
    workspace_root = Path(__file__).resolve().parents[3]
    default_dist = workspace_root / "musta-link-agent" / "dist"
    if default_dist.is_dir():
        return default_dist
    return Path(__file__).resolve().parents[2] / "musta-link-dist"


def get_available_releases() -> list[dict[str, Any]]:
    dist_dir = get_releases_dist_dir()
    manifest_file = dist_dir / "releases.json"
    base_url = get_minio_releases_base_url()
    prefix = get_minio_releases_prefix()
    items: list[dict[str, Any]] = []

    if manifest_file.is_file():
        try:
            data = json.loads(manifest_file.read_text(encoding="utf-8-sig"))
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and isinstance(data.get("releases"), list):
                items = data["releases"]
            elif isinstance(data, dict):
                items = [data]
        except Exception:
            items = []

    # Fallback: se releases.json não existir no disco local, constrói o catálogo
    # para todas as plataformas suportadas (Windows, Linux, macOS × amd64, arm64)
    # apontando para o bucket MinIO configurado no .env.
    if not items:
        for os_name in ("windows", "linux", "darwin"):
            for arch in ("amd64", "arm64"):
                fname = f"musta-link-{os_name}-{arch}.zip"
                local_zip = dist_dir / fname
                size_b = local_zip.stat().st_size if local_zip.is_file() else 3650000
                items.append(
                    {
                        "os": os_name,
                        "arch": arch,
                        "filename": fname,
                        "size_bytes": size_b,
                        "version": "1.0.0",
                    }
                )

    s3_pub = None
    bucket_name = None
    try:
        from cloud.config import StorageService

        st = StorageService()
        if st.bucket and st.s3_public:
            s3_pub = st.s3_public
            bucket_name = st.bucket
    except Exception:
        pass

    enriched: list[dict[str, Any]] = []
    for item in items:
        entry = dict(item)
        filename = entry.get("filename") or f"musta-link-{entry.get('os')}-{entry.get('arch')}.zip"
        minio_key = f"{prefix}/{filename}"
        entry["minio_key"] = minio_key
        entry["minio_public_url"] = f"{base_url}/{filename}"
        if s3_pub and bucket_name:
            try:
                entry["download_url"] = s3_pub.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket_name, "Key": minio_key},
                    ExpiresIn=86400,
                )
            except Exception:
                entry["download_url"] = f"{base_url}/{filename}"
        else:
            entry["download_url"] = f"{base_url}/{filename}"
        enriched.append(entry)

    return enriched


def get_release_zip_path(os_name: str, arch: str) -> Optional[Path]:
    """
    Valida estritamente `(os, arch)` contra a whitelist permitida e devolve o
    caminho do pacote `.zip` se existir em disco.
    """
    clean_os = (os_name or "").strip().lower()
    clean_arch = (arch or "").strip().lower()
    if clean_os not in ALLOWED_OS or clean_arch not in ALLOWED_ARCH:
        return None

    zip_path = get_releases_dist_dir() / f"musta-link-{clean_os}-{clean_arch}.zip"
    if zip_path.is_file():
        return zip_path
    return None
