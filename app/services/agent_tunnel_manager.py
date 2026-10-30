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
from app.config.dotenv import get_env, get_env_bool, get_env_float, get_env_int
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

# -----------------------------------------------------------------------------
# Configurações do Túnel e Protocolo MustaLink (carregadas dinamicamente via .env)
# -----------------------------------------------------------------------------
# Tamanho máximo do payload por frame binário (padrão: 64KB = 65536 bytes)
MAX_PAYLOAD_SIZE = get_env_int("MUSTA_TUNNEL_MAX_PAYLOAD_SIZE", 64 * 1024) or (64 * 1024)

# Máximo de conexões simultâneas multiplexadas num único túnel de agente (padrão: 64)
MAX_CONCURRENT_STREAMS = get_env_int("MUSTA_TUNNEL_MAX_CONCURRENT_STREAMS", 64) or 64

# Capacidade da fila assíncrona por stream para backpressure e controle de fluxo (padrão: 256)
MAX_STREAM_QUEUE_SIZE = get_env_int("MUSTA_TUNNEL_MAX_STREAM_QUEUE_SIZE", 256) or 256

# Timeout em segundos para o agente responder à abertura de socket local (padrão: 7.0s)
OPEN_ACK_TIMEOUT = get_env_float("MUSTA_TUNNEL_OPEN_ACK_TIMEOUT", 7.0) or 7.0

# Tempo de vida dos códigos temporários de emparelhamento em segundos (padrão: 600s = 10 min)
PAIRING_CODE_TTL_SECONDS = get_env_int("MUSTA_PAIRING_CODE_TTL_SECONDS", 600) or 600

# Timeout em segundos para encerramento gracioso de sockets no Windows (padrão: 1.5s)
SOCKET_CLOSE_TIMEOUT = get_env_float("MUSTA_TUNNEL_SOCKET_CLOSE_TIMEOUT", 1.5) or 1.5

# Endereço de ligação (bind) do listener de relay TCP local (padrão: 127.0.0.1)
TUNNEL_BIND_HOST = (get_env("MUSTA_TUNNEL_BIND_HOST", "127.0.0.1") or "127.0.0.1").strip()

# Parâmetros padrão ao gerar código de emparelhamento
DEFAULT_AGENT_ID = (get_env("MUSTA_AGENT_DEFAULT_ID", "luanda-pg-01") or "luanda-pg-01").strip()
DEFAULT_TARGET_DB = (get_env("MUSTA_AGENT_DEFAULT_TARGET_DB", "127.0.0.1:5432") or "127.0.0.1:5432").strip()

# Validade padrão e limite máximo para tokens JWT dedicados do agente (em dias)
DEFAULT_TOKEN_EXPIRES_DAYS = get_env_int("MUSTA_AGENT_TOKEN_EXPIRES_DAYS", 30) or 30
MAX_TOKEN_EXPIRES_DAYS = get_env_int("MUSTA_AGENT_TOKEN_MAX_DAYS", 365) or 365

# Versão padrão reportada pelo catálogo e sessões do agente
DEFAULT_AGENT_VERSION = (get_env("MUSTA_AGENT_DEFAULT_VERSION", "1.0.0") or "1.0.0").strip()

# TTL das URLs pré-assinadas de download dos binários no MinIO (segundos)
RELEASE_PRESIGNED_TTL = get_env_int("MUSTA_RELEASE_PRESIGNED_TTL", 86400) or 86400
RELEASE_SHORT_PRESIGNED_TTL = get_env_int("MUSTA_RELEASE_SHORT_PRESIGNED_TTL", 3600) or 3600
RELEASE_FALLBACK_FILE_SIZE = get_env_int("MUSTA_RELEASE_FALLBACK_FILE_SIZE", 3650000) or 3650000

_AGENT_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")
_PAIRING_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

# Whitelists de sistemas operacionais e arquiteturas permitidas para download
_env_os = get_env("MUSTA_ALLOWED_OS")
ALLOWED_OS = (
    {x.strip().lower() for x in _env_os.split(",") if x.strip()}
    if _env_os
    else {"windows", "linux", "darwin"}
)

_env_arch = get_env("MUSTA_ALLOWED_ARCH")
ALLOWED_ARCH = (
    {x.strip().lower() for x in _env_arch.split(",") if x.strip()}
    if _env_arch
    else {"amd64", "arm64"}
)


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
    expires_days: int = DEFAULT_TOKEN_EXPIRES_DAYS,
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
    return create_token(payload, timedelta(days=max(1, min(expires_days, MAX_TOKEN_EXPIRES_DAYS))))


def authenticate_agent_websocket_with_reason(
    websocket: WebSocket,
) -> Tuple[Optional[Tuple[int, str, str]], str]:
    """
    Valida estritamente a autenticação do agente via cabeçalho `Authorization: Bearer <token>`,
    retornando a tupla `(user_id, agent_id, jti)` e o motivo detalhado de erro em caso de recusa.
    """
    auth_header = (websocket.headers.get("authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None, "Cabeçalho Authorization Bearer ausente ou inválido"

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None, "Token Bearer vazio"

    payload = decode_token(token)
    if not isinstance(payload, dict):
        return None, "Token JWT malformado ou assinatura expirada/inválida"

    # Exige token específico de agente (recusa tokens normais de sessão web)
    if payload.get("typ") != "agent_tunnel":
        return None, f"Tipo de token incompatível ('{payload.get('typ')}'), esperado 'agent_tunnel'"

    encrypted_sub = payload.get("sub")
    if not encrypted_sub:
        return None, "Identificador de utilizador ausente no token"

    try:
        user_id = int(aes_decrypt(str(encrypted_sub)))
    except Exception:
        return None, "Falha na decifra do identificador do utilizador"

    token_agent_id = normalize_agent_id(str(payload.get("agent_id") or "default"))
    header_agent_id = websocket.headers.get("x-musta-agent-id")
    if header_agent_id and normalize_agent_id(header_agent_id) != token_agent_id:
        return None, f"ID do agente divergente (header '{header_agent_id}' != token '{token_agent_id}')"

    jti = str(payload.get("jti") or "")
    if agent_tunnel_registry.is_revoked(user_id, token_agent_id, jti):
        return None, f"Agente '{token_agent_id}' foi revogado no painel MustaInfo (credencial invalidada)"

    # Confirma na base de dados que o utilizador existe e está ativo
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return None, f"Utilizador #{user_id} não encontrado na base de dados"
        if not user.is_active:
            return None, f"Conta do utilizador #{user_id} está desativada"
    finally:
        db.close()

    return (user_id, token_agent_id, jti), "Autenticado com sucesso"


def authenticate_agent_websocket(websocket: WebSocket) -> Optional[Tuple[int, str, str]]:
    auth_info, _ = authenticate_agent_websocket_with_reason(websocket)
    return auth_info


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
        version: str = DEFAULT_AGENT_VERSION,
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
            host=TUNNEL_BIND_HOST,
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
                await asyncio.wait_for(writer.wait_closed(), timeout=SOCKET_CLOSE_TIMEOUT)
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
        # Limpa qualquer engine em cache para este utilizador, pois a porta local do túnel mudou
        try:
            from app.config.engine_manager_cache import EngineManager
            from app.ultils.conect_database import close_engine

            old_engine = EngineManager.get(session.user_id)
            if old_engine:
                close_engine(old_engine)
            EngineManager.remove(session.user_id)
        except Exception:
            pass

    async def unregister(self, session: AgentTunnelSession) -> None:
        key = (session.user_id, session.agent_id)
        if self._sessions.get(key) is session:
            self._sessions.pop(key, None)
        await session.close()
        # Limpa qualquer engine em cache para este utilizador, pois o túnel foi encerrado
        try:
            from app.config.engine_manager_cache import EngineManager
            from app.ultils.conect_database import close_engine

            old_engine = EngineManager.get(session.user_id)
            if old_engine:
                close_engine(old_engine)
            EngineManager.remove(session.user_id)
        except Exception:
            pass

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
        agent_id: str = DEFAULT_AGENT_ID,
        target_db: str = DEFAULT_TARGET_DB,
        expires_days: int = DEFAULT_TOKEN_EXPIRES_DAYS,
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
            presigned = storage.generate_url(key, expires=RELEASE_SHORT_PRESIGNED_TTL, fallback_url=direct_url)
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

    # 1. Verifica no diretório local gmTGBackend/musta-link-agent/dist
    backend_root = Path(__file__).resolve().parents[2]
    cand1 = backend_root / "musta-link-agent" / "dist"
    if cand1.is_dir():
        return cand1

    # 2. Verifica no diretório pai workspace/musta-link-agent/dist
    workspace_root = Path(__file__).resolve().parents[3]
    cand2 = workspace_root / "musta-link-agent" / "dist"
    if cand2.is_dir():
        return cand2

    return backend_root / "musta-link-dist"


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
        import hashlib

        for os_name in ("windows", "linux", "darwin"):
            for arch in ("amd64", "arm64"):
                fname = f"musta-link-{os_name}-{arch}.zip"
                local_zip = dist_dir / fname
                if local_zip.is_file():
                    size_b = local_zip.stat().st_size
                    try:
                        sha = hashlib.sha256(local_zip.read_bytes()).hexdigest()
                    except Exception:
                        sha = ""
                else:
                    size_b = RELEASE_FALLBACK_FILE_SIZE
                    sha = ""

                items.append(
                    {
                        "os": os_name,
                        "arch": arch,
                        "filename": fname,
                        "size_bytes": size_b,
                        "sha256": sha,
                        "version": DEFAULT_AGENT_VERSION,
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
                    ExpiresIn=RELEASE_PRESIGNED_TTL,
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


def get_musta_tunnel_config() -> dict[str, Any]:
    """Retorna um resumo de todas as variáveis de configuração ativas do MustaLink para diagnóstico e logs."""
    return {
        "max_payload_size": MAX_PAYLOAD_SIZE,
        "max_concurrent_streams": MAX_CONCURRENT_STREAMS,
        "max_stream_queue_size": MAX_STREAM_QUEUE_SIZE,
        "open_ack_timeout": OPEN_ACK_TIMEOUT,
        "pairing_code_ttl_seconds": PAIRING_CODE_TTL_SECONDS,
        "socket_close_timeout": SOCKET_CLOSE_TIMEOUT,
        "tunnel_bind_host": TUNNEL_BIND_HOST,
        "default_agent_id": DEFAULT_AGENT_ID,
        "default_target_db": DEFAULT_TARGET_DB,
        "default_token_expires_days": DEFAULT_TOKEN_EXPIRES_DAYS,
        "max_token_expires_days": MAX_TOKEN_EXPIRES_DAYS,
        "default_agent_version": DEFAULT_AGENT_VERSION,
        "minio_prefix": get_minio_releases_prefix(),
        "minio_base_url": get_minio_releases_base_url(),
        "dist_dir": str(get_releases_dist_dir()),
        "allowed_os": sorted(list(ALLOWED_OS)),
        "allowed_arch": sorted(list(ALLOWED_ARCH)),
    }



# -----------------------------------------------------------------------------
# Sincronização e Verificação de Versão das Releases no MinIO / S3
# -----------------------------------------------------------------------------

def parse_semver(ver: str) -> tuple[int, ...]:
    """Extrai tupla numérica (major, minor, patch) de uma string de versão."""
    clean = re.sub(r"^[vV]", "", (ver or "").strip())
    parts = []
    for p in clean.split("."):
        m = re.match(r"^(\d+)", p)
        if m:
            parts.append(int(m.group(1)))
        else:
            break
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])


def compare_versions(v1: str, v2: str) -> int:
    """
    Compara v1 e v2 segundo SemVer.
    Retorna -1 se v1 < v2, 0 se v1 == v2, 1 se v1 > v2.
    """
    p1 = parse_semver(v1)
    p2 = parse_semver(v2)
    if p1 < p2:
        return -1
    elif p1 > p2:
        return 1
    return 0


def extract_manifest_version(data: Any) -> Optional[str]:
    """Extrai a versão de um manifesto releases.json (seja lista de objetos ou dict)."""
    if isinstance(data, dict):
        if data.get("version"):
            return str(data["version"]).strip()
        releases = data.get("releases")
        if isinstance(releases, list) and releases and isinstance(releases[0], dict):
            return str(releases[0].get("version") or "").strip()
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        return str(data[0].get("version") or "").strip()
    return None


def sync_agent_releases_with_storage(force_upload: bool = False) -> dict[str, Any]:
    """
    Verifica e sincroniza os pacotes compilados do MustaLink Agent com o MinIO / S3:
      1. Se os pacotes estiverem ausentes no MinIO, realiza upload de todos os binários.
      2. Se existirem, analisa a versão do MinIO vs versão local:
         - Versão MinIO < Local (passada/inferior): atualiza todos os pacotes no MinIO.
         - Versão MinIO == Local (igual): verifica se falta algum ficheiro e envia apenas os ausentes.
         - Versão MinIO > Local (superior): mantém o MinIO intacto.
    """
    auto_sync = get_env_bool("MUSTA_AUTO_SYNC_MINIO_ON_STARTUP", True)
    if not auto_sync and not force_upload:
        log_message("ℹ️ [MustaLink] Sincronização automática com MinIO desativada por .env (MUSTA_AUTO_SYNC_MINIO_ON_STARTUP=false).", "info")
        return {"status": "skipped", "reason": "disabled_by_env"}

    dist_dir = get_releases_dist_dir()
    if not dist_dir.is_dir():
        log_message(f"⚠️ [MustaLink] Diretório de releases '{dist_dir}' não encontrado. Sincronização MinIO ignorada.", "warning")
        return {"status": "skipped", "reason": "no_dist_dir"}

    local_files = sorted(
        [
            p
            for p in dist_dir.iterdir()
            if p.is_file() and (p.suffix == ".zip" or p.name in ("releases.json", "SHA256SUMS.txt"))
        ]
    )

    if not local_files:
        log_message(f"⚠️ [MustaLink] Nenhum ficheiro .zip ou manifesto encontrado em '{dist_dir}'.", "warning")
        return {"status": "skipped", "reason": "no_local_files"}

    # Determina a versão local
    local_version = DEFAULT_AGENT_VERSION
    manifest_local_path = dist_dir / "releases.json"
    if manifest_local_path.is_file():
        try:
            m_data = json.loads(manifest_local_path.read_text(encoding="utf-8-sig"))
            extracted = extract_manifest_version(m_data)
            if extracted:
                local_version = extracted
        except Exception:
            pass

    # Inicializa StorageService
    try:
        from cloud.config import StorageService

        storage = StorageService()
    except Exception as err:
        log_message(f"⚠️ [MustaLink] Falha ao inicializar StorageService: {err}", "warning")
        return {"status": "error", "error": str(err)}

    if not storage.bucket or not storage.s3:
        log_message("⚠️ [MustaLink] MinIO/S3 não configurado (STORAGE_BUCKET ou cliente ausente). Sincronização ignorada.", "warning")
        return {"status": "skipped", "reason": "storage_not_configured"}

    prefix = get_minio_releases_prefix()
    base_url = get_minio_releases_base_url()

    # Inspeciona estado atual no MinIO
    remote_version: Optional[str] = None
    existing_remote_filenames: set[str] = set()

    try:
        paginator = storage.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=storage.bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                fname = key[len(prefix) :].lstrip("/")
                if fname:
                    existing_remote_filenames.add(fname)

        if "releases.json" in existing_remote_filenames:
            try:
                resp = storage.s3.get_object(Bucket=storage.bucket, Key=f"{prefix}/releases.json")
                body = resp["Body"].read().decode("utf-8-sig")
                remote_data = json.loads(body)
                remote_version = extract_manifest_version(remote_data)
            except Exception as read_err:
                log_message(f"⚠️ [MustaLink] Falha ao ler releases.json do MinIO ({read_err}). Será regravado.", "warning")
                remote_version = None
    except Exception as list_err:
        log_message(f"⚠️ [MustaLink] Falha ao listar objetos no MinIO ({list_err}). Tentando sincronização direta.", "warning")

    # Decisão de sincronização
    files_to_upload: list[Path] = []
    reason = ""

    if force_upload:
        files_to_upload = local_files
        reason = "forçada manualmente"
    elif remote_version is None or "releases.json" not in existing_remote_filenames:
        files_to_upload = local_files
        reason = f"pacotes ausentes no MinIO (enviando versão {local_version})"
    else:
        cmp = compare_versions(remote_version, local_version)
        if cmp < 0:
            # Versão no MinIO é passada / inferior (< local)
            files_to_upload = local_files
            reason = f"versão no MinIO desatualizada (remota {remote_version} < local {local_version})"
        elif cmp == 0:
            # Versão no MinIO é igual (== local): verifica se faltam arquivos
            missing = [p for p in local_files if p.name not in existing_remote_filenames]
            if missing:
                files_to_upload = missing
                reason = f"versão {local_version} igual, mas {len(missing)} ficheiro(s) estavam em falta no MinIO"
            else:
                log_message(
                    f"✅ [MustaLink] Pacotes do agente já estão no MinIO e atualizados (versão: {remote_version}, {len(existing_remote_filenames)} ficheiros).",
                    level="info",
                )
                return {
                    "status": "up_to_date",
                    "version": remote_version,
                    "files_count": len(existing_remote_filenames),
                    "base_url": base_url,
                }
        else:
            # Versão no MinIO é superior (> local)
            log_message(
                f"ℹ️ [MustaLink] MinIO possui versão mais recente que a local (remota {remote_version} > local {local_version}). Mantendo MinIO.",
                level="info",
            )
            return {
                "status": "up_to_date",
                "version": remote_version,
                "base_url": base_url,
            }

    # Executa o upload dos ficheiros selecionados
    log_message(
        f"🚀 [MustaLink] Sincronizando binários no MinIO ({reason}). Enviando {len(files_to_upload)} ficheiro(s)...",
        level="info",
    )

    uploaded: list[str] = []
    for file_path in files_to_upload:
        key = f"{prefix}/{file_path.name}"
        content_type = (
            "application/zip"
            if file_path.suffix == ".zip"
            else "application/json"
            if file_path.suffix == ".json"
            else "text/plain"
        )
        try:
            with open(file_path, "rb") as fh:
                storage.upload_file(fh, key=key, content_type=content_type, sync_async=False)
            uploaded.append(file_path.name)
        except Exception as up_err:
            log_message(f"❌ [MustaLink] Erro ao enviar {file_path.name} para o MinIO: {up_err}", "error")

    log_message(
        f"🎉 [MustaLink] Sincronização concluída no MinIO: {len(uploaded)}/{len(files_to_upload)} ficheiros enviados com sucesso (versão: {local_version}).",
        level="info",
    )

    return {
        "status": "synced",
        "version": local_version,
        "uploaded": uploaded,
        "base_url": base_url,
    }
