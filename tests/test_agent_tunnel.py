from __future__ import annotations

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from app.auth import create_access_token
from app.config.dependencies import _resolve_agent_or_host
from app.services.agent_tunnel_manager import (
    FRAME_CLOSE,
    FRAME_DATA,
    FRAME_OPEN,
    FRAME_OPEN_ACK,
    HEADER_STRUCT,
    AgentTunnelRegistry,
    AgentTunnelSession,
    authenticate_agent_websocket,
    create_agent_tunnel_token,
    get_available_releases,
    get_release_minio_url,
    get_release_zip_path,
    parse_agent_host,
    compare_versions,
    parse_semver,
    extract_manifest_version,
    sync_agent_releases_with_storage,
)
from app.services.crypto_utils import aes_encrypt


class FakeWebSocket:
    """Simula o transporte WebSocket binário do FastAPI para testes de integração."""

    def __init__(self, headers: dict[str, str] | None = None):
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.cookies: dict[str, str] = {}
        self.sent_frames: asyncio.Queue[bytes] = asyncio.Queue()
        self.incoming_frames: asyncio.Queue[bytes] = asyncio.Queue()
        self.closed = False

    async def send_bytes(self, data: bytes) -> None:
        await self.sent_frames.put(data)

    async def receive_bytes(self) -> bytes:
        data = await self.incoming_frames.get()
        if data == b"__EOF__":
            raise RuntimeError("WebSocket closed")
        return data

    async def close(self, code: int = 1000) -> None:
        self.closed = True
        await self.incoming_frames.put(b"__EOF__")


class TestAgentTunnelSecurityAndIsolation(unittest.IsolatedAsyncioTestCase):
    def test_parse_agent_host(self):
        self.assertEqual(parse_agent_host("agent"), "default")
        self.assertEqual(parse_agent_host("musta-agent"), "default")
        self.assertEqual(parse_agent_host("agent://luanda-pg-01"), "luanda-pg-01")
        self.assertEqual(parse_agent_host("musta-agent://erp_sql_02"), "erp_sql_02")
        self.assertIsNone(parse_agent_host("127.0.0.1"))
        self.assertIsNone(parse_agent_host("db.empresa.ao"))

    def test_authenticate_agent_websocket_security(self):
        user_id = 42
        valid_token = create_agent_tunnel_token(
            user_id=user_id, agent_id="luanda-pg-01", expires_days=7
        )

        fake_user = MagicMock()
        fake_user.id = user_id
        fake_user.is_active = True

        with patch("app.services.agent_tunnel_manager.SessionLocal") as mock_session_cls:
            mock_db = MagicMock()
            mock_db.query.return_value.filter.return_value.first.return_value = fake_user
            mock_session_cls.return_value = mock_db

            # 1. Sucesso com Authorization: Bearer + X-Musta-Agent-ID correspondente
            ws_ok = FakeWebSocket(
                headers={
                    "authorization": f"Bearer {valid_token}",
                    "x-musta-agent-id": "luanda-pg-01",
                }
            )
            auth = authenticate_agent_websocket(ws_ok)
            self.assertIsNotNone(auth)
            assert auth is not None
            self.assertEqual(auth[0], user_id)
            self.assertEqual(auth[1], "luanda-pg-01")

            # 2. Recusa se tentar usar um token de browser comum (typ="access")
            web_token = create_access_token({"sub": aes_encrypt(str(user_id))})
            ws_web = FakeWebSocket(headers={"authorization": f"Bearer {web_token}"})
            self.assertIsNone(authenticate_agent_websocket(ws_web))

            # 3. Recusa se o header X-Musta-Agent-ID divergir do token emitido
            ws_mismatch = FakeWebSocket(
                headers={
                    "authorization": f"Bearer {valid_token}",
                    "x-musta-agent-id": "outro-agente",
                }
            )
            self.assertIsNone(authenticate_agent_websocket(ws_mismatch))

            # 4. Recusa se não enviar cabeçalho Authorization (mesmo com cookie)
            ws_no_header = FakeWebSocket(headers={})
            ws_no_header.cookies["access_token"] = valid_token
            self.assertIsNone(authenticate_agent_websocket(ws_no_header))

    def test_pairing_code_single_use_and_release_packages(self):
        registry = AgentTunnelRegistry()
        record = registry.create_pairing_code(
            user_id=55,
            agent_id="luanda-pg-01",
            target_db="127.0.0.1:5432",
            expires_days=15,
        )
        self.assertRegex(record.code, r"^[A-Z0-9]{4}-[A-Z0-9]{4}$")

        # 1ª troca tem sucesso e consome o código
        consumed = registry.consume_pairing_code(record.code)
        self.assertIsNotNone(consumed)
        assert consumed is not None
        self.assertEqual(consumed["user_id"], 55)
        self.assertEqual(consumed["agent_id"], "luanda-pg-01")
        self.assertFalse(registry.is_revoked(55, "luanda-pg-01", None))

        # 2ª tentativa com o mesmo código falha imediatamente (uso único)
        self.assertIsNone(registry.consume_pairing_code(record.code))

        # Validação dos pacotes compilados em dist/ + URLs do MinIO vindas do .env
        releases = get_available_releases()
        self.assertGreaterEqual(len(releases), 6)
        self.assertTrue(all(".zip" in r.get("download_url", "") for r in releases))
        self.assertTrue(all("minio_key" in r for r in releases))
        win_minio_url = get_release_minio_url("windows", "amd64")
        self.assertIsNotNone(win_minio_url)
        assert win_minio_url is not None
        self.assertIn("musta-link-windows-amd64.zip", win_minio_url)

        # Bloqueio estrito de whitelist contra Path Traversal
        self.assertIsNone(get_release_zip_path("../etc", "amd64"))
        self.assertIsNone(get_release_zip_path("windows", "../passwd"))
        self.assertIsNone(get_release_minio_url("../etc", "amd64"))
        self.assertIsNone(get_release_minio_url("windows", "../passwd"))

    async def test_strict_tenant_isolation_and_revocation(self):
        registry = AgentTunnelRegistry()
        ws_tenant_a = FakeWebSocket()
        session_a = AgentTunnelSession(
            user_id=101,
            agent_id="luanda-pg-01",
            target_db="127.0.0.1:5432",
            websocket=ws_tenant_a,
            token_jti="jti-tenant-a",
        )
        await session_a.start_local_listener()
        await registry.register(session_a)

        try:
            # Tenant A consegue aceder ao seu próprio agente
            self.assertIs(registry.get_session(101, "luanda-pg-01"), session_a)
            self.assertIs(registry.get_session(101, "default"), session_a)

            # Tenant B (user_id=202) NUNCA consegue aceder ao agente do Tenant A
            self.assertIsNone(registry.get_session(202, "luanda-pg-01"))
            self.assertIsNone(registry.get_session(202, "default"))

            # Chamada sem user_id (None) NUNCA consegue aceder a qualquer agente
            self.assertIsNone(registry.get_session(None, "luanda-pg-01"))

            with patch("app.config.dependencies.agent_tunnel_registry", registry):
                host, port = _resolve_agent_or_host("agent://luanda-pg-01", 5432, 101)
                self.assertEqual(host, "127.0.0.1")
                self.assertEqual(port, session_a.local_port)

                with self.assertRaises(HTTPException) as ctx:
                    _resolve_agent_or_host("agent://luanda-pg-01", 5432, 202)
                self.assertEqual(ctx.exception.status_code, 503)

            # Revogação no Control Plane encerra imediatamente a sessão ativa e bloqueia o JTI
            was_active = await registry.revoke_agent(101, "luanda-pg-01")
            self.assertTrue(was_active)
            self.assertTrue(ws_tenant_a.closed)
            self.assertIsNone(registry.get_session(101, "luanda-pg-01"))
            self.assertTrue(registry.is_revoked(101, "luanda-pg-01", "jti-tenant-a"))
        finally:
            await session_a.close()

    async def test_multiplexed_concurrent_streams_e2e(self):
        ws = FakeWebSocket()
        session = AgentTunnelSession(
            user_id=101,
            agent_id="luanda-pg-01",
            target_db="127.0.0.1:5432",
            websocket=ws,
        )
        port = await session.start_local_listener()
        recv_task = asyncio.create_task(session.receive_loop())

        async def simulated_go_agent():
            while not ws.closed:
                frame = await ws.sent_frames.get()
                f_type, conn_id, p_len = HEADER_STRUCT.unpack_from(frame, 0)
                payload = frame[HEADER_STRUCT.size : HEADER_STRUCT.size + p_len]

                if f_type == FRAME_OPEN:
                    ack = HEADER_STRUCT.pack(FRAME_OPEN_ACK, conn_id, 0)
                    await ws.incoming_frames.put(ack)
                elif f_type == FRAME_DATA:
                    reply_payload = f"[ack:{conn_id}]".encode() + payload
                    reply = (
                        HEADER_STRUCT.pack(FRAME_DATA, conn_id, len(reply_payload))
                        + reply_payload
                    )
                    await ws.incoming_frames.put(reply)
                elif f_type == FRAME_CLOSE:
                    close_f = HEADER_STRUCT.pack(FRAME_CLOSE, conn_id, 0)
                    await ws.incoming_frames.put(close_f)

        agent_task = asyncio.create_task(simulated_go_agent())

        try:
            async def run_client(idx: int) -> None:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                msg = f"SELECT {idx} FROM vendas_luanda;".encode()
                writer.write(msg)
                await writer.drain()

                data = await asyncio.wait_for(reader.read(1024), timeout=3.0)
                self.assertTrue(data.endswith(msg))
                self.assertTrue(data.startswith(b"[ack:"))

                writer.close()
                await writer.wait_closed()

            await asyncio.gather(*(run_client(i) for i in range(1, 7)))
            self.assertEqual(session.total_connections_handled, 6)
        finally:
            await session.close()
            agent_task.cancel()
            recv_task.cancel()



    def test_version_analysis_and_semver(self):
        # 1. Comparações de versão SemVer
        self.assertEqual(compare_versions("1.0.0", "1.0.0"), 0)
        self.assertEqual(compare_versions("0.9.5", "1.0.0"), -1)
        self.assertEqual(compare_versions("1.0.1", "1.0.0"), 1)
        self.assertEqual(compare_versions("v1.0.0", "1.0.0"), 0)
        self.assertEqual(compare_versions("1.0", "1.0.0"), 0)

        # 2. Extração de versão do manifesto releases.json
        self.assertEqual(extract_manifest_version({"version": "1.2.0"}), "1.2.0")
        self.assertEqual(extract_manifest_version([{"version": "1.0.0"}]), "1.0.0")
        self.assertIsNone(extract_manifest_version([]))

    def test_storage_releases_sync_detection(self):
        # Testa a lógica de detecção de versão no MinIO com mock de StorageService
        mock_storage = MagicMock()
        mock_storage.bucket = "meu-bucket"
        mock_storage.s3 = MagicMock()

        # Simula resposta do MinIO com releases.json versão 1.0.0 já existente
        import io
        mock_manifest_body = io.BytesIO(b'[{"version": "1.0.0"}]')
        mock_storage.s3.get_object.return_value = {"Body": mock_manifest_body}

        # Simula list_objects_v2 retornando todos os 8 ficheiros
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "releases/musta-link/releases.json"},
                    {"Key": "releases/musta-link/SHA256SUMS.txt"},
                    {"Key": "releases/musta-link/musta-link-windows-amd64.zip"},
                    {"Key": "releases/musta-link/musta-link-windows-arm64.zip"},
                    {"Key": "releases/musta-link/musta-link-linux-amd64.zip"},
                    {"Key": "releases/musta-link/musta-link-linux-arm64.zip"},
                    {"Key": "releases/musta-link/musta-link-darwin-amd64.zip"},
                    {"Key": "releases/musta-link/musta-link-darwin-arm64.zip"},
                ]
            }
        ]
        mock_storage.s3.get_paginator.return_value = paginator

        with patch("cloud.config.StorageService", return_value=mock_storage):
            result = sync_agent_releases_with_storage(force_upload=False)
            self.assertEqual(result.get("status"), "up_to_date")
            self.assertEqual(result.get("version"), "1.0.0")
            # Nenhum upload foi necessário pois a versão é igual e todos os ficheiros existem
            mock_storage.upload_file.assert_not_called()

if __name__ == "__main__":
    unittest.main()
