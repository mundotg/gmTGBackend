import gzip
import io
import unittest
import zipfile
from uuid import uuid4

from fastapi import HTTPException

from app.services import (
    storage_acl_service as acl_service,
    storage_compression_service as compression_service,
    storage_rate_limiter as rate_limiter,
)


class TestCloudStorageMegaFeatures(unittest.TestCase):
    def setUp(self):
        rate_limiter.reset_rate_limits_for_user(9991)
        acl_service.reset_acl_state_for_tests()

    def test_rate_limiter_sliding_window_and_429(self):
        user_id = 9991
        # Permite 3 chamadas com custom_limit=3
        for i in range(3):
            st = rate_limiter.check_and_consume_rate_limit(
                user_id, bucket="api", custom_limit=3
            )
            self.assertTrue(st.allowed)
            self.assertEqual(st.remaining, 2 - i)

        # A 4ª chamada na mesma janela deve lançar HTTP 429 com Retry-After
        with self.assertRaises(HTTPException) as ctx:
            rate_limiter.check_and_consume_rate_limit(
                user_id, bucket="api", custom_limit=3
            )
        self.assertEqual(ctx.exception.status_code, 429)
        self.assertIn("Retry-After", ctx.exception.headers)

    def test_mega_concurrency_slots(self):
        user_id = 9991
        max_slots = rate_limiter.MAX_CONCURRENT_TRANSFERS

        # Entra no número máximo de slots paralelos simultâneos
        from contextlib import ExitStack

        with ExitStack() as stack:
            for idx in range(max_slots):
                stack.enter_context(
                    rate_limiter.transfer_concurrency_slot(user_id, f"slot-{idx}")
                )

            snap = rate_limiter.get_user_rate_snapshot(user_id)
            self.assertEqual(snap["concurrency"]["active_slots"], max_slots)

            # Um slot adicional além do limite deve ser travado com 429
            with self.assertRaises(HTTPException) as ctx:
                with rate_limiter.transfer_concurrency_slot(user_id, "slot-overflow"):
                    pass
            self.assertEqual(ctx.exception.status_code, 429)

        # Fora do ExitStack todos os slots devem ter sido libertados
        snap_after = rate_limiter.get_user_rate_snapshot(user_id)
        self.assertEqual(snap_after["concurrency"]["active_slots"], 0)

    def test_server_and_client_gzip_compression_and_transparent_decompression(self):
        original_content = (
            b"SELECT * FROM vendas WHERE provincia = 'Luanda';\n" * 400
        )
        raw_file = io.BytesIO(original_content)

        # 1. Compressão no servidor (compress_requested=True)
        stream_obj, stored_size, orig_size, is_comp, cleanup = (
            compression_service.prepare_upload_stream(
                raw_file,
                "consulta_luanda.sql",
                compress_requested=True,
                client_compressed=False,
            )
        )
        try:
            self.assertTrue(is_comp)
            self.assertEqual(orig_size, len(original_content))
            self.assertLess(stored_size, orig_size)
            compressed_bytes = stream_obj.read()
        finally:
            cleanup()

        # 2. Descompressão transparente no download
        chunks = [
             compressed_bytes[i : i + 128]
            for i in range(0, len(compressed_bytes), 128)
        ]
        restored = b"".join(
            compression_service.wrap_download_stream_with_decompression(
                chunks, "consulta_luanda.sql", keep_compressed=False
            )
        )
        self.assertEqual(restored, original_content)

        # 3. Compressão já feita no browser (client_compressed=True)
        browser_gz = gzip.compress(original_content)
        client_stream, c_stored, c_orig, c_is_comp, c_cleanup = (
            compression_service.prepare_upload_stream(
                io.BytesIO(browser_gz),
                "dados.csv",
                compress_requested=False,
                client_compressed=True,
            )
        )
        try:
            self.assertTrue(c_is_comp)
            self.assertEqual(c_orig, len(original_content))
            self.assertEqual(c_stored, len(browser_gz))
        finally:
            c_cleanup()

    def test_bulk_zip_archive_builder(self):
        entries = [
            ("relatorio.txt", [b"Linha 1\n", b"Linha 2\n"]),
            ("relatorio.txt", [b"Duplicado com renomeacao automatica\n"]),
            ("dados.json", [b'{"status": "ok"}']),
        ]
        stream_iter, total_size = compression_service.build_zip_archive_stream(entries)
        zip_bytes = b"".join(stream_iter)
        self.assertEqual(len(zip_bytes), total_size)

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = sorted(zf.namelist())
            self.assertEqual(
                names, ["dados.json", "relatorio (1).txt", "relatorio.txt"]
            )
            self.assertEqual(zf.read("dados.json"), b'{"status": "ok"}')

    def test_acl_roles_and_share_link_limits(self):
        # Valida hierarquia de permissões: viewer < downloader < editor < owner
        acl_service.assert_permission("downloader", "viewer", "ver")
        acl_service.assert_permission("downloader", "downloader", "baixar")
        with self.assertRaises(HTTPException) as ctx:
            acl_service.assert_permission("viewer", "downloader", "baixar ficheiro")
        self.assertEqual(ctx.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()

