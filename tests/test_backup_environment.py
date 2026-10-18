import pytest
from unittest.mock import patch, MagicMock
from importantConfig.db_backup_restore import (
    get_binary_info,
    validate_backup_environment,
    _run_command_async,
)

def test_get_binary_info_parses_postgres_version():
    with patch("importantConfig.db_backup_restore._resolve_binary", return_value="/usr/bin/pg_dump"):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)\n",
                stderr="",
            )
            info = get_binary_info("pg_dump")
            assert info["installed"] is True
            assert info["path"] == "/usr/bin/pg_dump"
            assert info["version"] == "18.6"
            assert info["major_version"] == 18


def test_get_binary_info_missing_binary():
    with patch("importantConfig.db_backup_restore._resolve_binary", side_effect=RuntimeError("Não encontrado")):
        info = get_binary_info("non_existent_binary")
        assert info["installed"] is False
        assert info["version"] is None
        assert info["path"] is None


def test_validate_backup_environment_structure():
    env = validate_backup_environment()
    assert "environment" in env
    assert "tools" in env
    assert "pg_dump" in env["tools"]
    assert "mysqldump" in env["tools"]
    assert "status" in env
    assert "warnings" in env
    assert isinstance(env["warnings"], list)


import asyncio

def test_run_command_async_detects_server_version_mismatch():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = b""
    mock_result.stderr = (
        b"pg_dump: error: aborting because of server version mismatch\n"
        b"pg_dump: detail: server version: 18.6 (Debian 18.6-1.pgdg13+2); pg_dump version: 17.11 (Debian 17.11-1.pgdg12+2)\n"
    )

    with patch("subprocess.run", return_value=mock_result):
        with pytest.raises(RuntimeError) as exc_info:
            asyncio.run(_run_command_async(["pg_dump"], {}, "backup pg"))

        msg = str(exc_info.value)
        assert "Incompatibilidade de versões do PostgreSQL" in msg
        assert "18.6" in msg
        assert "17.11" in msg
        assert "ARG PG_CLIENT_VERSION=18" in msg


from unittest.mock import AsyncMock
from importantConfig.db_backup_restore import backup_database
from app.models.connection_models import DBConnection

def test_backup_database_falls_back_to_python_sql():
    mock_conn = MagicMock(spec=DBConnection)
    mock_conn.type = "postgresql"
    mock_conn.database_name = "testdb"
    mock_conn.username = "testuser"
    mock_conn.host = "localhost"
    mock_conn.port = 5432
    mock_conn.password = "testpass"

    mock_db = MagicMock()

    with patch("importantConfig.db_backup_restore.get_connection_id_async", new_callable=AsyncMock) as mock_get_conn:
        mock_get_conn.return_value = mock_conn
        with patch("importantConfig.db_backup_restore._resolve_binary", return_value="/usr/bin/pg_dump"):
            with patch("importantConfig.db_backup_restore._run_command_async", side_effect=RuntimeError("server version mismatch")):
                with patch("importantConfig.db_backup_restore_sql.python_postgres_backup", new_callable=AsyncMock) as mock_py_backup:
                    mock_py_backup.return_value = "backups/testdb_backup.sql"
                    with patch("importantConfig.db_backup_restore._file_exists_and_nonempty"):
                        with patch("importantConfig.db_backup_restore._compress_file_async", new_callable=AsyncMock) as mock_compress:
                            mock_compress.return_value = "backups/testdb_backup.sql.gz"
                            
                            res = asyncio.run(backup_database(mock_db, user_id=1, connection_id=1, compress=True))
                            assert res == "backups/testdb_backup.sql.gz"
                            mock_py_backup.assert_called_once()

