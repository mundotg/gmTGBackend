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
