"""
Identificação do código em execução.

Existe por causa de um problema real: o servidor corre sem --reload
(start.bat usa `uvicorn --workers 4`), pelo que um processo antigo
continua a servir código antigo. O sintoma é um traceback a apontar para
linhas que já não existem no ficheiro — difícil de diagnosticar sem esta
informação.
"""

import os
import time
from datetime import UTC, timedelta
from pathlib import Path

from app import version


def test_commit_coincide_com_o_git():
    raiz = Path(__file__).resolve().parent.parent
    head = (raiz / ".git" / "HEAD").read_text(encoding="utf-8").strip()

    assert version.COMMIT != "desconhecido", head
    assert len(version.COMMIT) == 7


def test_info_tem_os_campos_de_diagnostico():
    info = version.get_version_info()

    assert set(info) == {
        "commit",
        "python",
        "arrancou_em",
        "codigo_alterado_em",
        "codigo_desatualizado",
    }


def test_codigo_nao_esta_desatualizado_em_repouso():
    assert version.get_version_info()["codigo_desatualizado"] is False


def test_deteta_ficheiro_alterado_apos_o_arranque(monkeypatch):
    # É este o caso que importa: editar código com o servidor a correr.
    alvo = Path(version.__file__)
    original = alvo.stat().st_mtime

    try:
        futuro = time.time() + 60
        os.utime(alvo, (futuro, futuro))

        assert version.get_version_info()["codigo_desatualizado"] is True

    finally:
        os.utime(alvo, (original, original))

    assert version.get_version_info()["codigo_desatualizado"] is False


def test_boot_time_e_anterior_a_agora():
    from datetime import datetime

    assert version.BOOT_TIME <= datetime.now(UTC) + timedelta(seconds=1)
