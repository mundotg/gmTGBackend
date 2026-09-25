"""
Identificação do código que está realmente a correr.

Existe por causa de um problema concreto e recorrente: o servidor arranca
sem `--reload` (ver start.bat, que usa `uvicorn --workers 4`, e
main.py, que chama uvicorn.run sem reload). Editar ficheiros não afeta um
processo já iniciado — os workers mantêm os módulos carregados em
memória. O resultado é um traceback que aponta para linhas que já não
existem no ficheiro, e horas perdidas a procurar um bug já corrigido.

Com isto, `GET /health` diz qual é o commit em execução, e basta compará-lo
com `git rev-parse --short HEAD` para saber se é preciso reiniciar.

Lê o .git diretamente em vez de invocar o git: não depende do binário
estar instalado (não está na imagem Docker) nem custa um subprocesso.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

_RAIZ = Path(__file__).resolve().parent.parent

# Momento em que o processo carregou este módulo. Comparado com a data de
# alteração dos ficheiros, mostra se o código mudou depois do arranque.
BOOT_TIME = datetime.now(UTC)


def _ler_commit() -> str:
    """Commit atual, lido do .git. 'desconhecido' se não for um repositório."""
    git = _RAIZ / ".git"

    try:
        head = (git / "HEAD").read_text(encoding="utf-8").strip()

        # HEAD destacado: contém o próprio hash.
        if not head.startswith("ref:"):
            return head[:7]

        ref = head.split(" ", 1)[1].strip()
        ficheiro_ref = git / ref

        if ficheiro_ref.exists():
            return ficheiro_ref.read_text(encoding="utf-8").strip()[:7]

        # Ref empacotada (repositório com packed-refs).
        empacotadas = git / "packed-refs"
        if empacotadas.exists():
            for linha in empacotadas.read_text(encoding="utf-8").splitlines():
                if linha.endswith(f" {ref}"):
                    return linha.split(" ", 1)[0][:7]

    except Exception:  # noqa: S110 - diagnóstico não pode impedir o arranque
        pass

    return "desconhecido"


def _codigo_mais_recente() -> str | None:
    """
    Data do ficheiro .py mais recentemente alterado.

    Se for posterior ao BOOT_TIME, o código em disco já não é o que está a
    correr — exatamente o sintoma que este módulo existe para tornar óbvio.
    """
    try:
        mais_recente = max(
            (p.stat().st_mtime for p in (_RAIZ / "app").rglob("*.py")),
            default=None,
        )

        if mais_recente is None:
            return None

        return datetime.fromtimestamp(mais_recente, UTC).isoformat()

    except Exception:
        return None


COMMIT = _ler_commit()


def get_version_info() -> dict:
    """Informação de diagnóstico sobre o processo em execução."""
    alterado_em = _codigo_mais_recente()

    desatualizado = False
    if alterado_em:
        desatualizado = datetime.fromisoformat(alterado_em) > BOOT_TIME

    return {
        "commit": COMMIT,
        "python": sys.version.split()[0],
        "arrancou_em": BOOT_TIME.isoformat(),
        "codigo_alterado_em": alterado_em,
        # True → há ficheiros mais recentes que o arranque: reiniciar.
        "codigo_desatualizado": desatualizado,
    }
