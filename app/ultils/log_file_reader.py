"""
Leitura do ficheiro `database_connector.log`.

Separado de `logger.py` (que o escreve) e das rotas (que o expõem): aqui não
há FastAPI nem base de dados, só ficheiro e bytes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

# Níveis aceites no filtro → como aparecem escritos na linha. O formato é
# "%(asctime)s - %(levelname)s - %(message)s", definido em logger.py.
NIVEIS = {
    "debug": "DEBUG",
    "info": "INFO",
    "success": "INFO",  # 'success' é registado como INFO pelo logger
    "warning": "WARNING",
    "error": "ERROR",
    "critical": "CRITICAL",
}


def normalizar_nivel(level: Optional[str]) -> Optional[str]:
    """
    Nome do nível como aparece no ficheiro, ou `None` se não houver filtro.

    Levanta `ValueError` para um nível desconhecido — a rota converte em 400.
    """
    if not level:
        return None

    nivel = NIVEIS.get(level.strip().lower())
    if nivel is None:
        raise ValueError(f"Nível inválido. Use um de: {', '.join(NIVEIS)}.")
    return nivel


def linha_corresponde(
    linha: str, nivel: Optional[str], procura: Optional[str]
) -> bool:
    if nivel and f" - {nivel} - " not in linha:
        return False
    if procura and procura.lower() not in linha.lower():
        return False
    return True


def ler_do_fim(
    caminho: Path,
    limite: int,
    nivel: Optional[str] = None,
    procura: Optional[str] = None,
) -> dict:
    """
    Últimas `limite` linhas que correspondem ao filtro, em ordem cronológica.

    Lê o ficheiro do fim para o início, em blocos, e pára assim que juntar
    linhas suficientes. Um log de 500 MB custa o mesmo que um de 5 KB — sem
    isto, um `read()` inteiro derrubava o processo por memória.
    """
    bloco = 64 * 1024
    encontradas: list[str] = []
    analisadas = 0
    resto = b""

    with caminho.open("rb") as ficheiro:
        ficheiro.seek(0, os.SEEK_END)
        posicao = tamanho = ficheiro.tell()

        while posicao > 0 and len(encontradas) < limite:
            passo = min(bloco, posicao)
            posicao -= passo
            ficheiro.seek(posicao)
            pedaco = ficheiro.read(passo) + resto

            linhas = pedaco.split(b"\n")
            # A primeira linha do bloco pode estar cortada a meio: guarda-se
            # para ser completada pelo bloco seguinte (que é o anterior no
            # ficheiro). No início do ficheiro já não há nada a completar.
            resto = linhas.pop(0) if posicao > 0 else b""

            for bruta in reversed(linhas):
                linha = bruta.decode("utf-8", errors="replace").rstrip("\r")
                if not linha.strip():
                    continue
                analisadas += 1
                if linha_corresponde(linha, nivel, procura):
                    encontradas.append(linha)
                    if len(encontradas) >= limite:
                        break

    encontradas.reverse()

    return {
        "linhas": encontradas,
        "total_retornado": len(encontradas),
        "linhas_analisadas": analisadas,
        # False = havia mais ficheiro por ler; sobe o `limit` para ver o resto.
        "inicio_do_ficheiro": posicao == 0,
        "tamanho_bytes": tamanho,
    }
