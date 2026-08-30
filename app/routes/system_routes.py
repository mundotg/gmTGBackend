"""
🖥️ Estado real do sistema, para a aba Monitoramento & Sistema.

Substitui os números fixos que a aba mostrava — uptime, CPU, storage, versões,
latência — por leituras a sério. Um painel de monitorização que inventa valores
é pior do que não existir: dá confiança sem a merecer.

Nada aqui levanta por uma métrica indisponível. Se o psutil não conseguir ler o
disco, esse campo vem a `null` e o resto continua a responder — a aba tem de
funcionar precisamente quando algo está mal.
"""

from __future__ import annotations

import platform
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal, check_database_health, get_db, sync_engine
from app.models.user_model import User
from app.services import system_settings_service as definicoes
from app.ultils.logger import get_log_file_path, log_message
from app.ultils.permissions import get_current_user, require_permission
from app.version import get_version_info

router = APIRouter(prefix="/system", tags=["Sistema"])

# Leitura do estado: qualquer pessoa que possa ver a aba.
_PODE_VER = require_permission("settings:system", "logs:view", "admin:*")


# ══════════════════════════ modelos ══════════════════════════
class Recurso(BaseModel):
    percent: Optional[float] = None
    usado: Optional[int] = None
    total: Optional[int] = None
    detalhe: Optional[str] = None


class EstadoSistema(BaseModel):
    saudavel: bool
    ambiente: str
    uptime_segundos: float
    arrancou_em: str
    commit: str
    python: str
    plataforma: str
    codigo_desatualizado: bool

    cpu: Recurso
    memoria: Recurso
    disco: Recurso

    base_dados: Dict[str, Any]
    redis: Dict[str, Any]


class LinhaLog(BaseModel):
    texto: str
    nivel: Optional[str] = None


class DefinicaoSistema(BaseModel):
    key: str
    titulo: str
    descricao: str
    criticidade: str
    permissao: str
    ativa: bool
    valor: bool


# ══════════════════════════ métricas ══════════════════════════
def _recursos() -> Dict[str, Recurso]:
    """CPU, memória e disco. Cada um falha isolado dos outros."""
    resultado = {"cpu": Recurso(), "memoria": Recurso(), "disco": Recurso()}
    try:
        import psutil
    except ImportError:  # pragma: no cover
        for r in resultado.values():
            r.detalhe = "psutil não instalado"
        return resultado

    try:
        # interval=None devolve a média desde a chamada anterior, em vez de
        # bloquear o pedido durante um segundo a amostrar.
        resultado["cpu"] = Recurso(
            percent=psutil.cpu_percent(interval=None),
            detalhe=f"{psutil.cpu_count(logical=False) or '?'} núcleos / "
            f"{psutil.cpu_count(logical=True) or '?'} threads",
        )
    except Exception as e:  # noqa: BLE001
        resultado["cpu"] = Recurso(detalhe=str(e)[:120])

    try:
        mem = psutil.virtual_memory()
        resultado["memoria"] = Recurso(
            percent=mem.percent, usado=mem.used, total=mem.total
        )
    except Exception as e:  # noqa: BLE001
        resultado["memoria"] = Recurso(detalhe=str(e)[:120])

    try:
        # `get_log_file_path` pode devolver None (sem handler de ficheiro).
        # Nesse caso mede-se o disco do diretório de trabalho, que é onde a
        # aplicação escreve de qualquer forma.
        caminho_log = get_log_file_path()
        alvo = str(caminho_log.parent) if caminho_log else "."
        disco = psutil.disk_usage(alvo)
        resultado["disco"] = Recurso(
            percent=disco.percent, usado=disco.used, total=disco.total
        )
    except Exception as e:  # noqa: BLE001
        resultado["disco"] = Recurso(detalhe=str(e)[:120])

    return resultado


def _estado_base_dados() -> Dict[str, Any]:
    alcancavel, detalhe = check_database_health()

    versao = None
    latencia_ms = None
    if alcancavel:
        try:
            inicio = time.perf_counter()
            with sync_engine.connect() as conn:
                bruto = conn.execute(text("SELECT version()")).scalar()
            latencia_ms = round((time.perf_counter() - inicio) * 1000, 1)
            # "PostgreSQL 16.9, compiled by…" → "PostgreSQL 16.9"
            versao = str(bruto).split(",")[0] if bruto else None
        except Exception as e:  # noqa: BLE001
            detalhe = str(e)[:120]

    return {
        "alcancavel": alcancavel,
        "detalhe": detalhe,
        "versao": versao,
        "latencia_ms": latencia_ms,
        "dialeto": sync_engine.dialect.name,
    }


def _estado_redis() -> Dict[str, Any]:
    try:
        from app.config.redis import redis_client

        if not redis_client:
            return {"alcancavel": False, "detalhe": "cliente não configurado"}

        inicio = time.perf_counter()
        redis_client.ping()
        return {
            "alcancavel": True,
            "detalhe": "ok",
            "latencia_ms": round((time.perf_counter() - inicio) * 1000, 1),
        }
    except Exception as e:  # noqa: BLE001
        return {"alcancavel": False, "detalhe": str(e)[:120]}


# ══════════════════════════ rotas ══════════════════════════
@router.get("/status", response_model=EstadoSistema, summary="Estado do sistema")
def estado(ator: User = Depends(_PODE_VER)):
    versao = get_version_info()
    recursos = _recursos()
    base_dados = _estado_base_dados()

    arrancou_em = datetime.fromisoformat(versao["arrancou_em"])
    if arrancou_em.tzinfo is None:
        arrancou_em = arrancou_em.replace(tzinfo=timezone.utc)

    return EstadoSistema(
        # "Saudável" é a base de dados responder. O resto é informativo: com CPU
        # a 100% o serviço está lento, sem base de dados está em baixo.
        saudavel=base_dados["alcancavel"],
        ambiente=__import__("os").getenv("ENV", "development"),
        uptime_segundos=(datetime.now(timezone.utc) - arrancou_em).total_seconds(),
        arrancou_em=versao["arrancou_em"],
        commit=versao["commit"],
        python=versao["python"],
        plataforma=f"{platform.system()} {platform.release()}",
        codigo_desatualizado=bool(versao.get("codigo_desatualizado")),
        cpu=recursos["cpu"],
        memoria=recursos["memoria"],
        disco=recursos["disco"],
        base_dados=base_dados,
        redis=_estado_redis(),
    )


@router.get(
    "/settings",
    response_model=List[DefinicaoSistema],
    summary="Definições globais e valores atuais",
)
def listar_definicoes(db: Session = Depends(get_db), ator: User = Depends(_PODE_VER)):
    """
    O catálogo vem do backend, não de uma cópia no frontend.

    Inclui `ativa`: a False, a opção é guardada mas ainda não tem nada que a
    consuma, e a aba di-lo em vez de fingir que age.
    """
    return definicoes.descrever(db)


@router.patch(
    "/settings/{key}",
    response_model=DefinicaoSistema,
    summary="Alterar uma definição global",
)
def alterar_definicao(
    key: str,
    valor: bool = Body(..., embed=True),
    db: Session = Depends(get_db),
    ator: User = Depends(require_permission("settings:system")),
):
    try:
        definicao = definicoes.definicao(key)
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Definição '{key}' não existe.",
        )

    definicoes.definir(db, key, valor, ator_id=ator.id)

    return DefinicaoSistema(
        key=definicao.key,
        titulo=definicao.titulo,
        descricao=definicao.descricao,
        criticidade=definicao.criticidade,
        permissao=definicao.permissao,
        ativa=definicao.ativa,
        valor=valor,
    )


@router.get(
    "/logs/tail", response_model=List[LinhaLog], summary="Últimas linhas do log"
)
def cauda_do_log(
    limite: int = Query(20, ge=1, le=200),
    nivel: Optional[str] = Query(None),
    ator: User = Depends(_PODE_VER),
):
    """Alimenta o painel de terminal da aba com o log real do processo."""
    from app.ultils.log_file_reader import ler_do_fim, normalizar_nivel

    caminho = get_log_file_path()
    if caminho is None:
        return []

    try:
        dados = ler_do_fim(caminho, limite, normalizar_nivel(nivel))
        linhas = dados.get("linhas") or []
        return [
            LinhaLog(texto=str(linha), nivel=_nivel_da_linha(str(linha)))
            for linha in linhas
        ]
    except FileNotFoundError:
        return []
    except Exception as e:  # noqa: BLE001
        log_message(f"[system] falha a ler o log: {e}", "warning")
        return []


def _nivel_da_linha(linha: str) -> Optional[str]:
    """Nível inferido do texto, para o frontend poder colorir sem o reanalisar."""
    maiuscula = linha.upper()
    for nivel in ("CRITICAL", "ERROR", "WARNING", "SUCCESS", "DEBUG", "INFO"):
        if nivel in maiuscula:
            return nivel.lower()
    return None


@router.post("/cache/clear-all", summary="Limpar todo o cache da aplicação")
def limpar_todo_o_cache(ator: User = Depends(require_permission("settings:system"))):
    """
    Descarta o cache inteiro: Redis e memória do processo.

    Não perde dados — tudo o que está em cache é recalculável. O efeito é uma
    janela de pedidos mais lentos enquanto volta a encher.
    """
    from app.config.cache_manager import CACHE_PREFIX, MEMORY_CACHE
    from app.config.redis import clear_cache_by_prefix

    em_memoria = len(MEMORY_CACHE)
    MEMORY_CACHE.clear()

    try:
        no_redis = clear_cache_by_prefix(CACHE_PREFIX)
    except Exception as e:  # noqa: BLE001
        log_message(f"[system] falha a limpar o Redis: {e}", "error")
        no_redis = 0

    definicoes.limpar_cache()

    log_message(
        f"[system] {ator.email} limpou todo o cache "
        f"(Redis: {no_redis}, memória: {em_memoria})",
        "warning",
    )
    return {
        "removidos_redis": no_redis,
        "removidos_memoria": em_memoria,
        "mensagem": "Cache limpo. Os próximos pedidos vão à origem.",
    }
