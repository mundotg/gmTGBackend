"""
🗄️ Gestão e Administração de Cache do Sistema.

Permite:
- Pesquisar e listar chaves de cache (Redis L2 e Memória L1)
- Inspecionar e visualizar valores em cache
- Editar valores e TTL de entradas em cache a partir do frontend
- Eliminar chaves de cache individualmente ou em lote
- Limpar todo o cache do sistema com segurança
- Gerir políticas de cache por utilizador (dados locais e versão/geração)

Protegido pela permissão `settings:system`.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.config.cache_manager import (
    CACHE_ENABLED,
    CACHE_PREFIX,
    MEMORY_CACHE,
    MEMORY_CACHE_TTL,
    clear_cache,
)
from app.config.redis import (
    REDIS_PREFIX,
    _build_key,
    _deserialize,
    _serialize,
    clear_all_cache,
    get_cache_info,
    r,
    redis_client,
)
from app.config.user_cache_policy import (
    definir_dados_locais,
    limpar_cache_do_utilizador,
    obter_geracao,
    usa_dados_locais,
)
from app.database import get_db
from app.models.geral_model import Settings
from app.models.user_model import User
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user, require_permission

router = APIRouter(
    prefix="/system/cache",
    tags=["Cache & Dados Locais"],
    dependencies=[Depends(require_permission("settings:system"))],
)


# ══════════════════════════ MODELOS PYDANTIC ══════════════════════════

class EstadoCacheUtilizador(BaseModel):
    user_id: int
    nome: str
    email: str
    usar_dados_locais: bool = Field(
        description="False = ignora metadados em cache e lê sempre da origem."
    )
    geracao: int = Field(
        description="Versão do cache. Sobe a cada limpeza; as entradas antigas deixam de ser alcançáveis."
    )


class AlterarDadosLocais(BaseModel):
    usar_dados_locais: bool


class ResultadoLimpeza(BaseModel):
    user_id: int
    geracao: int
    mensagem: str


class UtilizadoresCachePaginados(BaseModel):
    items: List[EstadoCacheUtilizador]
    total: int
    page: int
    limit: int
    total_pages: int
    total_sem_dados_locais: int = Field(
        default=0,
        description="Quantos utilizadores (no sistema todo, não só nesta página) leem sempre da origem.",
    )


class CacheItemResumo(BaseModel):
    key: str
    source: str  # "redis" | "memory" | "both"
    tipo: str  # "json" | "string" | "dict" | "list" | "number" | "binary"
    ttl: Optional[int] = None
    size_bytes: int = 0
    preview: str = ""
    updated_at: Optional[str] = None
    user_id: Optional[int] = None
    user_nome: Optional[str] = None
    user_email: Optional[str] = None


class CacheItemDetalhe(BaseModel):
    key: str
    source: str
    tipo: str
    ttl: Optional[int] = None
    size_bytes: int = 0
    value: Any = None
    raw_str: str = ""
    timestamp: Optional[float] = None
    function: Optional[str] = None
    user_id: Optional[int] = None
    user_nome: Optional[str] = None
    user_email: Optional[str] = None


class CacheStats(BaseModel):
    redis_connected: bool
    redis_keys: int
    memory_keys: int
    total_keys: int
    used_memory_human: Optional[str] = "—"
    hit_rate: float = 0.0
    cache_enabled: bool = True
    prefix: str = "cache:"


class CacheListResponse(BaseModel):
    items: List[CacheItemResumo]
    total: int
    page: int
    limit: int
    total_pages: int
    stats: CacheStats


class CacheEditRequest(BaseModel):
    value: Any
    ttl: Optional[int] = None  # Segundos de expiração. Se omitido/None, mantém expiração anterior


class CacheBulkDeleteRequest(BaseModel):
    keys: List[str]


class RespostaOperacao(BaseModel):
    success: bool
    message: str
    removed: Optional[int] = None
    key: Optional[str] = None


# ══════════════════════════ HELPERS DE CACHE ══════════════════════════

def _deserializar_valor_redis(raw: bytes) -> Any:
    """Tenta deserializar usando o pipeline pickle/gzip do app, fallback para UTF-8/JSON."""
    if raw is None:
        return None
    try:
        val = _deserialize(raw)
        if val is not None:
            return val
    except Exception:
        pass

    try:
        texto = raw.decode("utf-8")
        try:
            return json.loads(texto)
        except Exception:
            return texto
    except UnicodeDecodeError:
        return f"<dado binário: {len(raw)} bytes>"


def _extrair_valor_e_meta(
    valor_bruto: Any,
) -> tuple[Any, Optional[str], Optional[float], Optional[int]]:
    """Se o valor estiver empacotado pelo decorador @cache_response, desempacota."""
    if isinstance(valor_bruto, dict) and "value" in valor_bruto and (
        "function" in valor_bruto or "timestamp" in valor_bruto or "user_id" in valor_bruto
    ):
        return (
            valor_bruto.get("value"),
            valor_bruto.get("function"),
            valor_bruto.get("timestamp"),
            valor_bruto.get("user_id"),
        )
    return valor_bruto, None, None, None


def _extrair_user_id_de_chave_ou_dado(chave: str, valor_bruto: Any = None) -> Optional[int]:
    """Tenta extrair o user_id do formato da chave, metadados de memória ou payload."""
    if valor_bruto is not None:
        _, _, _, uid_payload = _extrair_valor_e_meta(valor_bruto)
        if uid_payload is not None:
            try:
                return int(uid_payload)
            except (ValueError, TypeError):
                pass

    if chave in MEMORY_CACHE:
        uid_mem = MEMORY_CACHE[chave].get("user_id")
        if uid_mem is not None:
            try:
                return int(uid_mem)
            except (ValueError, TypeError):
                pass

    # Regex para apanhar user:12, user_12, usuario:12, u:12, user_id:12.
    # O lookbehind evita falsos positivos como "menu:5" (que acabaria em "u:5").
    m = re.search(
        r'(?<![a-z0-9])(?:user_id|usuario|user|u)[:_\-](\d+)', chave, re.IGNORECASE
    )
    if m:
        try:
            return int(m.group(1))
        except (ValueError, TypeError):
            pass

    return None


def _inspecionar_valor(valor: Any) -> tuple[str, str, int]:
    """Retorna (tipo, preview, size_bytes)."""
    if valor is None:
        return "null", "null", 0

    val_real, fn, _, _ = _extrair_valor_e_meta(valor)

    if isinstance(val_real, dict):
        tipo = f"dict({fn})" if fn else "json"
        try:
            texto = json.dumps(val_real, ensure_ascii=False, default=str)
            size = len(texto.encode("utf-8"))
            preview = texto[:180] + ("..." if len(texto) > 180 else "")
        except Exception:
            preview = str(val_real)[:180]
            size = len(preview.encode("utf-8"))
        return tipo, preview, size

    if isinstance(val_real, (list, tuple, set)):
        tipo = f"list({fn})" if fn else "list"
        try:
            texto = json.dumps(list(val_real), ensure_ascii=False, default=str)
            size = len(texto.encode("utf-8"))
            preview = texto[:180] + ("..." if len(texto) > 180 else "")
        except Exception:
            preview = str(val_real)[:180]
            size = len(preview.encode("utf-8"))
        return tipo, preview, size

    if isinstance(val_real, (int, float, bool)):
        tipo = "number" if not isinstance(val_real, bool) else "boolean"
        preview = str(val_real)
        size = len(preview.encode("utf-8"))
        return tipo, preview, size

    if isinstance(val_real, str):
        tipo = "string"
        size = len(val_real.encode("utf-8"))
        preview = val_real[:180] + ("..." if len(val_real) > 180 else "")
        return tipo, preview, size

    if isinstance(val_real, bytes):
        tipo = "binary"
        size = len(val_real)
        preview = f"<bytes: {size} bytes>"
        return tipo, preview, size

    tipo = type(val_real).__name__
    preview = str(val_real)[:180]
    size = len(preview.encode("utf-8"))
    return tipo, preview, size


def _obter_todas_as_chaves(search: str = "") -> list[tuple[str, str]]:
    """
    Retorna lista única de (chave, source: 'redis' | 'memory' | 'both')
    ordenada alfabeticamente.
    """
    chaves_dict: dict[str, str] = {}
    filtro = search.strip().lower()

    # 1. Chaves da Memória RAM (L1)
    for k in list(MEMORY_CACHE.keys()):
        if not filtro or filtro in k.lower():
            chaves_dict[k] = "memory"

    # 2. Chaves do Redis (L2)
    if redis_client:
        try:
            redis_client.ping()
            cursor = 0
            padrao = f"*{filtro}*" if filtro else "*"
            while True:
                cursor, batch = redis_client.scan(cursor=cursor, match=padrao, count=250)
                if batch:
                    for raw_k in batch:
                        k_str = (
                            raw_k.decode("utf-8", errors="replace")
                            if isinstance(raw_k, bytes)
                            else str(raw_k)
                        )
                        if k_str in chaves_dict:
                            chaves_dict[k_str] = "both"
                        else:
                            chaves_dict[k_str] = "redis"
                if cursor == 0 or len(chaves_dict) >= 3000:
                    break
        except Exception as e:
            log_message(f"[cache-admin] Erro ao varrer chaves do Redis: {e}", "warning")

    # Ordena alfabeticamente
    itens_ordenados = sorted(chaves_dict.items(), key=lambda x: x[0].lower())
    return itens_ordenados


def _carregar_estatisticas() -> CacheStats:
    """Carrega estatísticas gerais de funcionamento e ocupação do cache."""
    redis_online = False
    redis_keys_count = 0
    hit_rate = 0.0
    used_memory = "—"

    if redis_client:
        try:
            redis_client.ping()
            redis_online = True
            info = get_cache_info()
            hit_rate = float(info.get("hit_rate", 0.0))
            used_memory = str(info.get("used_memory", "—"))

            # Contagem aproximada de chaves no redis
            padrao = f"{REDIS_PREFIX}*"
            cursor = 0
            while True:
                cursor, batch = redis_client.scan(cursor=cursor, match=padrao, count=500)
                redis_keys_count += len(batch)
                if cursor == 0:
                    break
        except Exception:
            redis_online = False

    memory_keys_count = len(MEMORY_CACHE)

    return CacheStats(
        redis_connected=redis_online,
        redis_keys=redis_keys_count,
        memory_keys=memory_keys_count,
        total_keys=redis_keys_count + memory_keys_count,
        used_memory_human=used_memory,
        hit_rate=hit_rate,
        cache_enabled=CACHE_ENABLED,
        prefix=REDIS_PREFIX,
    )


# ══════════════════════════ ROTAS DE GESTÃO DE CHAVES ══════════════════════════

@router.get(
    "/stats",
    response_model=CacheStats,
    summary="Estatísticas globais do cache",
)
def obter_estatisticas_cache():
    """Devolve informações de estado, ocupação e taxa de acerto do Redis e Memória."""
    return _carregar_estatisticas()


@router.get(
    "/keys",
    response_model=CacheListResponse,
    summary="Listar e pesquisar entradas de cache",
)
def listar_chaves(
    search: Optional[str] = Query(default="", description="Filtrar por nome de chave ou padrão"),
    tipo: str = Query(default="all", description="Filtrar por camada: all | redis | memory"),
    user_id: Optional[int] = Query(default=None, description="Filtrar por ID de utilizador"),
    page: int = Query(default=1, ge=1, description="Número da página"),
    limit: int = Query(default=20, ge=1, le=100, description="Itens por página"),
    db: Session = Depends(get_db),
):
    """
    Lista e pagina entradas de cache do Redis e da Memória RAM.
    Permite pesquisar por substring ou prefixo, e filtrar por utilizador.
    """
    chaves = _obter_todas_as_chaves(search or "")

    # Filtro por tipo/camada
    if tipo == "redis":
        chaves = [c for c in chaves if c[1] in ("redis", "both")]
    elif tipo == "memory":
        chaves = [c for c in chaves if c[1] in ("memory", "both")]

    # Filtro por utilizador se fornecido
    if user_id is not None:
        chaves_filtradas = []
        chaves_para_verificar_redis = []
        for chave, source in chaves:
            uid = _extrair_user_id_de_chave_ou_dado(chave)
            if uid is not None:
                if uid == user_id:
                    chaves_filtradas.append((chave, source))
            elif source in ("redis", "both") and redis_client:
                chaves_para_verificar_redis.append((chave, source))

        if chaves_para_verificar_redis and redis_client:
            chunk_size = 50
            for i in range(0, len(chaves_para_verificar_redis), chunk_size):
                chunk = chaves_para_verificar_redis[i : i + chunk_size]
                chunk_keys = [c[0] for c in chunk]
                try:
                    raw_vals = r.mget(chunk_keys)
                    for (chk_key, chk_src), raw_val in zip(chunk, raw_vals):
                        if raw_val:
                            val_deser = _deserializar_valor_redis(raw_val)
                            uid_chk = _extrair_user_id_de_chave_ou_dado(chk_key, val_deser)
                            if uid_chk == user_id:
                                chaves_filtradas.append((chk_key, chk_src))
                except Exception:
                    pass

        chaves = chaves_filtradas

    total = len(chaves)
    total_pages = max(1, (total + limit - 1) // limit)
    offset = (page - 1) * limit
    fatia = chaves[offset : offset + limit]

    itens_preparados: List[Dict[str, Any]] = []
    uids_da_pagina = set()

    for chave, source in fatia:
        ttl: Optional[int] = None
        size_bytes = 0
        tipo_dado = "unknown"
        preview = ""
        valor_lido: Any = None

        # Tenta ler da memória primeiro se existir
        if source in ("memory", "both") and chave in MEMORY_CACHE:
            mem_entry = MEMORY_CACHE[chave]
            valor_lido = mem_entry.get("val")
            tempo_decorrido = time.time() - mem_entry.get("ts", time.time())
            ttl_config = mem_entry.get("ttl", MEMORY_CACHE_TTL)
            if ttl_config:
                ttl = max(0, int(ttl_config - tempo_decorrido))

        # Se não leu da memória, lê do Redis
        if valor_lido is None and source in ("redis", "both") and redis_client:
            try:
                raw = r.get(chave)
                if raw is not None:
                    valor_lido = _deserializar_valor_redis(raw)
                    ttl_redis = r.ttl(chave)
                    if ttl_redis is not None and ttl_redis >= 0:
                        ttl = ttl_redis
            except Exception:
                pass

        tipo_dado, preview, size_bytes = _inspecionar_valor(valor_lido)
        item_uid = _extrair_user_id_de_chave_ou_dado(chave, valor_lido)
        if item_uid:
            uids_da_pagina.add(item_uid)

        itens_preparados.append(
            {
                "chave": chave,
                "source": source,
                "tipo": tipo_dado,
                "ttl": ttl,
                "size_bytes": size_bytes,
                "preview": preview,
                "user_id": item_uid,
            }
        )

    users_map = {}
    if uids_da_pagina:
        try:
            usuarios = (
                db.query(User.id, User.nome, User.email)
                .filter(User.id.in_(uids_da_pagina))
                .all()
            )
            users_map = {u.id: (u.nome, u.email) for u in usuarios}
        except Exception as e:
            log_message(f"[cache-admin] Erro ao carregar utilizadores de cache: {e}", "warning")

    itens_resumo: List[CacheItemResumo] = []
    for item in itens_preparados:
        u_nome, u_email = users_map.get(item["user_id"], (None, None))
        itens_resumo.append(
            CacheItemResumo(
                key=item["chave"],
                source=item["source"],
                tipo=item["tipo"],
                ttl=item["ttl"],
                size_bytes=item["size_bytes"],
                preview=item["preview"],
                user_id=item["user_id"],
                user_nome=u_nome,
                user_email=u_email,
            )
        )

    stats = _carregar_estatisticas()

    return CacheListResponse(
        items=itens_resumo,
        total=total,
        page=page,
        limit=limit,
        total_pages=total_pages,
        stats=stats,
    )


@router.get(
    "/keys/{key_path:path}",
    response_model=CacheItemDetalhe,
    summary="Obter detalhe de uma chave de cache",
)
def obter_detalhe_chave(key_path: str, db: Session = Depends(get_db)):
    """
    Retorna o valor completo, TTL e metadados de uma chave de cache específica.
    """
    chave = unquote(key_path)
    source = "not_found"
    valor_lido: Any = None
    ttl: Optional[int] = None
    function_name: Optional[str] = None
    ts: Optional[float] = None

    # 1. Verifica Memória
    if chave in MEMORY_CACHE:
        mem_entry = MEMORY_CACHE[chave]
        valor_lido = mem_entry.get("val")
        tempo_decorrido = time.time() - mem_entry.get("ts", time.time())
        ttl_config = mem_entry.get("ttl", MEMORY_CACHE_TTL)
        if ttl_config:
            ttl = max(0, int(ttl_config - tempo_decorrido))
        ts = mem_entry.get("ts")
        source = "memory"

    # 2. Verifica Redis
    if redis_client:
        try:
            raw = r.get(chave)
            if raw is not None:
                redis_val = _deserializar_valor_redis(raw)
                source = "both" if source == "memory" else "redis"
                if valor_lido is None:
                    valor_lido = redis_val
                ttl_redis = r.ttl(chave)
                if ttl_redis is not None and ttl_redis >= 0:
                    ttl = ttl_redis
        except Exception as e:
            log_message(f"[cache-admin] Erro ao ler '{chave}' do Redis: {e}", "warning")

    if source == "not_found" or valor_lido is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chave de cache '{chave}' não encontrada ou expirada.",
        )

    val_real, fn, timestamp_empacotado, _ = _extrair_valor_e_meta(valor_lido)
    function_name = fn or function_name
    ts = timestamp_empacotado or ts

    item_uid = _extrair_user_id_de_chave_ou_dado(chave, valor_lido)
    u_nome, u_email = None, None
    if item_uid:
        try:
            u = db.query(User.id, User.nome, User.email).filter(User.id == item_uid).first()
            if u:
                u_nome, u_email = u.nome, u.email
        except Exception:
            pass

    tipo_dado, _, size_bytes = _inspecionar_valor(valor_lido)

    try:
        raw_str = json.dumps(val_real, ensure_ascii=False, indent=2, default=str)
    except Exception:
        raw_str = str(val_real)

    return CacheItemDetalhe(
        key=chave,
        source=source,
        tipo=tipo_dado,
        ttl=ttl,
        size_bytes=size_bytes,
        value=val_real,
        raw_str=raw_str,
        timestamp=ts,
        function=function_name,
        user_id=item_uid,
        user_nome=u_nome,
        user_email=u_email,
    )


@router.put(
    "/keys/{key_path:path}",
    response_model=RespostaOperacao,
    summary="Editar o valor ou TTL de uma chave de cache",
)
def editar_chave(
    key_path: str,
    corpo: CacheEditRequest,
    ator: User = Depends(get_current_user),
):
    """
    Atualiza o valor e/ou tempo de vida (TTL) de uma chave no Redis e na Memória RAM.
    """
    chave = unquote(key_path)
    novo_valor = corpo.value

    # Se o valor enviado for uma string que é um JSON válido, faz parse para gravar estruturado
    if isinstance(novo_valor, str):
        stripped = novo_valor.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                novo_valor = json.loads(stripped)
            except Exception:
                pass

    sucesso = False

    # 1. Atualiza na Memória RAM se a chave existir ou for chave de memória
    if chave in MEMORY_CACHE or not redis_client:
        MEMORY_CACHE[chave] = {
            "ts": time.time(),
            "val": novo_valor,
            "ttl": corpo.ttl or MEMORY_CACHE_TTL,
        }
        sucesso = True

    # 2. Atualiza no Redis
    if redis_client:
        try:
            # Se a chave já existia no redis, verifica se tinha o formato de entry empacotado
            raw_antigo = r.get(chave)
            if raw_antigo:
                val_antigo = _deserializar_valor_redis(raw_antigo)
                if isinstance(val_antigo, dict) and "function" in val_antigo:
                    val_antigo["value"] = novo_valor
                    val_antigo["timestamp"] = time.time()
                    novo_valor = val_antigo

            raw_bytes = _serialize(novo_valor)
            if corpo.ttl and corpo.ttl > 0:
                r.setex(chave, corpo.ttl, raw_bytes)
            else:
                ttl_atual = r.ttl(chave)
                if ttl_atual and ttl_atual > 0:
                    r.setex(chave, ttl_atual, raw_bytes)
                else:
                    r.set(chave, raw_bytes)
            sucesso = True
        except Exception as e:
            log_message(f"[cache-admin] Erro ao gravar chave '{chave}' no Redis: {e}", "error")
            if not sucesso:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Falha ao gravar no Redis: {e}",
                )

    log_message(
        f"[cache] {ator.email} editou a chave de cache '{chave}' (TTL={corpo.ttl})",
        "warning",
    )

    return RespostaOperacao(
        success=True,
        message=f"Chave de cache '{chave}' atualizada com sucesso.",
        key=chave,
    )


@router.delete(
    "/keys/{key_path:path}",
    response_model=RespostaOperacao,
    summary="Eliminar uma chave de cache",
)
def eliminar_chave(
    key_path: str,
    ator: User = Depends(get_current_user),
):
    """
    Remove uma chave de cache específica do Redis e da Memória RAM.
    """
    chave = unquote(key_path)

    # 1. Remove da Memória RAM
    MEMORY_CACHE.pop(chave, None)

    # 2. Remove do Redis
    if redis_client:
        try:
            r.delete(chave)
            # Remove também sem ou com o prefixo se aplicável
            if not chave.startswith(REDIS_PREFIX):
                r.delete(_build_key(chave))
        except Exception as e:
            log_message(f"[cache-admin] Erro ao eliminar chave '{chave}' do Redis: {e}", "warning")

    log_message(
        f"[cache] {ator.email} eliminou a chave de cache '{chave}'",
        "warning",
    )

    return RespostaOperacao(
        success=True,
        message=f"Chave '{chave}' eliminada com sucesso.",
        key=chave,
    )


@router.post(
    "/keys/delete-bulk",
    response_model=RespostaOperacao,
    summary="Eliminar múltiplas chaves de cache",
)
def eliminar_chaves_em_lote(
    corpo: CacheBulkDeleteRequest,
    ator: User = Depends(get_current_user),
):
    """
    Remove uma lista de chaves de cache em lote.
    """
    chaves = corpo.keys
    if not chaves:
        return RespostaOperacao(success=True, message="Nenhuma chave fornecida.", removed=0)

    removidas = 0

    # 1. Memória RAM
    for k in chaves:
        if k in MEMORY_CACHE:
            MEMORY_CACHE.pop(k, None)
            removidas += 1

    # 2. Redis
    if redis_client:
        try:
            chaves_redis = []
            for k in chaves:
                chaves_redis.append(k)
                if not k.startswith(REDIS_PREFIX):
                    chaves_redis.append(_build_key(k))
            if chaves_redis:
                r.delete(*chaves_redis)
                removidas += len(chaves)
        except Exception as e:
            log_message(f"[cache-admin] Erro na eliminação em lote no Redis: {e}", "warning")

    log_message(
        f"[cache] {ator.email} eliminou {len(chaves)} chaves de cache em lote",
        "warning",
    )

    return RespostaOperacao(
        success=True,
        message=f"{len(chaves)} chaves de cache eliminadas com sucesso.",
        removed=len(chaves),
    )


@router.post(
    "/clear-all",
    summary="Limpar todo o cache do sistema",
)
def limpar_todo_o_cache(
    ator: User = Depends(get_current_user),
):
    """
    Esvazia totalmente o cache em memória (L1) e o namespace de cache no Redis (L2).
    """
    removidos_memoria = len(MEMORY_CACHE)
    MEMORY_CACHE.clear()

    removidos_redis = 0
    if redis_client:
        try:
            removidos_redis = clear_all_cache()
        except Exception as e:
            log_message(f"[cache-admin] Erro ao limpar cache no Redis: {e}", "error")

    log_message(
        f"[cache] {ator.email} limpou TODO o cache do sistema (Redis: {removidos_redis}, RAM: {removidos_memoria})",
        "warning",
    )

    return {
        "removidos_redis": removidos_redis,
        "removidos_memoria": removidos_memoria,
        "mensagem": f"Todo o cache foi limpo com sucesso ({removidos_redis} chaves Redis, {removidos_memoria} itens em memória).",
    }


# ══════════════════════════ ROTAS DE UTILIZADOR (LEGADO PRESERVADO) ══════════════════════════

def _utilizador_ou_404(db: Session, user_id: int) -> User:
    utilizador = db.query(User).filter(User.id == user_id).first()
    if not utilizador:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Utilizador {user_id} não encontrado.",
        )
    return utilizador


def _settings_de(db: Session, user_id: int) -> Optional[Settings]:
    return db.query(Settings).filter(Settings.user_id == user_id).first()


def _estado(
    db: Session,
    utilizador: User,
    definicoes: Optional[Settings] = None,
    definicoes_carregadas: bool = False,
) -> EstadoCacheUtilizador:
    if not definicoes_carregadas:
        definicoes = _settings_de(db, utilizador.id)
    if definicoes is not None:
        ativo = bool(definicoes.usar_dados_locais)
    else:
        ativo = usa_dados_locais(utilizador.id)

    return EstadoCacheUtilizador(
        user_id=utilizador.id,
        nome=utilizador.nome,
        email=utilizador.email,
        usar_dados_locais=ativo,
        geracao=obter_geracao(utilizador.id),
    )


@router.get(
    "/users",
    response_model=UtilizadoresCachePaginados,
    summary="Estado de cache dos utilizadores (paginado, com pesquisa)",
)
def listar_estados(
    search: Optional[str] = Query(default="", description="Pesquisar por nome ou email"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Alimenta a aba de políticas e o seletor de utilizador do explorador.

    Paginado: com muitos utilizadores, devolver todos de uma vez (e fazer
    uma consulta de Settings por cada um) tornava a aba lenta.
    """
    consulta = db.query(User)
    termo = (search or "").strip()
    if termo:
        padrao = f"%{termo}%"
        consulta = consulta.filter(or_(User.nome.ilike(padrao), User.email.ilike(padrao)))

    total = consulta.count()
    total_pages = max(1, (total + limit - 1) // limit)
    utilizadores = (
        consulta.order_by(User.nome, User.id).offset((page - 1) * limit).limit(limit).all()
    )

    # Settings da página numa só consulta (evita N+1).
    ids = [u.id for u in utilizadores]
    settings_por_user: Dict[int, Settings] = {}
    if ids:
        for s in db.query(Settings).filter(Settings.user_id.in_(ids)).all():
            settings_por_user[s.user_id] = s

    sem_dados_locais = (
        db.query(func.count(Settings.user_id))
        .filter(Settings.usar_dados_locais.is_(False))
        .scalar()
        or 0
    )

    return UtilizadoresCachePaginados(
        items=[
            _estado(db, u, settings_por_user.get(u.id), definicoes_carregadas=True)
            for u in utilizadores
        ],
        total=total,
        page=page,
        limit=limit,
        total_pages=total_pages,
        total_sem_dados_locais=int(sem_dados_locais),
    )


@router.get(
    "/users/{user_id}",
    response_model=EstadoCacheUtilizador,
    summary="Estado de cache de um utilizador",
)
def obter_estado(user_id: int, db: Session = Depends(get_db)):
    return _estado(db, _utilizador_ou_404(db, user_id))


@router.get(
    "/users/{user_id}/keys",
    response_model=CacheListResponse,
    summary="Listar as entradas de cache de um utilizador",
)
def listar_chaves_do_utilizador(
    user_id: int,
    search: Optional[str] = Query(default=""),
    tipo: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Atalho para `/keys?user_id=...`, com 404 se o utilizador não existir."""
    _utilizador_ou_404(db, user_id)
    return listar_chaves(
        search=search, tipo=tipo, user_id=user_id, page=page, limit=limit, db=db
    )


@router.post(
    "/users/{user_id}/clear",
    response_model=ResultadoLimpeza,
    summary="Limpar o cache de um utilizador",
)
def limpar(
    user_id: int,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    Invalida tudo o que este utilizador tem em cache subindo a sua geração.
    """
    utilizador = _utilizador_ou_404(db, user_id)
    geracao = limpar_cache_do_utilizador(utilizador.id)

    log_message(
        f"[cache] {ator.email} limpou o cache de {utilizador.email} (geração {geracao})",
        "warning",
    )
    return ResultadoLimpeza(
        user_id=utilizador.id,
        geracao=geracao,
        mensagem=f"Cache de {utilizador.nome} limpo.",
    )


@router.patch(
    "/users/{user_id}",
    response_model=EstadoCacheUtilizador,
    summary="Ligar ou desligar dados locais para um utilizador",
)
def alterar_dados_locais(
    user_id: int,
    corpo: AlterarDadosLocais,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    utilizador = _utilizador_ou_404(db, user_id)

    definicoes = _settings_de(db, user_id)
    if definicoes is None:
        definicoes = Settings(user_id=user_id)
        db.add(definicoes)

    definicoes.usar_dados_locais = corpo.usar_dados_locais
    db.commit()

    definir_dados_locais(user_id, corpo.usar_dados_locais)

    log_message(
        f"[cache] {ator.email} definiu dados locais de {utilizador.email} para {corpo.usar_dados_locais}",
        "info",
    )
    return _estado(db, utilizador)
