# app/routes/nexus_routes.py
"""
Rotas do Nexus Cognitiva:
- REST API para Publicações, Comentários, Reações, Gostos, Partilhas, Visualizações e Saves.
- WebSocket para Reações e Comentários em tempo real por artigo (/ws/posts/{post_id}).
- Server-Sent Events (SSE) para:
  1. Notificações e atualizações do Feed em tempo real (/sse/feed)
  2. Métricas dinâmicas do artigo (/sse/posts/{post_id}/metrics)
- Cache L1/L2 com invalidação automática nas escritas.
- Logs detalhados de auditoria e performance.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
from datetime import datetime
from typing import  AsyncGenerator, Dict, List, Optional, Set, Union

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Security,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from app.config.dotenv import get_env
from sqlalchemy import desc, func, or_
from sqlalchemy.orm import Session, joinedload

from app.auth import decode_token
from app.config.api_security import bearer_scheme, cookie_scheme
from app.config.cache_manager import CACHE_PREFIX, clear_cache
from app.database import get_db
from app.models.nexus_models import (
    NexusComment,
    NexusLike,
    NexusPost,
    NexusReaction,
    NexusSave,
    NexusShare,
    NexusView,
)
from app.models.user_model import User
from app.schemas.nexus_schemas import (
    AuthorInfo,
    NexusCommentCreateSchema,
    NexusCommentResponseSchema,
    NexusLikeToggleResponse,
    NexusPostCreateSchema,
    NexusPostListResponseSchema,
    NexusPostResponseSchema,
    NexusPostUpdateSchema,
    NexusReactionCreateSchema,
    NexusReactionResponseSchema,
    NexusSaveToggleResponse,
    NexusShareCreateSchema,
    NexusShareResponseSchema,
    NexusViewCreateSchema,
    NexusViewResponseSchema,
)
from app.services.crypto_utils import aes_decrypt
from app.ultils.get_id_by_token import (
    _get_sub_from_payload,
    get_current_user_id,
    ws_user_id,
)
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user

router = APIRouter(prefix="/nexus", tags=["Nexus Cognitiva"])

NEXUS_FRONTEND_URL = get_env("NEXUS_FRONTEND_URL", "").strip().rstrip("/")


def _get_nexus_article_url(post_id: Union[int, str]) -> str:
    """
    Constrói a URL de redirecionamento do artigo para notificações e partilhas.
    Se NEXUS_FRONTEND_URL estiver configurada com um domínio dedicado (ex: https://nexuscognitiva.com),
    gera o link correspondente.
    """
    if NEXUS_FRONTEND_URL:
        if NEXUS_FRONTEND_URL.startswith("http://") or NEXUS_FRONTEND_URL.startswith("https://"):
            return f"{NEXUS_FRONTEND_URL}/artigo/{post_id}"
        elif NEXUS_FRONTEND_URL.startswith("/"):
            return f"{NEXUS_FRONTEND_URL}/artigo/{post_id}"
    return f"/nexus-cognitiva/artigo/{post_id}"


# ============================================================
# 🌐 Gestor de Conexões WebSocket & SSE em Tempo Real
# ============================================================
class NexusConnectionManager:
    """
    Controla conexões ativas:
    - WebSockets agrupados por `post_id` para reações e comentários em tempo real.
    - Filas de SSE para eventos unidirecionais do Feed e métricas.
    """

    def __init__(self):
        # post_id -> set(WebSocket)
        self.post_rooms: Dict[int, Set[WebSocket]] = {}
        # Conexões WebSocket globais para notificações do Nexus
        self.notification_clients: Set[WebSocket] = set()
        # Lista de queues para SSE do feed
        self.feed_subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def connect_post(self, post_id: int, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            if post_id not in self.post_rooms:
                self.post_rooms[post_id] = set()
            self.post_rooms[post_id].add(websocket)
        log_message(
            f"🔌 [WebSocket] Cliente conectado ao post #{post_id} (Total na sala: {len(self.post_rooms[post_id])})",
            source="nexus_routes.py",
            level="info",
        )

    async def disconnect_post(self, post_id: int, websocket: WebSocket):
        async with self._lock:
            if post_id in self.post_rooms:
                self.post_rooms[post_id].discard(websocket)
                if not self.post_rooms[post_id]:
                    del self.post_rooms[post_id]
        log_message(
            f"🔌 [WebSocket] Cliente desconectado do post #{post_id}",
            source="nexus_routes.py",
            level="info",
        )

    async def broadcast_post(self, post_id: int, message: dict):
        """Envia mensagem para todos os clientes ativos a visualizar o post especificado."""
        async with self._lock:
            sockets = list(self.post_rooms.get(post_id, set()))
        if not sockets:
            return

        dead_sockets = []
        for ws in sockets:
            try:
                await ws.send_json(message)
            except Exception:
                dead_sockets.append(ws)

        if dead_sockets:
            async with self._lock:
                if post_id in self.post_rooms:
                    for ws in dead_sockets:
                        self.post_rooms[post_id].discard(ws)

    async def connect_notifications(self, websocket: WebSocket):
        """Conecta um cliente ao canal global de notificações do Nexus."""
        await websocket.accept()
        async with self._lock:
            self.notification_clients.add(websocket)
        log_message(
            f"🔔 [WebSocket] Cliente conectado ao canal de notificações (Total ativos: {len(self.notification_clients)})",
            source="nexus_routes.py",
            level="info",
        )

    async def disconnect_notifications(self, websocket: WebSocket):
        """Desconecta um cliente do canal global de notificações."""
        async with self._lock:
            self.notification_clients.discard(websocket)
        log_message(
            f"🔔 [WebSocket] Cliente desconectado das notificações",
            source="nexus_routes.py",
            level="info",
        )

    async def broadcast_notification(self, message: dict):
        """
        Dispara notificação em tempo real para TODOS os clientes conectados
        (tanto no canal global como em qualquer sala de artigo aberta).
        """
        async with self._lock:
            recipients = set(self.notification_clients)
            for room in self.post_rooms.values():
                recipients.update(room)

        if not recipients:
            return

        dead_sockets = []
        for ws in recipients:
            try:
                await ws.send_json(message)
            except Exception:
                dead_sockets.append(ws)

        if dead_sockets:
            async with self._lock:
                for ws in dead_sockets:
                    self.notification_clients.discard(ws)
                    for room in self.post_rooms.values():
                        room.discard(ws)

    def get_room_readers_count(self, post_id: int) -> int:
        return len(self.post_rooms.get(post_id, set()))

    async def register_feed_subscriber(self, queue: asyncio.Queue):
        async with self._lock:
            self.feed_subscribers.append(queue)

    async def unregister_feed_subscriber(self, queue: asyncio.Queue):
        async with self._lock:
            if queue in self.feed_subscribers:
                self.feed_subscribers.remove(queue)

    async def broadcast_feed_sse(self, event_type: str, data: dict):
        """Dispara evento SSE para todos os assinantes do Feed."""
        payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
        async with self._lock:
            queues = list(self.feed_subscribers)
        for q in queues:
            try:
                await q.put(payload)
            except Exception as e:
                log_message(f"⚠️ Erro ao enfileirar evento SSE: {e}", source="nexus_routes.py", level="warning")


ws_manager = NexusConnectionManager()


# ============================================================
# 🔑 Autenticação Opcional (para visitantes lerem o feed)
# ============================================================
def get_optional_user_id(
    access_token: Optional[str] = Security(cookie_scheme),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> Optional[int]:
    """Retorna o user_id se autenticado, ou None para visitantes anónimos."""
    token = access_token or (credentials.credentials if credentials else None)
    if not token:
        return None
    try:
        payload = decode_token(token)
        encrypted_sub = _get_sub_from_payload(payload)
        if not encrypted_sub:
            return None
        return int(aes_decrypt(encrypted_sub))
    except Exception:
        return None


# ============================================================
# 🧹 Invalidação de Cache
# ============================================================
def _invalidate_nexus_cache(post_id: Optional[int] = None) -> None:
    """Descarta cache das listagens e de artigos específicos após escrita."""
    try:
        clear_cache(f"{CACHE_PREFIX}list_nexus_posts:*")
        clear_cache(f"{CACHE_PREFIX}nexus_feed:*")
        if post_id:
            clear_cache(f"{CACHE_PREFIX}get_nexus_post:{post_id}:*")
        log_message(
            f"🧹 Cache do Nexus Cognitiva invalidado com sucesso (post_id={post_id})",
            source="nexus_routes.py",
            level="info",
        )
    except Exception as e:
        log_message(f"⚠️ Falha ao invalidar cache do Nexus: {e}", source="nexus_routes.py", level="warning")


def _calculate_read_time(text: str, read_time_override: Optional[int] = None) -> int:
    """
    Estima com rigor os minutos de leitura de um texto Markdown:
    - Se o autor definiu manualmente um tempo válido (>0), respeita essa preferência.
    - Velocidade média padrão para leitura técnica/editorial: 200 a 220 palavras por minuto.
    - Remove marcações de Markdown, pontuações excessivas e links para apurar as palavras reais.
    - Pondera blocos de código (~3 segundos por linha de código).
    - Arredondamento estatístico com round(), garantindo no mínimo 1 minuto.
    """
    if read_time_override and read_time_override > 0:
        return min(read_time_override, 180)
    if not text or not text.strip():
        return 1

    clean_text = re.sub(r'```[\s\S]*?```', '', text)
    clean_text = re.sub(r'#+|[*_~`>|\[\]\(\)]', ' ', clean_text)
    clean_text = re.sub(r'https?://\S+', '', clean_text)
    words = len(clean_text.split())

    code_blocks = re.findall(r'```[\s\S]*?```', text)
    code_lines = sum(len(cb.strip().split('\n')) - 2 for cb in code_blocks if len(cb.strip().split('\n')) > 2)
    code_time_minutes = (code_lines * 3) / 60.0

    total_minutes = (words / 220.0) + code_time_minutes
    return max(1, round(total_minutes))


def _build_author_info(user: Optional[User]) -> Optional[AuthorInfo]:
    if not user:
        return None
    cargo_nome = user.cargo.nome if user.cargo else (user.role.name if user.role else "Membro")
    return AuthorInfo(
        id=user.id,
        nome=user.nome,
        email=user.email,
        avatar_url=user.avatar_url,
        cargo=cargo_nome,
    )


# ============================================================
# 📄 1. ROTAS DE PUBLICAÇÃO (POSTS)
# ============================================================

@router.get("/posts", response_model=NexusPostListResponseSchema)
def list_posts(
    page: int = Query(default=1, ge=1),
    size: int = Query(default=10, ge=1, le=50),
    search: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    content_type: Optional[str] = Query(default=None),
    min_age: Optional[str] = Query(default=None),
    only_verified: bool = Query(default=False),
    sort_by: str = Query(default="recent"),  # recent | views | bookmarks | likes
    current_user_id: Optional[int] = Depends(get_optional_user_id),
    db: Session = Depends(get_db),
):
    """
    Lista publicações com suporte a filtros facetados, ordenação dinâmica
    e verificação de likes/saves para o utilizador autenticado.
    """
    start_time = datetime.utcnow()
    query = db.query(NexusPost).filter(NexusPost.status == "published")

    # Filtro de texto
    if search and search.strip():
        term = f"%{search.strip().lower()}%"
        query = query.filter(
            or_(
                func.lower(NexusPost.title).like(term),
                func.lower(NexusPost.summary).like(term),
                func.lower(NexusPost.category).like(term),
            )
        )

    # Filtros facetados
    if category and category.lower() != "all":
        query = query.filter(NexusPost.category == category)

    if content_type and content_type.lower() != "all":
        query = query.filter(NexusPost.content_type == content_type)

    if min_age and min_age.lower() != "all":
        query = query.filter(NexusPost.min_age == min_age)

    if only_verified:
        query = query.filter(NexusPost.is_verified == True)

    # Total de registros correspondentes
    total = query.count()

    # Ordenação
    if sort_by == "views":
        query = query.order_by(desc(NexusPost.views_count), desc(NexusPost.published_at))
    elif sort_by == "bookmarks":
        query = query.order_by(desc(NexusPost.bookmarks_count), desc(NexusPost.published_at))
    elif sort_by == "likes":
        query = query.order_by(desc(NexusPost.likes_count), desc(NexusPost.published_at))
    else:
        query = query.order_by(desc(NexusPost.published_at))

    # Paginação
    offset = (page - 1) * size
    posts = (
        query.options(joinedload(NexusPost.author).joinedload(User.cargo))
        .offset(offset)
        .limit(size)
        .all()
    )

    # Verificação de saves e likes do utilizador logado em batch
    user_likes_set: Set[int] = set()
    user_saves_set: Set[int] = set()
    if current_user_id and posts:
        post_ids = [p.id for p in posts]
        likes = (
            db.query(NexusLike.post_id)
            .filter(NexusLike.user_id == current_user_id, NexusLike.post_id.in_(post_ids))
            .all()
        )
        user_likes_set = {row[0] for row in likes}

        saves = (
            db.query(NexusSave.post_id)
            .filter(NexusSave.user_id == current_user_id, NexusSave.post_id.in_(post_ids))
            .all()
        )
        user_saves_set = {row[0] for row in saves}

    items = []
    for p in posts:
        items.append(
            NexusPostResponseSchema(
                id=p.id,
                author_id=p.author_id,
                author=_build_author_info(p.author),
                title=p.title,
                summary=p.summary,
                body_markdown=p.body_markdown,
                category=p.category,
                subcategory=p.subcategory,
                content_type=p.content_type,
                min_age=p.min_age,
                status=p.status,
                read_time_minutes=p.read_time_minutes,
                cover_image=p.cover_image,
                video_url=p.video_url,
                external_url=p.external_url,
                tags=p.tags or [],
                sources=p.sources or [],
                mentions=p.mentions or [],
                is_verified=p.is_verified,
                views_count=p.views_count,
                likes_count=p.likes_count,
                shares_count=p.shares_count,
                bookmarks_count=p.bookmarks_count,
                comments_count=p.comments_count,
                published_at=p.published_at,
                created_at=p.created_at,
                updated_at=p.updated_at,
                is_liked=p.id in user_likes_set,
                is_saved=p.id in user_saves_set,
            )
        )

    duration = (datetime.utcnow() - start_time).total_seconds() * 1000
    log_message(
        f"📖 [Nexus] Listagem de posts (página {page}/{math.ceil(total / size) or 1}, {len(items)} itens, {duration:.1f}ms)",
        source="nexus_routes.py",
        level="info",
    )

    return NexusPostListResponseSchema(
        items=items,
        total=total,
        page=page,
        size=size,
        pages=math.ceil(total / size) if total > 0 else 1,
    )


@router.get("/posts/{post_id}", response_model=NexusPostResponseSchema)
def get_post(
    post_id: int,
    current_user_id: Optional[int] = Depends(get_optional_user_id),
    db: Session = Depends(get_db),
):
    """Retorna detalhes completos de uma publicação específica."""
    post = (
        db.query(NexusPost)
        .options(joinedload(NexusPost.author).joinedload(User.cargo))
        .filter(NexusPost.id == post_id)
        .first()
    )
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publicação não encontrada.")

    # Status de like e save para o leitor atual
    is_liked = False
    is_saved = False
    if current_user_id:
        is_liked = (
            db.query(NexusLike)
            .filter(NexusLike.user_id == current_user_id, NexusLike.post_id == post_id)
            .first()
            is not None
        )
        is_saved = (
            db.query(NexusSave)
            .filter(NexusSave.user_id == current_user_id, NexusSave.post_id == post_id)
            .first()
            is not None
        )

    return NexusPostResponseSchema(
        id=post.id,
        author_id=post.author_id,
        author=_build_author_info(post.author),
        title=post.title,
        summary=post.summary,
        body_markdown=post.body_markdown,
        category=post.category,
        subcategory=post.subcategory,
        content_type=post.content_type,
        min_age=post.min_age,
        status=post.status,
        read_time_minutes=post.read_time_minutes,
        cover_image=post.cover_image,
        video_url=post.video_url,
        external_url=post.external_url,
        tags=post.tags or [],
        sources=post.sources or [],
        mentions=post.mentions or [],
        is_verified=post.is_verified,
        views_count=post.views_count,
        likes_count=post.likes_count,
        shares_count=post.shares_count,
        bookmarks_count=post.bookmarks_count,
        comments_count=post.comments_count,
        published_at=post.published_at,
        created_at=post.created_at,
        updated_at=post.updated_at,
        is_liked=is_liked,
        is_saved=is_saved,
    )


@router.post("/posts", response_model=NexusPostResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_post(
    payload: NexusPostCreateSchema,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cria uma nova publicação no Nexus Cognitiva com validação e difusão SSE no Feed."""
    # Verificação de segurança de URLs
    for url_val in (payload.cover_image, payload.external_url):
        if url_val:
            lower = url_val.lower().strip()
            if lower.startswith("javascript:") or lower.startswith("data:") or lower.startswith("vbscript:"):
                raise HTTPException(status_code=400, detail="URL rejeitada por conter esquema inseguro.")

    # Selo de verificação automática para artigos científicos com DOI/fontes preenchidas
    has_valid_source = False
    if payload.sources:
        for s in payload.sources:
            if s.doi or (s.url and s.url.startswith("http")):
                has_valid_source = True
                break

    is_verified = payload.content_type == "cientifico" and has_valid_source
    read_time = _calculate_read_time(payload.body_markdown, payload.read_time_minutes)

    # Serialização limpa de JSON
    tags_json = payload.tags or []
    sources_json = [s.model_dump() for s in payload.sources] if payload.sources else []
    mentions_json = [m.model_dump() for m in payload.mentions] if payload.mentions else []

    post = NexusPost(
        author_id=user.id,
        title=payload.title.strip(),
        summary=payload.summary.strip() if payload.summary else None,
        body_markdown=payload.body_markdown,
        category=payload.category,
        subcategory=payload.subcategory,
        content_type=payload.content_type,
        min_age=payload.min_age,
        status="published",
        read_time_minutes=read_time,
        cover_image=payload.cover_image,
        video_url=payload.video_url,
        external_url=payload.external_url,
        tags=tags_json,
        sources=sources_json,
        mentions=mentions_json,
        is_verified=is_verified,
        published_at=datetime.utcnow(),
    )

    db.add(post)
    db.commit()
    db.refresh(post)

    _invalidate_nexus_cache()

    log_message(
        f"📝 [Nexus] Novo artigo publicado #{post.id} ('{post.title[:30]}') por user_id={user.id} ({user.nome})",
        source="nexus_routes.py",
        level="info",
    )

    # Disparo de SSE para o feed ao vivo
    await ws_manager.broadcast_feed_sse(
        "new_post",
        {
            "id": post.id,
            "title": post.title,
            "category": post.category,
            "content_type": post.content_type,
            "author_name": user.nome,
            "published_at": post.published_at.isoformat(),
        },
    )

    # 🔔 Notificação via WebSocket para todos os utilizadores (convite para ler, comentar e partilhar)
    await ws_manager.broadcast_notification(
        {
            "event": "nexus_notification",
            "id": f"notif-post-{post.id}-{int(datetime.utcnow().timestamp())}",
            "type": "new_post",
            "title": "Novo Artigo Publicado!",
            "message": f"'{post.title}' foi publicado por {user.nome}. Venha conferir, comentar e partilhar!",
            "post_id": post.id,
            "post_title": post.title,
            "author_name": user.nome,
            "author_avatar": user.avatar_url,
            "action_url": _get_nexus_article_url(post.id),
            "created_at": datetime.utcnow().isoformat(),
            "call_to_action": "Dar uma olhada",
        }
    )

    return NexusPostResponseSchema(
        id=post.id,
        author_id=post.author_id,
        author=_build_author_info(user),
        title=post.title,
        summary=post.summary,
        body_markdown=post.body_markdown,
        category=post.category,
        subcategory=post.subcategory,
        content_type=post.content_type,
        min_age=post.min_age,
        status=post.status,
        read_time_minutes=post.read_time_minutes,
        cover_image=post.cover_image,
        video_url=post.video_url,
        external_url=post.external_url,
        tags=post.tags or [],
        sources=post.sources or [],
        mentions=post.mentions or [],
        is_verified=post.is_verified,
        views_count=0,
        likes_count=0,
        shares_count=0,
        bookmarks_count=0,
        comments_count=0,
        published_at=post.published_at,
        created_at=post.created_at,
        updated_at=post.updated_at,
        is_liked=False,
        is_saved=False,
    )


@router.put("/posts/{post_id}", response_model=NexusPostResponseSchema)
def update_post(
    post_id: int,
    payload: NexusPostUpdateSchema,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Atualiza um artigo (autor ou administrador)."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    # Autorização: apenas autor ou super admin
    is_admin = user.role and user.role.name in {"admin", "super_admin"}
    if post.author_id != user.id and not is_admin:
        raise HTTPException(status_code=403, detail="Não tem permissão para editar esta publicação.")

    if payload.title is not None:
        post.title = payload.title.strip()
    if payload.summary is not None:
        post.summary = payload.summary.strip()
    if payload.body_markdown is not None or payload.read_time_minutes is not None:
        if payload.body_markdown is not None:
            post.body_markdown = payload.body_markdown
        text_for_calc = payload.body_markdown if payload.body_markdown is not None else post.body_markdown
        post.read_time_minutes = _calculate_read_time(text_for_calc, payload.read_time_minutes)
    if payload.category is not None:
        post.category = payload.category
    if payload.subcategory is not None:
        post.subcategory = payload.subcategory
    if payload.content_type is not None:
        post.content_type = payload.content_type
    if payload.min_age is not None:
        post.min_age = payload.min_age
    if payload.status is not None:
        post.status = payload.status
    if payload.cover_image is not None:
        post.cover_image = payload.cover_image
    if payload.video_url is not None:
        post.video_url = payload.video_url
    if payload.external_url is not None:
        post.external_url = payload.external_url
    if payload.tags is not None:
        post.tags = payload.tags
    if payload.sources is not None:
        post.sources = [s.model_dump() for s in payload.sources]
    if payload.mentions is not None:
        post.mentions = [m.model_dump() for m in payload.mentions]

    post.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(post)

    _invalidate_nexus_cache(post_id)

    log_message(
        f"✏️ [Nexus] Artigo atualizado #{post.id} por user_id={user.id}",
        source="nexus_routes.py",
        level="info",
    )

    return NexusPostResponseSchema(
        id=post.id,
        author_id=post.author_id,
        author=_build_author_info(post.author),
        title=post.title,
        summary=post.summary,
        body_markdown=post.body_markdown,
        category=post.category,
        subcategory=post.subcategory,
        content_type=post.content_type,
        min_age=post.min_age,
        status=post.status,
        read_time_minutes=post.read_time_minutes,
        cover_image=post.cover_image,
        video_url=post.video_url,
        external_url=post.external_url,
        tags=post.tags or [],
        sources=post.sources or [],
        mentions=post.mentions or [],
        is_verified=post.is_verified,
        views_count=post.views_count,
        likes_count=post.likes_count,
        shares_count=post.shares_count,
        bookmarks_count=post.bookmarks_count,
        comments_count=post.comments_count,
        published_at=post.published_at,
        created_at=post.created_at,
        updated_at=post.updated_at,
    )


@router.delete("/posts/{post_id}", status_code=status.HTTP_200_OK)
def delete_post(
    post_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove uma publicação e recursos associados."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    is_admin = user.role and user.role.name in {"admin", "super_admin"}
    if post.author_id != user.id and not is_admin:
        raise HTTPException(status_code=403, detail="Não tem permissão para eliminar esta publicação.")

    db.delete(post)
    db.commit()
    _invalidate_nexus_cache(post_id)

    log_message(
        f"🗑️ [Nexus] Artigo removido #{post_id} por user_id={user.id}",
        source="nexus_routes.py",
        level="info",
    )
    return {"message": "Publicação eliminada com sucesso."}


# ============================================================
# 💬 2. ROTAS DE COMENTÁRIOS COM TRANSMISSÃO WEBSOCKET
# ============================================================

@router.get("/posts/{post_id}/comments", response_model=List[NexusCommentResponseSchema])
def list_comments(
    post_id: int,
    current_user_id: Optional[int] = Depends(get_optional_user_id),
    db: Session = Depends(get_db),
):
    """Lista todos os comentários de uma publicação."""
    comments = (
        db.query(NexusComment)
        .options(joinedload(NexusComment.user).joinedload(User.cargo))
        .filter(NexusComment.post_id == post_id, NexusComment.is_active == True)
        .order_by(NexusComment.created_at.asc())
        .all()
    )

    user_comment_likes: Set[int] = set()
    if current_user_id and comments:
        c_ids = [c.id for c in comments]
        likes = (
            db.query(NexusLike.comment_id)
            .filter(NexusLike.user_id == current_user_id, NexusLike.comment_id.in_(c_ids))
            .all()
        )
        user_comment_likes = {r[0] for r in likes}

    result = []
    for c in comments:
        result.append(
            NexusCommentResponseSchema(
                id=c.id,
                post_id=c.post_id,
                user_id=c.user_id,
                parent_id=c.parent_id,
                content=c.content,
                likes_count=c.likes_count,
                is_edited=c.is_edited,
                is_active=c.is_active,
                created_at=c.created_at,
                updated_at=c.updated_at,
                author=_build_author_info(c.user),
                is_liked=c.id in user_comment_likes,
            )
        )
    return result


@router.post("/posts/{post_id}/comments", response_model=NexusCommentResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_comment(
    post_id: int,
    payload: NexusCommentCreateSchema,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cria um comentário/resposta e propaga via WebSocket em tempo real para todos na sala."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    # Se for resposta, valida existência do parent
    if payload.parent_id:
        parent = db.query(NexusComment).filter(NexusComment.id == payload.parent_id, NexusComment.post_id == post_id).first()
        if not parent:
            raise HTTPException(status_code=404, detail="Comentário pai não encontrado.")

    comment = NexusComment(
        post_id=post_id,
        user_id=user.id,
        parent_id=payload.parent_id,
        content=payload.content.strip(),
    )
    db.add(comment)

    # Incrementa contador desnormalizado
    post.comments_count = (post.comments_count or 0) + 1
    db.commit()
    db.refresh(comment)

    _invalidate_nexus_cache(post_id)

    log_message(
        f"💬 [Nexus] Novo comentário #{comment.id} no post #{post_id} por user_id={user.id}",
        source="nexus_routes.py",
        level="info",
    )

    response_data = NexusCommentResponseSchema(
        id=comment.id,
        post_id=comment.post_id,
        user_id=comment.user_id,
        parent_id=comment.parent_id,
        content=comment.content,
        likes_count=0,
        is_edited=False,
        is_active=True,
        created_at=comment.created_at,
        updated_at=comment.updated_at,
        author=_build_author_info(user),
        is_liked=False,
    )

    # 📡 Emissão imediata via WebSocket para a sala do artigo
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "new_comment",
            "comment": response_data.model_dump(mode="json"),
            "comments_count": post.comments_count,
        },
    )

    # 🔔 Notificação global via WebSocket para convidar os utilizadores a participar na discussão
    snippet = comment.content.strip()
    if len(snippet) > 80:
        snippet = snippet[:80] + "..."

    await ws_manager.broadcast_notification(
        {
            "event": "nexus_notification",
            "id": f"notif-comm-{comment.id}-{int(datetime.utcnow().timestamp())}",
            "type": "new_comment",
            "title": f"Novo Comentário em '{post.title[:35]}...'",
            "message": f"{user.nome} comentou: \"{snippet}\". Junte-se à conversa e dê a sua opinião!",
            "post_id": post.id,
            "post_title": post.title,
            "comment_id": comment.id,
            "author_name": user.nome,
            "author_avatar": user.avatar_url,
            "action_url": _get_nexus_article_url(post.id),
            "created_at": datetime.utcnow().isoformat(),
            "call_to_action": "Participar na Discussão",
        }
    )

    return response_data


# ============================================================
# ❤️ 3. GOSTOS (LIKES) & REAÇÕES COM WEBSOCKET
# ============================================================

@router.post("/posts/{post_id}/like", response_model=NexusLikeToggleResponse)
async def toggle_post_like(
    post_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Alterna gosto (like/unlike) num post e notifica via WebSocket."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    existing = (
        db.query(NexusLike)
        .filter(NexusLike.user_id == user.id, NexusLike.post_id == post_id)
        .first()
    )

    liked = False
    if existing:
        db.delete(existing)
        post.likes_count = max(0, (post.likes_count or 1) - 1)
        liked = False
    else:
        new_like = NexusLike(user_id=user.id, post_id=post_id)
        db.add(new_like)
        post.likes_count = (post.likes_count or 0) + 1
        liked = True

    db.commit()
    _invalidate_nexus_cache(post_id)

    log_message(
        f"👍 [Nexus] Like alternado (post_id={post_id}, user_id={user.id}, liked={liked}, total={post.likes_count})",
        source="nexus_routes.py",
        level="info",
    )

    # Broadcast via WebSocket
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "post_likes_updated",
            "post_id": post_id,
            "likes_count": post.likes_count,
            "user_id": user.id,
            "liked": liked,
        },
    )

    return NexusLikeToggleResponse(liked=liked, likes_count=post.likes_count)


@router.post("/comments/{comment_id}/like", response_model=NexusLikeToggleResponse)
async def toggle_comment_like(
    comment_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Alterna gosto (like/unlike) num comentário e transmite em tempo real."""
    comment = db.query(NexusComment).filter(NexusComment.id == comment_id).first()
    if not comment:
        raise HTTPException(status_code=404, detail="Comentário não encontrado.")

    existing = (
        db.query(NexusLike)
        .filter(NexusLike.user_id == user.id, NexusLike.comment_id == comment_id)
        .first()
    )

    liked = False
    if existing:
        db.delete(existing)
        comment.likes_count = max(0, (comment.likes_count or 1) - 1)
        liked = False
    else:
        new_like = NexusLike(user_id=user.id, comment_id=comment_id)
        db.add(new_like)
        comment.likes_count = (comment.likes_count or 0) + 1
        liked = True

    db.commit()

    log_message(
        f"👍 [Nexus] Like de comentário alternado (comment_id={comment_id}, user_id={user.id}, liked={liked}, total={comment.likes_count})",
        source="nexus_routes.py",
        level="info",
    )

    # Broadcast via WebSocket para a sala do artigo
    await ws_manager.broadcast_post(
        comment.post_id,
        {
            "event": "comment_likes_updated",
            "comment_id": comment_id,
            "likes_count": comment.likes_count,
            "user_id": user.id,
            "liked": liked,
        },
    )

    return NexusLikeToggleResponse(liked=liked, likes_count=comment.likes_count)


@router.post("/posts/{post_id}/react", response_model=NexusReactionResponseSchema)
async def react_to_post(
    post_id: int,
    payload: NexusReactionCreateSchema,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Regista ou altera uma reação qualitativa ao post (ex: curious, insight, applaud).
    Notifica a sala WebSocket correspondente.
    """
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    # Verifica se já existe reação deste tipo pelo utilizador
    reaction = (
        db.query(NexusReaction)
        .filter(
            NexusReaction.user_id == user.id,
            NexusReaction.post_id == post_id,
            NexusReaction.reaction_type == payload.reaction_type,
        )
        .first()
    )

    if not reaction:
        reaction = NexusReaction(
            user_id=user.id,
            post_id=post_id,
            reaction_type=payload.reaction_type,
        )
        db.add(reaction)
        db.commit()
        db.refresh(reaction)

    # Totalização de reações no post
    summary = (
        db.query(NexusReaction.reaction_type, func.count(NexusReaction.id))
        .filter(NexusReaction.post_id == post_id)
        .group_by(NexusReaction.reaction_type)
        .all()
    )
    reactions_dict = {row[0]: row[1] for row in summary}

    # Transmite atualização em tempo real
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "reactions_updated",
            "post_id": post_id,
            "reactions": reactions_dict,
        },
    )

    return NexusReactionResponseSchema(
        id=reaction.id,
        reaction_type=reaction.reaction_type,
        post_id=reaction.post_id,
        comment_id=reaction.comment_id,
        user_id=reaction.user_id,
        created_at=reaction.created_at,
    )


# ============================================================
# 🔖 4. SAVES / ARTIGOS GUARDADOS
# ============================================================

@router.post("/posts/{post_id}/save", response_model=NexusSaveToggleResponse)
def toggle_save_post(
    post_id: int,
    collection_name: str = Query(default="default"),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Guarda ou remove um artigo da lista de salvos/favoritos do utilizador."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    existing = (
        db.query(NexusSave)
        .filter(NexusSave.user_id == user.id, NexusSave.post_id == post_id)
        .first()
    )

    saved = False
    if existing:
        db.delete(existing)
        post.bookmarks_count = max(0, (post.bookmarks_count or 1) - 1)
        saved = False
    else:
        new_save = NexusSave(
            user_id=user.id,
            post_id=post_id,
            collection_name=collection_name,
        )
        db.add(new_save)
        post.bookmarks_count = (post.bookmarks_count or 0) + 1
        saved = True

    db.commit()
    _invalidate_nexus_cache(post_id)

    log_message(
        f"🔖 [Nexus] Save alternado (post_id={post_id}, user_id={user.id}, saved={saved}, total={post.bookmarks_count})",
        source="nexus_routes.py",
        level="info",
    )
    return NexusSaveToggleResponse(
        saved=saved,
        bookmarks_count=post.bookmarks_count,
        collection_name=collection_name,
    )


# ============================================================
# 🔄 5. PARTILHAS & VISUALIZAÇÕES
# ============================================================

@router.post("/posts/{post_id}/share", response_model=NexusShareResponseSchema)
async def register_share(
    post_id: int,
    payload: NexusShareCreateSchema,
    request: Request,
    current_user_id: Optional[int] = Depends(get_optional_user_id),
    db: Session = Depends(get_db),
):
    """Regista partilha externa (WhatsApp, LinkedIn, cópia de link, etc.)."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")

    share = NexusShare(
        post_id=post_id,
        user_id=current_user_id,
        platform=payload.platform,
        ip_address=ip,
        user_agent=user_agent[:500] if user_agent else None,
    )
    db.add(share)
    post.shares_count = (post.shares_count or 0) + 1
    db.commit()
    db.refresh(share)

    # Notifica via WebSocket na sala do artigo
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "share_registered",
            "post_id": post_id,
            "shares_count": post.shares_count,
            "platform": payload.platform,
        },
    )

    return NexusShareResponseSchema(
        id=share.id,
        post_id=post_id,
        platform=share.platform,
        shares_count=post.shares_count,
    )


@router.post("/posts/{post_id}/view", response_model=NexusViewResponseSchema)
async def register_view(
    post_id: int,
    payload: NexusViewCreateSchema,
    request: Request,
    current_user_id: Optional[int] = Depends(get_optional_user_id),
    db: Session = Depends(get_db),
):
    """Regista visualização ou tempo de permanência na leitura (visitantes autenticados ou anónimos)."""
    post = db.query(NexusPost).filter(NexusPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Publicação não encontrada.")

    forwarded = request.headers.get("x-forwarded-for")
    ip = (forwarded.split(",")[0].strip() if forwarded else None) or request.headers.get("x-real-ip") or (request.client.host if request.client else None)
    user_agent = request.headers.get("user-agent")

    view = NexusView(
        post_id=post_id,
        user_id=current_user_id,
        read_time_seconds=payload.read_time_seconds or 0,
        ip_address=ip,
        user_agent=user_agent[:500] if user_agent else None,
    )
    db.add(view)
    post.views_count = (post.views_count or 0) + 1
    db.commit()
    db.refresh(view)

    # Invalida cache para que novas leituras tragam a métrica atualizada
    _invalidate_nexus_cache(post_id)

    log_message(
        f"👁️ [Nexus] Visualização registada no post #{post_id} (user_id={current_user_id or 'anónimo'}, ip={ip}, total={post.views_count})",
        source="nexus_routes.py",
        level="info",
    )

    # Transmissão via WebSocket para atualizar contador em tempo real
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "views_count_updated",
            "post_id": post_id,
            "views_count": post.views_count,
        },
    )

    return NexusViewResponseSchema(
        id=view.id,
        post_id=post_id,
        views_count=post.views_count,
    )


# ============================================================
# 🔌 6. WEBSOCKET PARA REAÇÕES E COMENTÁRIOS AO VIVO
# ============================================================

@router.websocket("/ws/posts/{post_id}")
async def post_reactions_comments_websocket(
    websocket: WebSocket,
    post_id: int,
):
    """
    Canal bidirecional WebSocket para o artigo:
    - Transmite reações e novos comentários em tempo real.
    - Suporta envio direto de ações pelo cliente (ex: {"action": "ping"}, {"action": "typing"}).
    - Reporta contagem de leitores simultâneos conectados à sala.
    """
    user_id = ws_user_id(websocket)
    await ws_manager.connect_post(post_id, websocket)

    # Notifica o cliente do estado inicial e leitores ativos
    readers = ws_manager.get_room_readers_count(post_id)
    await websocket.send_json({
        "event": "connected",
        "post_id": post_id,
        "active_readers": readers,
        "authenticated_user_id": user_id,
    })

    # Notifica outros leitores sobre nova presença
    await ws_manager.broadcast_post(
        post_id,
        {
            "event": "readers_count_changed",
            "post_id": post_id,
            "active_readers": readers,
        },
    )

    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                action = payload.get("action")

                if action == "ping":
                    await websocket.send_json({"event": "pong", "timestamp": datetime.utcnow().isoformat()})
                elif action == "typing":
                    await ws_manager.broadcast_post(
                        post_id,
                        {
                            "event": "user_typing",
                            "post_id": post_id,
                            "user_id": user_id,
                            "author_name": payload.get("author_name", "Alguém"),
                        },
                    )
            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        await ws_manager.disconnect_post(post_id, websocket)
        readers = ws_manager.get_room_readers_count(post_id)
        await ws_manager.broadcast_post(
            post_id,
            {
                "event": "readers_count_changed",
                "post_id": post_id,
                "active_readers": readers,
            },
        )
    except Exception as e:
        log_message(f"❌ Erro na WebSocket do post #{post_id}: {e}", source="nexus_routes.py", level="error")
        await ws_manager.disconnect_post(post_id, websocket)


# ============================================================
# 🔔 6.1. WEBSOCKET GLOBAL DE NOTIFICAÇÕES DO NEXUS
# ============================================================

@router.websocket("/ws/notifications")
async def nexus_notifications_websocket(websocket: WebSocket):
    """
    Canal WebSocket global para notificações em tempo real do Nexus Cognitiva:
    - Notifica instantaneamente novos artigos com convite para ler, comentar e partilhar.
    - Notifica novos comentários com convite para participar na discussão.
    """
    await ws_manager.connect_notifications(websocket)
    try:
        # Mensagem inicial de boas-vindas
        await websocket.send_json({
            "event": "notifications_connected",
            "message": "Canal de notificações do Nexus Cognitiva conectado com sucesso.",
            "timestamp": datetime.utcnow().isoformat(),
        })

        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                if payload.get("action") == "ping":
                    await websocket.send_json({"event": "pong", "timestamp": datetime.utcnow().isoformat()})
            except Exception:
                pass
    except WebSocketDisconnect:
        await ws_manager.disconnect_notifications(websocket)
    except Exception as e:
        log_message(f"❌ Erro no WebSocket de notificações: {e}", source="nexus_routes.py", level="error")
        await ws_manager.disconnect_notifications(websocket)


# ============================================================
# 📡 7. SERVER-SENT EVENTS (SSE) PARA FEED E MÉTRICAS
# ============================================================

@router.get("/sse/feed")
async def sse_feed_stream(request: Request):
    """
    Transmissão SSE unidirecional para o Feed do Nexus Cognitiva.
    Permite aos clientes receberem:
    - Notificações de novos artigos publicados em tempo real.
    - Pulso de atividade da plataforma (heartbeat periódico).
    """
    queue: asyncio.Queue = asyncio.Queue()
    await ws_manager.register_feed_subscriber(queue)

    async def event_generator() -> AsyncGenerator[str, None]:
        # Mensagem inicial de conexão estabelecida
        init_data = json.dumps({"connected": True, "time": datetime.utcnow().isoformat()})
        yield f"event: connected\ndata: {init_data}\n\n"

        try:
            while True:
                # Se o cliente desligou a aba/conexão
                if await request.is_disconnected():
                    break

                try:
                    # Espera eventos com timeout para heartbeat de 25 segundos
                    message = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield message
                except asyncio.TimeoutError:
                    # Heartbeat SSE regular para manter a ligação HTTP ativa
                    heartbeat = json.dumps({"ping": datetime.utcnow().isoformat()})
                    yield f": heartbeat {heartbeat}\n\n"

        except asyncio.CancelledError:
            pass
        finally:
            await ws_manager.unregister_feed_subscriber(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/sse/posts/{post_id}/metrics")
async def sse_post_metrics_stream(
    post_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Transmissão SSE de métricas ao vivo do artigo (leituras ativas, total views, likes).
    Ideal para autores ou dashboards com consumo leve via EventSource.
    """
    async def metrics_generator() -> AsyncGenerator[str, None]:
        last_metrics = None
        while True:
            if await request.is_disconnected():
                break

            try:
                # Consulta rápida das métricas atuais
                post = db.query(NexusPost.views_count, NexusPost.likes_count, NexusPost.shares_count, NexusPost.comments_count).filter(NexusPost.id == post_id).first()
                if not post:
                    yield f"event: error\ndata: {json.dumps({'error': 'Post not found'})}\n\n"
                    break

                readers = ws_manager.get_room_readers_count(post_id)
                current_metrics = {
                    "views": post[0],
                    "likes": post[1],
                    "shares": post[2],
                    "comments": post[3],
                    "active_readers": readers,
                    "timestamp": datetime.utcnow().isoformat(),
                }

                # Emite apenas se houve alteração ou a cada ciclo
                if current_metrics != last_metrics:
                    yield f"event: metrics_update\ndata: {json.dumps(current_metrics)}\n\n"
                    last_metrics = current_metrics

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_message(f"⚠️ Erro no SSE de métricas do post #{post_id}: {e}", source="nexus_routes.py", level="warning")
                await asyncio.sleep(10)

    return StreamingResponse(
        metrics_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
