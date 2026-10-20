"""
Controlador de Vídeos e Recomendações do YouTube.
Endpoints para busca, detalhes, registro de histórico e recomendações personalizadas.
"""
from datetime import datetime
from typing import List, Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user_model import User
from app.models.video import Video
from app.models.watch_history import WatchHistory
from app.schemas.recommendation import RecommendationRead
from app.schemas.video import VideoRead, WatchHistoryCreate, WatchHistoryRead
from app.services.recommendation_service import recommendation_service
from app.services.youtube_service import youtube_service
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user

router = APIRouter(tags=["Vídeos & Recomendações"])


# ============================================================
# 🔍 BUSCA DE VÍDEOS
# ============================================================
@router.get(
    "/api/videos/search",
    response_model=List[VideoRead],
    summary="Busca vídeos no YouTube e persiste no banco",
)
@router.get(
    "/videos/search",
    response_model=List[VideoRead],
    include_in_schema=False,
)
def search_videos(
    q: str = Query(..., min_length=1, description="Termo de pesquisa"),
    limit: int = Query(20, ge=1, le=50, description="Quantidade máxima de resultados"),
    category_id: Optional[str] = Query(None, description="Filtro opcional por categoria"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Realiza busca de vídeos na API do YouTube com cache Redis, persistindo
    os novos vídeos encontrados no banco de dados local.
    """
    try:
        video_creates = youtube_service.search_videos(
            query=q,
            max_results=limit,
            category_id=category_id,
        )

        persisted_videos: List[Video] = []
        for vc in video_creates:
            v_orm = recommendation_service.upsert_video(db, vc)
            persisted_videos.append(v_orm)

        db.commit()
        return persisted_videos

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"Erro na rota de busca de vídeos: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocorreu um erro interno ao processar a busca de vídeos.",
        )


# ============================================================
# 📺 DETALHE DO VÍDEO
# ============================================================
@router.get(
    "/api/videos/{youtube_id}",
    response_model=VideoRead,
    summary="Detalhes de um vídeo específico",
)
@router.get(
    "/videos/{youtube_id}",
    response_model=VideoRead,
    include_in_schema=False,
)
def get_video_detail(
    youtube_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna os detalhes de um vídeo pelo seu ID do YouTube ou UUID local.
    Se não existir localmente, busca da API do YouTube e persiste.
    """
    # 1. Tentar buscar pelo youtube_id
    video = db.query(Video).filter(Video.youtube_id == youtube_id).first()

    # 2. Se for um UUID válido, tentar buscar pelo ID primário
    if not video:
        try:
            parsed_uuid = uuid.UUID(youtube_id)
            video = db.query(Video).filter(Video.id == parsed_uuid).first()
        except ValueError:
            pass

    # 3. Se não encontrar no banco, buscar via YouTube API
    if not video:
        video_create = youtube_service.get_video_details(youtube_id)
        if not video_create:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Vídeo com identificador '{youtube_id}' não foi encontrado.",
            )
        video = recommendation_service.upsert_video(db, video_create)
        db.commit()

    return video


# ============================================================
# ⏱️ REGISTRAR HISTÓRICO DE VISUALIZAÇÃO
# ============================================================
@router.post(
    "/api/videos/{id}/watch",
    response_model=WatchHistoryRead,
    status_code=status.HTTP_201_CREATED,
    summary="Registra histórico de visualização do vídeo",
)
@router.post(
    "/videos/{id}/watch",
    response_model=WatchHistoryRead,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
)
def register_watch_history(
    id: str,
    payload: WatchHistoryCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Salva uma entrada no histórico do utilizador informando a duração assistida
    e se o vídeo foi curtido.
    """
    # Localizar o vídeo por UUID ou youtube_id
    video = None
    try:
        parsed_uuid = uuid.UUID(id)
        video = db.query(Video).filter(Video.id == parsed_uuid).first()
    except ValueError:
        pass

    if not video:
        video = db.query(Video).filter(Video.youtube_id == id).first()

    if not video:
        # Tenta buscar os dados do vídeo se for um youtube_id novo
        video_create = youtube_service.get_video_details(id)
        if video_create:
            video = recommendation_service.upsert_video(db, video_create)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Vídeo não encontrado para registrar histórico.",
            )

    history = WatchHistory(
        id=uuid.uuid4(),
        user_id=current_user.id,
        video_id=video.id,
        watched_at=datetime.utcnow(),
        watch_duration=payload.watch_duration,
        liked=payload.liked,
    )
    db.add(history)

    # Se curtiu, incrementa métrica do vídeo
    if payload.liked:
        video.like_count = (video.like_count or 0) + 1

    db.commit()
    db.refresh(history)
    return history


# ============================================================
# 🎯 RECOMENDAÇÕES DO USUÁRIO
# ============================================================
@router.get(
    "/api/recommendations",
    response_model=List[RecommendationRead],
    summary="Lista recomendações personalizadas do usuário",
)
@router.get(
    "/recommendations",
    response_model=List[RecommendationRead],
    include_in_schema=False,
)
def list_recommendations(
    limit: int = Query(20, ge=1, le=100, description="Limite de recomendações"),
    category_id: Optional[str] = Query(None, description="Categoria opcional"),
    refresh: bool = Query(False, description="Forçar recálculo e ignorar cache"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna o feed de vídeos recomendados para o utilizador autenticado.
    Se já existirem recomendações válidas e refresh=False, devolve do banco.
    """
    try:
        recs = recommendation_service.get_recommendations(
            user_id=current_user.id,
            limit=limit,
            refresh=refresh,
            category_id=category_id,
            db=db,
        )
        return recs

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"Erro ao listar recomendações para utilizador {current_user.id}: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível gerar recomendações neste momento.",
        )


@router.post(
    "/api/recommendations/refresh",
    response_model=List[RecommendationRead],
    summary="Força a regeneração de recomendações",
)
@router.post(
    "/recommendations/refresh",
    response_model=List[RecommendationRead],
    include_in_schema=False,
)
def refresh_recommendations(
    limit: int = Query(20, ge=1, le=100),
    category_id: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Regenera forçadamente as recomendações do utilizador através do algoritmo
    e busca novos candidatos no YouTube Data API.
    """
    try:
        recs = recommendation_service.refresh_for_user(
            user_id=current_user.id,
            limit=limit,
            category_id=category_id,
            db=db,
        )
        return recs

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"Erro ao regenerar recomendações: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao regenerar recomendações.",
        )
