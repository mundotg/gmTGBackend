"""
Serviço de recomendação inteligente de vídeos do YouTube.
Calcula scores personalizados com base em histórico, engajamento e recência.
"""
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import uuid

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session, joinedload

from app.core.config import (
    RECOMMENDATION_CANDIDATE_LIMIT,
    RECOMMENDATION_DEFAULT_LIMIT,
    RECOMMENDATION_ENGAGEMENT_BENCHMARK,
    RECOMMENDATION_FALLBACK_QUERIES,
    RECOMMENDATION_FAVORITE_CHANNELS_LIMIT,
    RECOMMENDATION_HIGH_SCORE_THRESHOLD,
    RECOMMENDATION_HISTORY_LIMIT,
    RECOMMENDATION_MIN_CACHED_COUNT,
    RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT,
    RECOMMENDATION_RECENCY_DAYS_HORIZON,
    RECOMMENDATION_RECENCY_MIN_FLOOR,
    RECOMMENDATION_SIMILARITY_CATEGORY_MATCH,
    RECOMMENDATION_SIMILARITY_CHANNEL_MATCH,
    RECOMMENDATION_SIMILARITY_COLD_START,
    RECOMMENDATION_SIMILARITY_DEFAULT,
    RECOMMENDATION_TTL_HOURS,
    RECOMMENDATION_WEIGHT_CATEGORY,
    RECOMMENDATION_WEIGHT_ENGAGEMENT,
    RECOMMENDATION_WEIGHT_RECENCY,
)
from app.models.recommendation import Recommendation
from app.models.video import Video
from app.models.watch_history import WatchHistory
from app.schemas.video import VideoCreate
from app.services.youtube_service import youtube_service
from app.ultils.logger import log_message


class RecommendationService:
    """Motor de cálculo e gestão de recomendações personalizadas."""

    def __init__(self):
        self.ttl_hours = RECOMMENDATION_TTL_HOURS
        self.history_limit = RECOMMENDATION_HISTORY_LIMIT
        self.candidate_limit = RECOMMENDATION_CANDIDATE_LIMIT
        self.fallback_queries = [
            q.strip() for q in RECOMMENDATION_FALLBACK_QUERIES.split(",") if q.strip()
        ]
        self.favorite_channels_limit = RECOMMENDATION_FAVORITE_CHANNELS_LIMIT
        self.query_channel_terms_limit = RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT
        self.min_cached_count = RECOMMENDATION_MIN_CACHED_COUNT
        self.high_score_threshold = RECOMMENDATION_HIGH_SCORE_THRESHOLD

        self.weight_engagement = RECOMMENDATION_WEIGHT_ENGAGEMENT
        self.weight_category = RECOMMENDATION_WEIGHT_CATEGORY
        self.weight_recency = RECOMMENDATION_WEIGHT_RECENCY

        self.similarity_category_match = RECOMMENDATION_SIMILARITY_CATEGORY_MATCH
        self.similarity_channel_match = RECOMMENDATION_SIMILARITY_CHANNEL_MATCH
        self.similarity_cold_start = RECOMMENDATION_SIMILARITY_COLD_START
        self.similarity_default = RECOMMENDATION_SIMILARITY_DEFAULT

        self.engagement_benchmark = max(RECOMMENDATION_ENGAGEMENT_BENCHMARK, 0.001)
        self.recency_days_horizon = max(RECOMMENDATION_RECENCY_DAYS_HORIZON, 1)
        self.recency_min_floor = RECOMMENDATION_RECENCY_MIN_FLOOR

    def get_cached(
        self,
        user_id: int,
        limit: int,
        db: Session,
    ) -> List[Recommendation]:
        """
        Retorna as recomendações ativas (não expiradas) salvas em banco para o utilizador.
        """
        now = datetime.utcnow()
        recommendations = (
            db.query(Recommendation)
            .options(joinedload(Recommendation.video))
            .filter(
                Recommendation.user_id == user_id,
                Recommendation.expires_at > now,
            )
            .order_by(Recommendation.score.desc())
            .limit(limit)
            .all()
        )
        return recommendations

    def upsert_video(self, db: Session, video_data: VideoCreate) -> Video:
        """
        Garante que o vídeo está persistido na tabela `videos` sem duplicidade.
        """
        existing = (
            db.query(Video)
            .filter(Video.youtube_id == video_data.youtube_id)
            .first()
        )
        if existing:
            # Atualiza métricas recentes
            existing.view_count = video_data.view_count
            existing.like_count = video_data.like_count
            existing.comment_count = video_data.comment_count
            if video_data.title:
                existing.title = video_data.title
            if video_data.thumbnail_url:
                existing.thumbnail_url = video_data.thumbnail_url
            db.flush()
            return existing

        new_video = Video(
            id=uuid.uuid4(),
            youtube_id=video_data.youtube_id,
            title=video_data.title,
            description=video_data.description,
            thumbnail_url=video_data.thumbnail_url,
            channel_title=video_data.channel_title,
            category_id=video_data.category_id,
            published_at=video_data.published_at,
            view_count=video_data.view_count,
            like_count=video_data.like_count,
            comment_count=video_data.comment_count,
        )
        db.add(new_video)
        db.flush()
        return new_video

    def calculate_score(
        self,
        candidate: Video,
        top_category: Optional[str],
        favorite_channels: Set[str],
    ) -> float:
        """
        Score = 0.5 * (like/view normalizado) + 0.3 * similaridade_categoria + 0.2 * recência
        Intervalo final: [0.0, 1.0]
        """
        # 1. Like/View normalizado
        views = max(candidate.view_count or 0, 1)
        likes = candidate.like_count or 0
        like_ratio = likes / views
        normalized_engagement = min(like_ratio / self.engagement_benchmark, 1.0)

        # 2. Similaridade de categoria / canal
        if top_category and candidate.category_id == top_category:
            similarity = self.similarity_category_match
        elif candidate.channel_title and candidate.channel_title in favorite_channels:
            similarity = self.similarity_channel_match
        elif not top_category:
            similarity = self.similarity_cold_start  # cold start razoável
        else:
            similarity = self.similarity_default

        # 3. Recência
        if candidate.published_at:
            delta_days = max((datetime.utcnow() - candidate.published_at).days, 0)
            recency = max(self.recency_min_floor, 1.0 - (delta_days / float(self.recency_days_horizon)))
        else:
            recency = 0.5

        final_score = (
            (self.weight_engagement * normalized_engagement)
            + (self.weight_category * similarity)
            + (self.weight_recency * recency)
        )
        return round(max(0.0, min(1.0, final_score)), 4)

    def refresh_for_user(
        self,
        user_id: int,
        limit: int = 20,
        category_id: Optional[str] = None,
        db: Session = None,
    ) -> List[Recommendation]:
        """
        Recalcula recomendações personalizadas com base no histórico recente e YouTube Data API.
        """
        if db is None:
            raise ValueError("Sessão do banco de dados é obrigatória")

        # 1. Buscar últimas entradas do watch_history do utilizador (definido no .env)
        history_entries = (
            db.query(WatchHistory)
            .options(joinedload(WatchHistory.video))
            .filter(WatchHistory.user_id == user_id)
            .order_by(WatchHistory.watched_at.desc())
            .limit(self.history_limit)
            .all()
        )

        watched_video_ids: Set[uuid.UUID] = {entry.video_id for entry in history_entries}
        watched_youtube_ids: Set[str] = {
            entry.video.youtube_id for entry in history_entries if entry.video
        }

        # 2. Extrair category_id e channel_title mais frequentes
        categories: List[str] = []
        channels: List[str] = []

        for entry in history_entries:
            if entry.video:
                if entry.video.category_id:
                    categories.append(entry.video.category_id)
                if entry.video.channel_title:
                    channels.append(entry.video.channel_title)

        top_category = category_id
        if not top_category and categories:
            top_category = Counter(categories).most_common(1)[0][0]

        favorite_channels: Set[str] = {
            c for c, _ in Counter(channels).most_common(self.favorite_channels_limit)
        }

        # 3. Chamar youtube_service para buscar candidatos externos
        search_terms = []
        if top_category:
            search_terms.append(f"categoria {top_category}")
        if favorite_channels:
            search_terms.extend(list(favorite_channels)[:self.query_channel_terms_limit])
        if not search_terms:
            search_terms = self.fallback_queries

        candidate_creates: List[VideoCreate] = []
        for term in search_terms:
            try:
                results = youtube_service.search_videos(
                    query=term,
                    max_results=self.candidate_limit,
                    category_id=top_category if term.startswith("categoria") else None,
                )
                candidate_creates.extend(results)
            except Exception as e:
                log_message(f"Aviso ao buscar candidatos para '{term}': {e}", "warning")

        # 4. Incluir também vídeos pré-existentes no DB que o usuário ainda não assistiu
        existing_unwatched = (
            db.query(Video)
            .filter(~Video.id.in_(watched_video_ids) if watched_video_ids else True)
            .order_by(Video.view_count.desc())
            .limit(self.candidate_limit)
            .all()
        )

        # 5. Persistir todos os candidatos na tabela de vídeos e obter objetos Video ORM
        candidate_orm_videos: List[Video] = list(existing_unwatched)
        seen_yt_ids = {v.youtube_id for v in candidate_orm_videos}

        for vc in candidate_creates:
            if vc.youtube_id in watched_youtube_ids or vc.youtube_id in seen_yt_ids:
                continue
            seen_yt_ids.add(vc.youtube_id)
            try:
                v_orm = self.upsert_video(db, vc)
                candidate_orm_videos.append(v_orm)
            except Exception as ex:
                log_message(f"Erro ao salvar vídeo candidato {vc.youtube_id}: {ex}", "warning")

        # 6. Filtrar vídeos já assistidos e pontuar candidatos
        scored_candidates = []
        for video in candidate_orm_videos:
            if video.id in watched_video_ids or video.youtube_id in watched_youtube_ids:
                continue

            score = self.calculate_score(video, top_category, favorite_channels)

            # Motivo explicativo amigável
            if top_category and video.category_id == top_category:
                reason = "Baseado nas suas categorias mais assistidas"
            elif video.channel_title and video.channel_title in favorite_channels:
                reason = f"Similar ao canal {video.channel_title}"
            elif score >= self.high_score_threshold:
                reason = "Alta taxa de engajamento e aprovação"
            else:
                reason = "Em alta na comunidade de tecnologia"

            scored_candidates.append({
                "video": video,
                "score": score,
                "reason": reason,
            })

        # Ordenar pelos maiores scores
        scored_candidates.sort(key=lambda x: x["score"], reverse=True)
        top_candidates = scored_candidates[:limit]

        # 7. Limpar recomendações antigas do utilizador
        db.query(Recommendation).filter(Recommendation.user_id == user_id).delete()

        # 8. Persistir top recomendações com expiração configurada no .env
        now = datetime.utcnow()
        expires_at = now + timedelta(hours=self.ttl_hours)

        new_recs: List[Recommendation] = []
        for item in top_candidates:
            rec = Recommendation(
                id=uuid.uuid4(),
                user_id=user_id,
                video_id=item["video"].id,
                score=item["score"],
                reason=item["reason"],
                generated_at=now,
                expires_at=expires_at,
            )
            db.add(rec)
            new_recs.append(rec)

        db.commit()

        # Recarregar com o relacionamento video carregado
        return self.get_cached(user_id, limit, db)

    def get_recommendations(
        self,
        user_id: int,
        limit: int = 20,
        refresh: bool = False,
        category_id: Optional[str] = None,
        db: Session = None,
    ) -> List[Recommendation]:
        """
        Retorna as recomendações do usuário:
        Se existir em cache/DB válido e refresh=False, devolve.
        Caso contrário, recalcula e persiste.
        """
        if not refresh:
            cached = self.get_cached(user_id, limit, db)
            if cached and len(cached) >= min(limit, self.min_cached_count):
                return cached

        return self.refresh_for_user(
            user_id=user_id,
            limit=limit,
            category_id=category_id,
            db=db,
        )


recommendation_service = RecommendationService()
