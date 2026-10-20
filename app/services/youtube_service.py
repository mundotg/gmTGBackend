"""
Serviço de integração com a API do YouTube Data v3.
Encapsula chamadas ao YouTube, com cache Redis de 1 hora e tratamento rigoroso de cotas.
"""
import re
from datetime import datetime
from typing import List, Optional
from fastapi import HTTPException, status
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.config.dotenv import get_env
from app.config.redis import read_cache, write_cache
from app.core.config import (
    YOUTUBE_API_KEY,
    YOUTUBE_CACHE_TTL,
    YOUTUBE_DEFAULT_SEARCH_QUERY,
    YOUTUBE_FALLBACK_CATALOG_ENABLED,
    YOUTUBE_MAX_SEARCH_RESULTS,
)
from app.schemas.video import VideoCreate
from app.ultils.logger import log_message


# Coleção de fallback para quando a chave não for configurada ou cota esgotada em dev
FALLBACK_TECH_VIDEOS = [
    {
        "youtube_id": "dQw4w9WgXcQ",
        "title": "Rick Astley - Never Gonna Give You Up (Official Music Video)",
        "description": "The official video for Never Gonna Give You Up by Rick Astley.",
        "thumbnail_url": "https://img.youtube.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
        "channel_title": "Rick Astley",
        "category_id": "10",
        "published_at": "2009-10-25T06:57:33Z",
        "view_count": 1600000000,
        "like_count": 17000000,
        "comment_count": 2500000,
    },
    {
        "youtube_id": "7eh4d6sabA0",
        "title": "FastAPI in 100 Seconds",
        "description": "Learn the basics of FastAPI, a modern, fast Python web framework for building APIs.",
        "thumbnail_url": "https://img.youtube.com/vi/7eh4d6sabA0/hqdefault.jpg",
        "channel_title": "Fireship",
        "category_id": "28",
        "published_at": "2021-08-11T18:00:00Z",
        "view_count": 890000,
        "like_count": 45000,
        "comment_count": 1200,
    },
    {
        "youtube_id": "SqcY0GlETPk",
        "title": "Next.js 14 Full Course 2024 | Build and Deploy a Full Stack App",
        "description": "Master Next.js 14 App Router, Server Actions, Tailwind CSS and PostgreSQL.",
        "thumbnail_url": "https://img.youtube.com/vi/SqcY0GlETPk/hqdefault.jpg",
        "channel_title": "JavaScript Mastery",
        "category_id": "28",
        "published_at": "2023-12-15T15:00:00Z",
        "view_count": 1400000,
        "like_count": 68000,
        "comment_count": 3100,
    },
    {
        "youtube_id": "aircAruvnKk",
        "title": "Neural Networks from Scratch - P.1 Intro and Neuron Code",
        "description": "Introduction to neural networks from scratch in Python.",
        "thumbnail_url": "https://img.youtube.com/vi/aircAruvnKk/hqdefault.jpg",
        "channel_title": "sentdex",
        "category_id": "27",
        "published_at": "2020-04-09T14:00:00Z",
        "view_count": 2300000,
        "like_count": 89000,
        "comment_count": 4200,
    },
    {
        "youtube_id": "rfscVS0vtbw",
        "title": "Learn Python - Full Course for Beginners [Tutorial]",
        "description": "This course will give you a full introduction into all of the core concepts in python.",
        "thumbnail_url": "https://img.youtube.com/vi/rfscVS0vtbw/hqdefault.jpg",
        "channel_title": "freeCodeCamp.org",
        "category_id": "27",
        "published_at": "2018-07-11T17:00:00Z",
        "view_count": 48000000,
        "like_count": 1250000,
        "comment_count": 56000,
    },
    {
        "youtube_id": "Oe421EPjeBE",
        "title": "Node.js and Express.js - Full Course",
        "description": "Learn how to use Node and Express in this comprehensive course.",
        "thumbnail_url": "https://img.youtube.com/vi/Oe421EPjeBE/hqdefault.jpg",
        "channel_title": "freeCodeCamp.org",
        "category_id": "28",
        "published_at": "2021-04-20T13:30:00Z",
        "view_count": 3200000,
        "like_count": 92000,
        "comment_count": 3800,
    },
    {
        "youtube_id": "zZ6vybT1HQs",
        "title": "Docker in 100 Seconds",
        "description": "Learn Docker basics and how containerization revolutionized software engineering.",
        "thumbnail_url": "https://img.youtube.com/vi/zZ6vybT1HQs/hqdefault.jpg",
        "channel_title": "Fireship",
        "category_id": "28",
        "published_at": "2020-09-22T19:00:00Z",
        "view_count": 2100000,
        "like_count": 110000,
        "comment_count": 2900,
    },
    {
        "youtube_id": "Ke90Tje7VS0",
        "title": "React JS - Full Course for Beginners",
        "description": "Dive deep into modern React with hooks, components, state management and Vite.",
        "thumbnail_url": "https://img.youtube.com/vi/Ke90Tje7VS0/hqdefault.jpg",
        "channel_title": "freeCodeCamp.org",
        "category_id": "28",
        "published_at": "2022-01-10T16:00:00Z",
        "view_count": 4500000,
        "like_count": 140000,
        "comment_count": 4500,
    },
]


class YouTubeService:
    """Cliente para a API do YouTube Data v3."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or YOUTUBE_API_KEY or get_env("YOUTUBE_API_KEY", "")
        self.cache_ttl = YOUTUBE_CACHE_TTL
        self.default_query = YOUTUBE_DEFAULT_SEARCH_QUERY
        self.max_results_limit = YOUTUBE_MAX_SEARCH_RESULTS
        self.fallback_catalog_enabled = YOUTUBE_FALLBACK_CATALOG_ENABLED

    def _get_client(self):
        if not self.api_key:
            return None
        return build("youtube", "v3", developerKey=self.api_key, cache_discovery=False)

    def _parse_published_at(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            clean_str = re.sub(r"Z$", "+00:00", date_str)
            dt = datetime.fromisoformat(clean_str)
            return dt.replace(tzinfo=None)
        except Exception:
            return None

    def search_videos(
        self,
        query: str,
        max_results: int = 50,
        category_id: Optional[str] = None,
    ) -> List[VideoCreate]:
        """
        Busca vídeos no YouTube com consulta, categoria opcional e popula estatísticas.
        Aplica cache Redis de 1 hora por chave de busca.
        """
        clean_query = (query or "").strip() or self.default_query
        effective_max = min(max(max_results, 1), self.max_results_limit)
        cache_key = f"yt:search:{clean_query}:{category_id or 'all'}:{effective_max}"

        # 1. Verificar cache Redis
        cached_data = read_cache(cache_key)
        if cached_data and isinstance(cached_data, list):
            try:
                return [VideoCreate(**item) for item in cached_data]
            except Exception as e:
                log_message(f"Falha ao deserializar cache de vídeos: {e}", "warning")

        # 2. Executar chamada à API oficial do YouTube se houver API key
        client = self._get_client()
        if client:
            try:
                search_params = {
                    "q": clean_query,
                    "part": "snippet",
                    "type": "video",
                    "maxResults": effective_max,
                }
                if category_id:
                    search_params["videoCategoryId"] = str(category_id)

                search_response = client.search().list(**search_params).execute()
                items = search_response.get("items", [])
                video_ids = [
                    item["id"]["videoId"]
                    for item in items
                    if item.get("id", {}).get("videoId")
                ]

                if not video_ids:
                    return []

                # Buscar estatísticas detalhadas (views, likes, comments)
                video_details_response = (
                    client.videos()
                    .list(part="snippet,statistics", id=",".join(video_ids))
                    .execute()
                )

                results: List[VideoCreate] = []
                for v in video_details_response.get("items", []):
                    snippet = v.get("snippet", {})
                    stats = v.get("statistics", {})
                    thumbnails = snippet.get("thumbnails", {})
                    thumb_url = (
                        thumbnails.get("high", {}).get("url")
                        or thumbnails.get("medium", {}).get("url")
                        or thumbnails.get("default", {}).get("url")
                        or f"https://img.youtube.com/vi/{v.get('id')}/hqdefault.jpg"
                    )

                    video_obj = VideoCreate(
                        youtube_id=v.get("id"),
                        title=snippet.get("title", "Sem título"),
                        description=snippet.get("description", ""),
                        thumbnail_url=thumb_url,
                        channel_title=snippet.get("channelTitle", "Canal desconhecido"),
                        category_id=snippet.get("categoryId", category_id),
                        published_at=self._parse_published_at(snippet.get("publishedAt")),
                        view_count=int(stats.get("viewCount", 0)),
                        like_count=int(stats.get("likeCount", 0)),
                        comment_count=int(stats.get("commentCount", 0)),
                    )
                    results.append(video_obj)

                # Persistir no cache Redis
                try:
                    write_cache(
                        cache_key,
                        [item.model_dump(mode="json") for item in results],
                        ttl=self.cache_ttl,
                    )
                except Exception as ex:
                    log_message(f"Erro ao salvar cache Redis: {ex}", "warning")

                return results

            except HttpError as err:
                status_code = getattr(err.resp, "status", None) or 500
                error_content = str(err)
                log_message(
                    f"Erro HTTP na API do YouTube ({status_code}): {error_content}",
                    "error",
                )

                # 403 quota exceeded -> HTTP 429
                if status_code == 403 and any(
                    x in error_content for x in ["quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"]
                ):
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Cota diária da API do YouTube excedida (10k unidades/dia). Tente novamente mais tarde.",
                    )
                if status_code >= 500:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="Serviço da API do YouTube temporariamente indisponível.",
                    )
                raise HTTPException(
                    status_code=status_code,
                    detail=f"Erro da API do YouTube: {error_content}",
                )

            except Exception as e:
                log_message(f"Falha inesperada ao comunicar com o YouTube: {e}", "error")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Falha ao contactar a API externa do YouTube.",
                )

        # 3. Fallback inteligente quando a API key não foi configurada no ambiente
        if not self.fallback_catalog_enabled:
            log_message("YOUTUBE_API_KEY não configurada e fallback desativado via .env.", "warning")
            return []

        log_message(
            "YOUTUBE_API_KEY não configurada. Usando catálogo integrado para demonstração.",
            "info",
        )
        filtered = []
        for item in FALLBACK_TECH_VIDEOS:
            q_lower = clean_query.lower()
            if not q_lower or (
                q_lower in item["title"].lower()
                or q_lower in item["description"].lower()
                or q_lower in item["channel_title"].lower()
            ):
                filtered.append(
                    VideoCreate(
                        youtube_id=item["youtube_id"],
                        title=item["title"],
                        description=item["description"],
                        thumbnail_url=item["thumbnail_url"],
                        channel_title=item["channel_title"],
                        category_id=item["category_id"],
                        published_at=self._parse_published_at(item["published_at"]),
                        view_count=item["view_count"],
                        like_count=item["like_count"],
                        comment_count=item["comment_count"],
                    )
                )

        if not filtered and FALLBACK_TECH_VIDEOS:
            # Retorna amostra padrão
            filtered = [
                VideoCreate(
                    youtube_id=item["youtube_id"],
                    title=item["title"],
                    description=item["description"],
                    thumbnail_url=item["thumbnail_url"],
                    channel_title=item["channel_title"],
                    category_id=item["category_id"],
                    published_at=self._parse_published_at(item["published_at"]),
                    view_count=item["view_count"],
                    like_count=item["like_count"],
                    comment_count=item["comment_count"],
                )
                for item in FALLBACK_TECH_VIDEOS[:max_results]
            ]

        # Salva em cache
        write_cache(
            cache_key,
            [v.model_dump(mode="json") for v in filtered],
            ttl=self.cache_ttl,
        )
        return filtered

    def get_video_details(self, youtube_id: str) -> Optional[VideoCreate]:
        """Recupera detalhes e métricas de um vídeo específico pelo seu youtube_id."""
        cache_key = f"yt:video:{youtube_id}"
        cached = read_cache(cache_key)
        if cached:
            try:
                return VideoCreate(**cached)
            except Exception:
                pass

        client = self._get_client()
        if client:
            try:
                response = client.videos().list(part="snippet,statistics", id=youtube_id).execute()
                items = response.get("items", [])
                if not items:
                    return None
                item = items[0]
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                thumbnails = snippet.get("thumbnails", {})
                thumb_url = (
                    thumbnails.get("high", {}).get("url")
                    or thumbnails.get("medium", {}).get("url")
                    or f"https://img.youtube.com/vi/{youtube_id}/hqdefault.jpg"
                )

                video_data = VideoCreate(
                    youtube_id=youtube_id,
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    thumbnail_url=thumb_url,
                    channel_title=snippet.get("channelTitle", ""),
                    category_id=snippet.get("categoryId"),
                    published_at=self._parse_published_at(snippet.get("publishedAt")),
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats.get("likeCount", 0)),
                    comment_count=int(stats.get("commentCount", 0)),
                )
                write_cache(cache_key, video_data.model_dump(mode="json"), ttl=self.cache_ttl)
                return video_data
            except HttpError as err:
                status_code = getattr(err.resp, "status", None) or 500
                if status_code == 403:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Cota diária da API do YouTube excedida.",
                    )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Erro ao consultar detalhes no YouTube.",
                )

        # Fallback local
        if not self.fallback_catalog_enabled:
            return None

        for item in FALLBACK_TECH_VIDEOS:
            if item["youtube_id"] == youtube_id:
                return VideoCreate(
                    youtube_id=item["youtube_id"],
                    title=item["title"],
                    description=item["description"],
                    thumbnail_url=item["thumbnail_url"],
                    channel_title=item["channel_title"],
                    category_id=item["category_id"],
                    published_at=self._parse_published_at(item["published_at"]),
                    view_count=item["view_count"],
                    like_count=item["like_count"],
                    comment_count=item["comment_count"],
                )

        return None


youtube_service = YouTubeService()
