"""
Configurações do módulo de recomendações e integrações externas da aplicação.
Todos os valores estáticos e parâmetros de pesos/limites são lidos do .env para
permitir ajuste e calibragem rápidos em ambiente de produção ou desenvolvimento.
"""
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from app.config.dotenv import get_env, get_env_bool, get_env_float, get_env_int


class CoreSettings(BaseSettings):
    """Configurações centrais do sistema de recomendações de vídeo e YouTube."""

    # -------------------------------------------------------------
    # 📺 YouTube Data API v3
    # -------------------------------------------------------------
    YOUTUBE_API_KEY: str = Field(
        default_factory=lambda: get_env("YOUTUBE_API_KEY", "")
    )
    YOUTUBE_CACHE_TTL: int = Field(
        default_factory=lambda: get_env_int("YOUTUBE_CACHE_TTL", 3600)
    )
    YOUTUBE_DEFAULT_SEARCH_QUERY: str = Field(
        default_factory=lambda: get_env(
            "YOUTUBE_DEFAULT_SEARCH_QUERY", "technology programming"
        )
    )
    YOUTUBE_MAX_SEARCH_RESULTS: int = Field(
        default_factory=lambda: get_env_int("YOUTUBE_MAX_SEARCH_RESULTS", 50)
    )
    YOUTUBE_FALLBACK_CATALOG_ENABLED: bool = Field(
        default_factory=lambda: get_env_bool("YOUTUBE_FALLBACK_CATALOG_ENABLED", True)
    )

    # -------------------------------------------------------------
    # 🎯 Recomendações: Limites e Expiração
    # -------------------------------------------------------------
    RECOMMENDATION_TTL_HOURS: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_TTL_HOURS", 24)
    )
    RECOMMENDATION_DEFAULT_LIMIT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_DEFAULT_LIMIT", 20)
    )
    RECOMMENDATION_HISTORY_LIMIT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_HISTORY_LIMIT", 20)
    )
    RECOMMENDATION_CANDIDATE_LIMIT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_CANDIDATE_LIMIT", 25)
    )
    RECOMMENDATION_FALLBACK_QUERIES: str = Field(
        default_factory=lambda: get_env(
            "RECOMMENDATION_FALLBACK_QUERIES",
            "tecnologia programação,desenvolvimento web,computação em nuvem",
        )
    )
    RECOMMENDATION_FAVORITE_CHANNELS_LIMIT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_FAVORITE_CHANNELS_LIMIT", 5)
    )
    RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT", 2)
    )
    RECOMMENDATION_MIN_CACHED_COUNT: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_MIN_CACHED_COUNT", 5)
    )
    RECOMMENDATION_HIGH_SCORE_THRESHOLD: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_HIGH_SCORE_THRESHOLD", 0.7)
    )

    # -------------------------------------------------------------
    # ⚖️ Pesos do Algoritmo de Score
    # Score = (weight_eng * norm_engagement) + (weight_cat * similarity) + (weight_rec * recency)
    # -------------------------------------------------------------
    RECOMMENDATION_WEIGHT_ENGAGEMENT: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_WEIGHT_ENGAGEMENT", 0.5)
    )
    RECOMMENDATION_WEIGHT_CATEGORY: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_WEIGHT_CATEGORY", 0.3)
    )
    RECOMMENDATION_WEIGHT_RECENCY: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_WEIGHT_RECENCY", 0.2)
    )

    # -------------------------------------------------------------
    # 📐 Parâmetros de Similaridade e Recência
    # -------------------------------------------------------------
    RECOMMENDATION_SIMILARITY_CATEGORY_MATCH: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_SIMILARITY_CATEGORY_MATCH", 1.0)
    )
    RECOMMENDATION_SIMILARITY_CHANNEL_MATCH: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_SIMILARITY_CHANNEL_MATCH", 0.9)
    )
    RECOMMENDATION_SIMILARITY_COLD_START: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_SIMILARITY_COLD_START", 0.6)
    )
    RECOMMENDATION_SIMILARITY_DEFAULT: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_SIMILARITY_DEFAULT", 0.3)
    )

    # -------------------------------------------------------------
    # 📐 Parâmetros de Calibragem e Normalização
    # -------------------------------------------------------------
    RECOMMENDATION_ENGAGEMENT_BENCHMARK: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_ENGAGEMENT_BENCHMARK", 0.08)
    )
    RECOMMENDATION_RECENCY_DAYS_HORIZON: int = Field(
        default_factory=lambda: get_env_int("RECOMMENDATION_RECENCY_DAYS_HORIZON", 365)
    )
    RECOMMENDATION_RECENCY_MIN_FLOOR: float = Field(
        default_factory=lambda: get_env_float("RECOMMENDATION_RECENCY_MIN_FLOOR", 0.1)
    )

    model_config = {
        "env_file": ".env",
        "extra": "ignore",
    }


    # -------------------------------------------------------------
    # 🎮 Tapatan 3x3 - Configurações do Jogo & WebSocket
    # -------------------------------------------------------------
    GAME_TURN_TIMEOUT_SECONDS: int = Field(
        default_factory=lambda: get_env_int("GAME_TURN_TIMEOUT_SECONDS", 30)
    )
    GAME_AI_DEFAULT_DIFFICULTY: str = Field(
        default_factory=lambda: get_env("GAME_AI_DEFAULT_DIFFICULTY", "medium")
    )
    GAME_MAX_CHAT_HISTORY: int = Field(
        default_factory=lambda: get_env_int("GAME_MAX_CHAT_HISTORY", 100)
    )
    GAME_WS_PING_INTERVAL: int = Field(
        default_factory=lambda: get_env_int("GAME_WS_PING_INTERVAL", 15)
    )
    GAME_KNOWLEDGE_BASE_PATH: str = Field(
        default_factory=lambda: get_env(
            "GAME_KNOWLEDGE_BASE_PATH", "app/services/game_knowledge_base.json"
        )
    )
    GAME_RECONNECT_TIMEOUT_SECONDS: int = Field(
        default_factory=lambda: get_env_int("GAME_RECONNECT_TIMEOUT_SECONDS", 45)
    )
    GAME_STATS_CACHE_TTL_SECONDS: int = Field(
        default_factory=lambda: get_env_int("GAME_STATS_CACHE_TTL_SECONDS", 60)
    )


settings = CoreSettings()


# Exportações diretas no módulo para consumo rápido
YOUTUBE_API_KEY: str = settings.YOUTUBE_API_KEY
YOUTUBE_CACHE_TTL: int = settings.YOUTUBE_CACHE_TTL
YOUTUBE_DEFAULT_SEARCH_QUERY: str = settings.YOUTUBE_DEFAULT_SEARCH_QUERY
YOUTUBE_MAX_SEARCH_RESULTS: int = settings.YOUTUBE_MAX_SEARCH_RESULTS
YOUTUBE_FALLBACK_CATALOG_ENABLED: bool = settings.YOUTUBE_FALLBACK_CATALOG_ENABLED

RECOMMENDATION_TTL_HOURS: int = settings.RECOMMENDATION_TTL_HOURS
RECOMMENDATION_DEFAULT_LIMIT: int = settings.RECOMMENDATION_DEFAULT_LIMIT
RECOMMENDATION_HISTORY_LIMIT: int = settings.RECOMMENDATION_HISTORY_LIMIT
RECOMMENDATION_CANDIDATE_LIMIT: int = settings.RECOMMENDATION_CANDIDATE_LIMIT
RECOMMENDATION_FALLBACK_QUERIES: str = settings.RECOMMENDATION_FALLBACK_QUERIES
RECOMMENDATION_FAVORITE_CHANNELS_LIMIT: int = settings.RECOMMENDATION_FAVORITE_CHANNELS_LIMIT
RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT: int = settings.RECOMMENDATION_QUERY_CHANNEL_TERMS_LIMIT
RECOMMENDATION_MIN_CACHED_COUNT: int = settings.RECOMMENDATION_MIN_CACHED_COUNT
RECOMMENDATION_HIGH_SCORE_THRESHOLD: float = settings.RECOMMENDATION_HIGH_SCORE_THRESHOLD

RECOMMENDATION_WEIGHT_ENGAGEMENT: float = settings.RECOMMENDATION_WEIGHT_ENGAGEMENT
RECOMMENDATION_WEIGHT_CATEGORY: float = settings.RECOMMENDATION_WEIGHT_CATEGORY
RECOMMENDATION_WEIGHT_RECENCY: float = settings.RECOMMENDATION_WEIGHT_RECENCY

RECOMMENDATION_SIMILARITY_CATEGORY_MATCH: float = settings.RECOMMENDATION_SIMILARITY_CATEGORY_MATCH
RECOMMENDATION_SIMILARITY_CHANNEL_MATCH: float = settings.RECOMMENDATION_SIMILARITY_CHANNEL_MATCH
RECOMMENDATION_SIMILARITY_COLD_START: float = settings.RECOMMENDATION_SIMILARITY_COLD_START
RECOMMENDATION_SIMILARITY_DEFAULT: float = settings.RECOMMENDATION_SIMILARITY_DEFAULT

RECOMMENDATION_ENGAGEMENT_BENCHMARK: float = settings.RECOMMENDATION_ENGAGEMENT_BENCHMARK
RECOMMENDATION_RECENCY_DAYS_HORIZON: int = settings.RECOMMENDATION_RECENCY_DAYS_HORIZON
RECOMMENDATION_RECENCY_MIN_FLOOR: float = settings.RECOMMENDATION_RECENCY_MIN_FLOOR

GAME_TURN_TIMEOUT_SECONDS: int = settings.GAME_TURN_TIMEOUT_SECONDS
GAME_AI_DEFAULT_DIFFICULTY: str = settings.GAME_AI_DEFAULT_DIFFICULTY
GAME_MAX_CHAT_HISTORY: int = settings.GAME_MAX_CHAT_HISTORY
GAME_WS_PING_INTERVAL: int = settings.GAME_WS_PING_INTERVAL
GAME_KNOWLEDGE_BASE_PATH: str = settings.GAME_KNOWLEDGE_BASE_PATH
GAME_RECONNECT_TIMEOUT_SECONDS: int = settings.GAME_RECONNECT_TIMEOUT_SECONDS
GAME_STATS_CACHE_TTL_SECONDS: int = settings.GAME_STATS_CACHE_TTL_SECONDS

