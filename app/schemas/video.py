import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class VideoBase(BaseModel):
    youtube_id: str = Field(..., max_length=20, description="Identificador único do vídeo no YouTube")
    title: str = Field(..., max_length=500, description="Título do vídeo")
    description: Optional[str] = Field(default=None, description="Descrição detalhada")
    thumbnail_url: Optional[str] = Field(default=None, max_length=500, description="URL da thumbnail")
    channel_title: Optional[str] = Field(default=None, max_length=255, description="Nome do canal")
    category_id: Optional[str] = Field(default=None, max_length=10, description="ID da categoria no YouTube")
    published_at: Optional[datetime] = Field(default=None, description="Data de publicação no YouTube")
    view_count: int = Field(default=0, ge=0, description="Contagem de visualizações")
    like_count: int = Field(default=0, ge=0, description="Contagem de curtidas")
    comment_count: int = Field(default=0, ge=0, description="Contagem de comentários")


class VideoCreate(VideoBase):
    pass


class VideoRead(VideoBase):
    id: uuid.UUID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class WatchHistoryCreate(BaseModel):
    watch_duration: int = Field(default=0, ge=0, description="Duração assistida em segundos")
    liked: bool = Field(default=False, description="Se o utilizador deu like no vídeo")


class WatchHistoryRead(BaseModel):
    id: uuid.UUID
    user_id: int
    video_id: uuid.UUID
    watched_at: datetime
    watch_duration: int
    liked: bool
    video: Optional[VideoRead] = None

    model_config = ConfigDict(from_attributes=True)
