import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field
from app.schemas.video import VideoRead


class RecommendationRead(BaseModel):
    id: uuid.UUID
    user_id: int
    video_id: uuid.UUID
    score: float = Field(..., ge=0.0, le=1.0, description="Pontuação da recomendação entre 0.0 e 1.0")
    reason: Optional[str] = Field(default=None, description="Motivo da recomendação")
    generated_at: datetime
    expires_at: datetime
    video: VideoRead

    model_config = ConfigDict(from_attributes=True)


class RecommendationRequest(BaseModel):
    limit: int = Field(default=20, ge=1, le=100, description="Limite máximo de vídeos a retornar")
    category_id: Optional[str] = Field(default=None, description="Filtrar por categoria específica do YouTube")
    refresh: bool = Field(default=False, description="Forçar recálculo e ignorar cache")
