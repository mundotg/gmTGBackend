# schemas/chat.py

from datetime import datetime
from pydantic import BaseModel, ConfigDict


# ==========================================
# MESSAGES
# ==========================================
class MessageCreate(BaseModel):
    content: str


class MessageResponse(BaseModel):
    id: int
    session_id: int
    role: str
    content: str
    tokens: int | None = None
    model_used: str | None = None
    created_at: datetime

    # 🚀 Pydantic v2: ConfigDict substitui a antiga "class Config"
    model_config = ConfigDict(from_attributes=True)


# ==========================================
# SESSIONS
# ==========================================
class ChatSessionCreate(BaseModel):
    title: str | None = None


class ChatSessionResponse(BaseModel):
    id: int
    title: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    # 🚀 Pydantic v2
    model_config = ConfigDict(from_attributes=True)


# ==========================================
# SESSÃO COM MENSAGENS (Para o Frontend)
# ==========================================
class ChatSessionDetailResponse(ChatSessionResponse):
    """
    Retorna a sessão de chat completa com a lista de mensagens.
    O model_config (from_attributes=True) é herdado automaticamente do ChatSessionResponse.
    """

    # 🚀 Python 3.10+: Usa-se list[] nativo em vez de typing.List
    messages: list[MessageResponse] = []


class FeedbackCreate(BaseModel):
    message_id: int
    rating: int  # 1 a 5
    comment: str | None = None
