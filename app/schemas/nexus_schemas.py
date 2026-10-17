# app/schemas/nexus_schemas.py
from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, ConfigDict, Field


# ============================================================
# 👤 Informações do Autor
# ============================================================
class AuthorInfo(BaseModel):
    id: int
    nome: str
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    cargo: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 📚 Fontes e Menções
# ============================================================
class SourceReferenceSchema(BaseModel):
    title: str = Field(..., min_length=2, max_length=500)
    authors: str = Field(..., min_length=2, max_length=500)
    year: int = Field(..., ge=1800, le=2100)
    doi: Optional[str] = Field(default=None, max_length=255)
    url: Optional[str] = Field(default=None, max_length=1000)
    verified: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True)


class MentionSchema(BaseModel):
    name: str = Field(..., min_length=2, max_length=200)
    role: Optional[str] = Field(default=None, max_length=200)

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 📄 Publicações (NexusPost)
# ============================================================
class NexusPostCreateSchema(BaseModel):
    title: str = Field(..., min_length=3, max_length=255)
    summary: Optional[str] = Field(default=None, max_length=1000)
    body_markdown: str = Field(..., min_length=10)
    category: str = Field(..., min_length=2, max_length=100)
    subcategory: Optional[str] = Field(default=None, max_length=100)
    content_type: str = Field(default="cientifico", max_length=50)
    min_age: str = Field(default="all", max_length=20)
    cover_image: Optional[str] = Field(default=None, max_length=1000)
    video_url: Optional[str] = Field(default=None, max_length=1000)
    external_url: Optional[str] = Field(default=None, max_length=1000)
    read_time_minutes: Optional[int] = Field(default=None, ge=1, le=180)
    tags: Optional[List[str]] = Field(default_factory=list)
    sources: Optional[List[SourceReferenceSchema]] = Field(default_factory=list)
    mentions: Optional[List[MentionSchema]] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class NexusPostUpdateSchema(BaseModel):
    title: Optional[str] = Field(default=None, min_length=3, max_length=255)
    summary: Optional[str] = Field(default=None, max_length=1000)
    body_markdown: Optional[str] = Field(default=None, min_length=10)
    category: Optional[str] = Field(default=None, max_length=100)
    subcategory: Optional[str] = Field(default=None, max_length=100)
    content_type: Optional[str] = Field(default=None, max_length=50)
    min_age: Optional[str] = Field(default=None, max_length=20)
    status: Optional[str] = Field(default=None, max_length=50)
    cover_image: Optional[str] = Field(default=None, max_length=1000)
    video_url: Optional[str] = Field(default=None, max_length=1000)
    external_url: Optional[str] = Field(default=None, max_length=1000)
    read_time_minutes: Optional[int] = Field(default=None, ge=1, le=180)
    tags: Optional[List[str]] = None
    sources: Optional[List[SourceReferenceSchema]] = None
    mentions: Optional[List[MentionSchema]] = None

    model_config = ConfigDict(from_attributes=True)


class NexusPostResponseSchema(BaseModel):
    id: int
    author_id: int
    author: Optional[AuthorInfo] = None
    title: str
    summary: Optional[str] = None
    body_markdown: str
    category: str
    subcategory: Optional[str] = None
    content_type: str
    min_age: str
    status: str
    read_time_minutes: int
    cover_image: Optional[str] = None
    video_url: Optional[str] = None
    external_url: Optional[str] = None
    tags: Optional[List[str]] = None
    sources: Optional[List[Dict[str, Any]]] = None
    mentions: Optional[List[Dict[str, Any]]] = None
    is_verified: bool
    views_count: int
    likes_count: int
    shares_count: int
    bookmarks_count: int
    comments_count: int
    published_at: datetime
    created_at: datetime
    updated_at: datetime
    is_liked: Optional[bool] = False
    is_saved: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True)


class NexusPostListResponseSchema(BaseModel):
    items: List[NexusPostResponseSchema]
    total: int
    page: int
    size: int
    pages: int

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 💬 Comentários (NexusComment)
# ============================================================
class NexusCommentCreateSchema(BaseModel):
    content: str = Field(..., min_length=1, max_length=5000)
    parent_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class NexusCommentUpdateSchema(BaseModel):
    content: str = Field(..., min_length=1, max_length=5000)

    model_config = ConfigDict(from_attributes=True)


class NexusCommentResponseSchema(BaseModel):
    id: int
    post_id: int
    user_id: int
    parent_id: Optional[int] = None
    content: str
    likes_count: int
    is_edited: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime
    author: Optional[AuthorInfo] = None
    replies_count: Optional[int] = 0
    is_liked: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 🎭 Reações e Gostos (NexusReaction & NexusLike)
# ============================================================
class NexusReactionCreateSchema(BaseModel):
    reaction_type: str = Field(default="like", max_length=50)
    comment_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class NexusReactionResponseSchema(BaseModel):
    id: int
    reaction_type: str
    post_id: Optional[int] = None
    comment_id: Optional[int] = None
    user_id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class NexusLikeToggleResponse(BaseModel):
    liked: bool
    likes_count: int

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 🔖 Artigos Guardados / Saves (NexusSave)
# ============================================================
class NexusSaveToggleResponse(BaseModel):
    saved: bool
    bookmarks_count: int
    collection_name: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 🔄 Partilhas (NexusShare)
# ============================================================
class NexusShareCreateSchema(BaseModel):
    platform: str = Field(default="copy_link", max_length=50)

    model_config = ConfigDict(from_attributes=True)


class NexusShareResponseSchema(BaseModel):
    id: int
    post_id: int
    platform: str
    shares_count: int

    model_config = ConfigDict(from_attributes=True)


# ============================================================
# 👁️ Visualizações (NexusView)
# ============================================================
class NexusViewCreateSchema(BaseModel):
    read_time_seconds: int = Field(default=0, ge=0)

    model_config = ConfigDict(from_attributes=True)


class NexusViewResponseSchema(BaseModel):
    id: int
    post_id: int
    views_count: int

    model_config = ConfigDict(from_attributes=True)
