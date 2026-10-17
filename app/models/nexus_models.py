# app/models/nexus_models.py
"""
Modelos do Nexus Cognitiva:
- NexusPost: Publicações / Estudos / Artigos
- NexusComment: Comentários e respostas encadeadas
- NexusReaction: Reações qualitativas ao post ou comentário (like, curious, insight, etc.)
- NexusLike: Registo direto de gostos em posts ou comentários
- NexusShare: Partilhas em redes ou links externos
- NexusView: Visualizações e métricas de leitura
- NexusSave: Artigos guardados / salvos (favoritos)
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    JSON,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.orm import relationship
from app.database import Base


# ============================================================
# 📄 1. PUBLICAÇÃO / POST (NexusPost)
# ============================================================
class NexusPost(Base):
    __tablename__ = "nexus_posts"

    id = Column(Integer, primary_key=True, index=True)
    author_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Identificação e Conteúdo
    title = Column(String(255), nullable=False, index=True)
    summary = Column(Text, nullable=True)
    body_markdown = Column(Text, nullable=False)

    # Taxonomia e Categorização
    category = Column(String(100), nullable=False, index=True)
    subcategory = Column(String(100), nullable=True, index=True)
    content_type = Column(
        String(50),
        nullable=False,
        default="cientifico",
        index=True,
    )  # cientifico | curiosidade | experiencia | opiniao | analise
    min_age = Column(String(20), nullable=False, default="all")  # all | 13+ | 16+ | 18+
    status = Column(
        String(50),
        nullable=False,
        default="published",
        index=True,
    )  # draft | pending_review | published | restricted | archived | removed

    # Multimédia e Links
    cover_image = Column(String(1000), nullable=True)
    video_url = Column(String(1000), nullable=True)
    external_url = Column(String(1000), nullable=True)
    read_time_minutes = Column(Integer, default=5, nullable=False)

    # Metadados estruturados (JSON)
    tags = Column(JSON, nullable=True)  # ex: ["Neurociência", "IA", "Angola"]
    sources = Column(JSON, nullable=True)  # ex: [{"title": "...", "authors": "...", "year": 2024, "doi": "..."}]
    mentions = Column(JSON, nullable=True)  # ex: [{"name": "Dr. Silva", "role": "Investigador"}]
    is_verified = Column(Boolean, default=False, nullable=False)  # Selo de verificação científica

    # Contadores agregados para otimização do feed
    views_count = Column(Integer, default=0, nullable=False)
    likes_count = Column(Integer, default=0, nullable=False)
    shares_count = Column(Integer, default=0, nullable=False)
    bookmarks_count = Column(Integer, default=0, nullable=False)
    comments_count = Column(Integer, default=0, nullable=False)

    # Timestamps
    published_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relações
    author = relationship("User", foreign_keys=[author_id], lazy="selectin")
    comments = relationship(
        "NexusComment",
        back_populates="post",
        cascade="all, delete-orphan",
        order_by="NexusComment.created_at.asc()",
    )
    reactions = relationship(
        "NexusReaction",
        back_populates="post",
        cascade="all, delete-orphan",
    )
    likes = relationship(
        "NexusLike",
        back_populates="post",
        cascade="all, delete-orphan",
    )
    shares = relationship(
        "NexusShare",
        back_populates="post",
        cascade="all, delete-orphan",
    )
    views = relationship(
        "NexusView",
        back_populates="post",
        cascade="all, delete-orphan",
    )
    saves = relationship(
        "NexusSave",
        back_populates="post",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<NexusPost(id={self.id}, title='{self.title[:30]}', author_id={self.author_id})>"


# ============================================================
# 💬 2. COMENTÁRIOS (NexusComment)
# ============================================================
class NexusComment(Base):
    __tablename__ = "nexus_comments"

    id = Column(Integer, primary_key=True, index=True)
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    parent_id = Column(
        Integer,
        ForeignKey("nexus_comments.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )  # Suporte a respostas encadeadas (threads)

    content = Column(Text, nullable=False)
    likes_count = Column(Integer, default=0, nullable=False)
    is_edited = Column(Boolean, default=False, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relações
    post = relationship("NexusPost", back_populates="comments")
    user = relationship("User", foreign_keys=[user_id], lazy="selectin")
    parent = relationship("NexusComment", remote_side=[id], back_populates="replies")
    replies = relationship(
        "NexusComment",
        back_populates="parent",
        cascade="all, delete-orphan",
        order_by="NexusComment.created_at.asc()",
    )
    reactions = relationship(
        "NexusReaction",
        back_populates="comment",
        cascade="all, delete-orphan",
    )
    likes = relationship(
        "NexusLike",
        back_populates="comment",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<NexusComment(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


# ============================================================
# 🎭 3. REAÇÕES (NexusReaction)
# ============================================================
class NexusReaction(Base):
    __tablename__ = "nexus_reactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    comment_id = Column(
        Integer,
        ForeignKey("nexus_comments.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    reaction_type = Column(
        String(50),
        nullable=False,
        default="like",
        index=True,
    )  # like | curious | insight | inspiring | skeptical | applaud
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relações
    user = relationship("User", foreign_keys=[user_id])
    post = relationship("NexusPost", back_populates="reactions")
    comment = relationship("NexusComment", back_populates="reactions")

    __table_args__ = (
        UniqueConstraint("user_id", "post_id", "reaction_type", name="uq_nexus_reaction_user_post"),
        UniqueConstraint("user_id", "comment_id", "reaction_type", name="uq_nexus_reaction_user_comment"),
    )

    def __repr__(self):
        return f"<NexusReaction(user_id={self.user_id}, type='{self.reaction_type}')>"


# ============================================================
# ❤️ 4. GOSTOS / LIKES DIRETOS (NexusLike)
# ============================================================
class NexusLike(Base):
    __tablename__ = "nexus_likes"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    comment_id = Column(
        Integer,
        ForeignKey("nexus_comments.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relações
    user = relationship("User", foreign_keys=[user_id])
    post = relationship("NexusPost", back_populates="likes")
    comment = relationship("NexusComment", back_populates="likes")

    __table_args__ = (
        UniqueConstraint("user_id", "post_id", name="uq_nexus_like_user_post"),
        UniqueConstraint("user_id", "comment_id", name="uq_nexus_like_user_comment"),
    )

    def __repr__(self):
        return f"<NexusLike(user_id={self.user_id}, post_id={self.post_id}, comment_id={self.comment_id})>"


# ============================================================
# 🔄 5. PARTILHAS (NexusShare)
# ============================================================
class NexusShare(Base):
    __tablename__ = "nexus_shares"

    id = Column(Integer, primary_key=True, index=True)
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    platform = Column(
        String(50),
        nullable=False,
        default="copy_link",
    )  # copy_link | whatsapp | linkedin | twitter | facebook | embed
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    # Relações
    user = relationship("User", foreign_keys=[user_id])
    post = relationship("NexusPost", back_populates="shares")

    def __repr__(self):
        return f"<NexusShare(id={self.id}, post_id={self.post_id}, platform='{self.platform}')>"


# ============================================================
# 👁️ 6. VISUALIZAÇÕES / LEITURAS (NexusView)
# ============================================================
class NexusView(Base):
    __tablename__ = "nexus_views"

    id = Column(Integer, primary_key=True, index=True)
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)
    read_time_seconds = Column(Integer, default=0, nullable=False)
    viewed_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    # Relações
    user = relationship("User", foreign_keys=[user_id])
    post = relationship("NexusPost", back_populates="views")

    def __repr__(self):
        return f"<NexusView(id={self.id}, post_id={self.post_id}, read_time_seconds={self.read_time_seconds})>"


# ============================================================
# 🔖 7. ARTIGOS GUARDADOS / SAVES (NexusSave)
# ============================================================
class NexusSave(Base):
    __tablename__ = "nexus_saves"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    post_id = Column(
        Integer,
        ForeignKey("nexus_posts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    collection_name = Column(String(100), default="default", nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    # Relações
    user = relationship("User", foreign_keys=[user_id])
    post = relationship("NexusPost", back_populates="saves")

    __table_args__ = (
        UniqueConstraint("user_id", "post_id", name="uq_nexus_save_user_post"),
    )

    def __repr__(self):
        return f"<NexusSave(user_id={self.user_id}, post_id={self.post_id})>"
