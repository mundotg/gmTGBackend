import uuid
import enum
from datetime import datetime, date as dt_date
from typing import Optional

from sqlalchemy import (
    Integer,
    String,
    BigInteger,
    ForeignKey,
    Text,
    UniqueConstraint,
    Enum,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base

ALLOWED_EXTENSIONS = {
    ".txt",
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".json",
    ".csv",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".mp4",
    ".mp3",
    ".avi",
    ".mkv",
    ".csv",
    ".json",
    ".xml",
    ".html",
    ".css",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".py",
    ".java",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".go",
    ".rb",
    ".php",
    ".swift",
    ".kt",
    ".rs",
    ".dart",
    ".sh",
    ".bat",
    ".ps1",
    ".sql",
    ".md",
    ".yml",
    ".yaml",
    ".log",
    ".cfg",
    ".ini",
    ".env",
    ".dockerfile",
    ".k8s.yaml",
    ".helm.yaml",
    ".ipynb",
    ".r",
    ".sas",
    ".stata",
    ".spss",
    ".m",
    ".lua",
    ".groovy",
    ".scala",
    ".clj",
    ".cljs",
    ".coffee",
    ".asm",
    ".v",
    ".sv",
    ".vh",
    ".vhd",
    ".vhdl",
    ".svelte",
    ".vue",
    ".angular",
    ".react",
    ".ember",
    ".backbone",
    ".flutter",
    ".dart",
    ".xcodeproj",
    ".xcworkspace",
    ".sln",
    ".csproj",
    ".vbproj",
    ".fsproj",
    ".fsx",
    ".fsi",
    ".fs",
    ".fsproj",
    ".fsx",
    ".fsi",
    ".fs",
    ".gradle",
    ".pom.xml",
    ".maven",
    ".ant",
    ".makefile",
    ".cmake",
    ".meson",
    ".ninja",
    ".bazel",
    ".buck",
    ".buildkite.yml",
    ".circleci.yml",
    ".travis.yml",
}


class LogAction(str, enum.Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"
    DELETE = "delete"


class LogStatus(str, enum.Enum):
    SUCCESS = "success"
    ERROR = "error"


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    max_storage_mb: Mapped[int] = mapped_column(nullable=False)
    max_requests_per_day: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    users = relationship("User", back_populates="plan")


class StorageFolder(Base):
    """Pasta do storage, em árvore, com senha opcional.

    A senha é **controlo de acesso**, não cifragem: guarda-se o hash bcrypt e o
    conteúdo fica legível no bucket. Protege tudo o que está abaixo — para
    chegar a um ficheiro é preciso ter desbloqueado todas as pastas com senha
    no caminho até à raiz (ver `storage_folder_service`).

    As pastas são apenas metadados: a chave do objeto continua a ser
    `{user_id}/{uuid}.ext`, portanto renomear ou mover uma pasta não mexe em
    nada no bucket.
    """

    __tablename__ = "storage_folders"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, index=True
    )
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("storage_folders.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255))
    password_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_deleted: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    user = relationship("User")
    parent = relationship("StorageFolder", remote_side=[id], backref="children")

    __table_args__ = (
        UniqueConstraint(
            "user_id", "parent_id", "name", name="uq_folder_user_parent_name"
        ),
    )

    @property
    def is_locked(self) -> bool:
        return bool(self.password_hash)


class FileModel(Base):
    __tablename__ = "files"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, index=True
    )
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE")
    )
    folder_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("storage_folders.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    filename: Mapped[str] = mapped_column(String(255))
    path: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    mime_type: Mapped[Optional[str]] = mapped_column(String(100))
    is_deleted: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    user = relationship("User", back_populates="files")
    folder = relationship("StorageFolder")


class StorageUsage(Base):
    __tablename__ = "storage_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), unique=True
    )
    used_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    user = relationship("User", back_populates="storage_usage")


class RequestUsage(Base):
    __tablename__ = "request_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE")
    )
    date: Mapped[dt_date] = mapped_column(default=dt_date.today)
    request_count: Mapped[int] = mapped_column(default=0)

    user = relationship("User", back_populates="request_usage")

    __table_args__ = (
        UniqueConstraint("user_id", "date", name="uq_user_date_requests"),
    )


class NetworkMetric(Base):
    __tablename__ = "network_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE")
    )
    date: Mapped[dt_date] = mapped_column(default=dt_date.today)

    ingress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    egress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)

    user = relationship("User", back_populates="network_metrics")

    __table_args__ = (UniqueConstraint("user_id", "date", name="uq_user_date_network"),)


class LogCloud(Base):
    __tablename__ = "logs_clouds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE")
    )
    action: Mapped[LogAction] = mapped_column(Enum(LogAction))
    filename: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[LogStatus] = mapped_column(Enum(LogStatus))
    message: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
