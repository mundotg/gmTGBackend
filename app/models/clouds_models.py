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


class FileModel(Base):
    __tablename__ = "files"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, index=True
    )
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE")
    )
    filename: Mapped[str] = mapped_column(String(255))
    path: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    mime_type: Mapped[Optional[str]] = mapped_column(String(100))
    is_deleted: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    user = relationship("User", back_populates="files")


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
