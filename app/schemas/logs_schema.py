import enum

from pydantic import BaseModel
from datetime import datetime
from typing import Literal


class LogModel(BaseModel):
    message: str
    level: Literal["info", "error", "success", "warning"] = "info"
    timestamp: datetime = datetime.now()


class LogLevel(enum.Enum):
    info = "info"
    error = "error"
    warning = "warning"
    success = "success"
