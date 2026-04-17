from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime
from app.database import Base  # tua base declarativa


class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)

    message = Column(Text, nullable=False)

    level = Column(String(20), nullable=False, default="info")

    source = Column(String(100), nullable=True)

    user = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
