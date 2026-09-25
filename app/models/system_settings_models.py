"""
⚙️ Definições globais da aplicação.

Chave/valor em vez de uma coluna por definição: estas opções nascem e morrem ao
ritmo do produto, e cada uma nova não deve custar uma migração. O preço é não
haver tipos ao nível da base de dados — por isso a leitura passa sempre pelo
`system_settings_service`, que conhece o tipo e o valor por omissão de cada
chave e nunca devolve o texto em cru.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from app.database import Base


class SystemSetting(Base):
    __tablename__ = "system_settings"

    key = Column(String(64), primary_key=True)

    # Serializado em texto (`"true"`, `"false"`, números, JSON). Quem lê sabe
    # o tipo que espera; ver `DEFINICOES` no serviço.
    value = Column(Text, nullable=False)

    # Quem mexeu pela última vez. Estas opções mudam o comportamento de toda a
    # instalação — pôr a aplicação em manutenção, por exemplo — e saber quem o
    # fez é metade do valor de as ter numa tabela em vez de num ficheiro .env.
    updated_by_id = Column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    updated_by = relationship("User")

    def __repr__(self) -> str:
        return f"<SystemSetting(key='{self.key}', value='{self.value}')>"
