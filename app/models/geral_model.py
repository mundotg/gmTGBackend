from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey
)
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base


class Settings(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)

    # 🔗 Relação 1:1 com User
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True
    )

    # ⚙️ Preferências do utilizador
    theme = Column(String(20), default="light", nullable=False)
    language = Column(String(10), default="pt", nullable=False)
    sidebar_collapsed = Column(Boolean, default=False, nullable=False)
    preferred_db_type = Column(String(50))
    # 🔔 Notificações (Falta isto para a aba "Notificações" do frontend)
    email_notifications = Column(Boolean, default=True, nullable=False)
    app_notifications = Column(Boolean, default=True, nullable=False)

    # 🌍 Regionalização (Opcional, mas útil para uma plataforma de dados)
    timezone = Column(String(50), default="UTC", nullable=False)

    # 🗄️ Dados locais
    #
    # Há metadados que a aplicação consulta uma vez e reutiliza: nomes de
    # tabelas, colunas, enums, contagens. Isso é rápido, mas fica desatualizado
    # assim que alguém altera o schema por fora — e quem está a trabalhar na
    # estrutura da base quer ver o estado real, não uma fotografia.
    #
    # A False, este utilizador passa a ler sempre da origem: o cache é ignorado
    # na leitura e não é escrito. É por utilizador de propósito, para que um
    # programador possa desligá-lo sem penalizar o desempenho de toda a gente.
    usar_dados_locais = Column(Boolean, default=True, nullable=False)

    # 🕒 Auditoria
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # 🔁 ORM relationship
    user = relationship(
        "User",
        back_populates="settings",
        foreign_keys=[user_id],
        uselist=False
    )

    def __repr__(self):
        return (
            f"<Settings(id={self.id}, user_id={self.user_id}, "
            f"theme='{self.theme}', language='{self.language}')>"
        )
