"""
Modelos de dados do Jogo Tapatan 3x3 (Alinhe 3 Tático).
Gerencia sessões de jogo (local, online multiplayer e vs IA),
histórico detalhado de jogadas, mensagens de chat ao vivo e estatísticas consolidadas.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import relationship

from app.database import Base



class SalaGame(Base):
    """
    Informações da sala de jogo (Tapatan Online Multiplayer).
    Gerencia código da sala, anfitrião, convidado, status da sala e configurações de acesso.
    """
    __tablename__ = "sala_game"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    room_code = Column(String(20), unique=True, nullable=False, index=True)
    nome_sala = Column(String(100), nullable=False, default="Sala Tapatan")
    host_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    guest_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    status = Column(String(20), nullable=False, default="waiting", index=True)  # waiting, active, finished, closed
    is_private = Column(Boolean, nullable=False, default=False)
    password_hash = Column(String(255), nullable=True)
    max_players = Column(Integer, nullable=False, default=2)
    mode = Column(String(20), nullable=False, default="online")  # online, local, ai
    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
        onupdate=func.now(),
    )

    # Relacionamentos
    host = relationship("User", foreign_keys=[host_user_id])
    guest = relationship("User", foreign_keys=[guest_user_id])
    sessions = relationship(
        "GameSession",
        back_populates="sala",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<SalaGame(id={self.id}, room_code='{self.room_code}', nome='{self.nome_sala}', status='{self.status}')>"


class GameSession(Base):
    """
    Sessão de partida do Tapatan 3x3.
    Suporta os modos:
    - 'local': 2 jogadores na mesma máquina
    - 'online': Duelo em tempo real via WebSocket em sala dedicada
    - 'ai': Humano vs IA com motor heurístico e aprendizado baseado em BD
    """
    __tablename__ = "game_sessions"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    sala_id = Column(sa.Uuid(), ForeignKey("sala_game.id", ondelete="SET NULL"), nullable=True, index=True)
    room_code = Column(String(20), unique=True, nullable=True, index=True)
    mode = Column(String(20), nullable=False, default="local", index=True)  # local, online, ai
    status = Column(String(20), nullable=False, default="active", index=True)  # waiting, active, finished, abandoned

    # Jogadores
    player1_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    player1_name = Column(String(100), nullable=False, default="Jogador 1")
    player2_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    player2_name = Column(String(100), nullable=False, default="Jogador 2")

    # Estado da partida
    current_turn = Column(Integer, nullable=False, default=1)  # 1 ou 2
    winner_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    winner_player = Column(Integer, nullable=True)  # 1 = P1, 2 = P2, 0 = Empate, None = Em andamento
    winning_line = Column(String(50), nullable=True)  # ex: "[2, 5, 8]"
    difficulty = Column(String(20), nullable=True, default="medium")  # easy, medium, hard (para modo IA)
    
    # Representação JSON do tabuleiro 3x3 (9 casas: índice 1 a 9)
    board_state = Column(sa.JSON, nullable=True)

    # Métricas
    total_moves = Column(Integer, nullable=False, default=0)
    duration_seconds = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    # Relacionamentos
    sala = relationship("SalaGame", back_populates="sessions")
    moves = relationship(
        "GameMove",
        back_populates="game",
        cascade="all, delete-orphan",
        order_by="GameMove.step",
        passive_deletes=True,
    )
    chats = relationship(
        "GameChat",
        back_populates="game",
        cascade="all, delete-orphan",
        order_by="GameChat.created_at",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<GameSession(id={self.id}, mode='{self.mode}', status='{self.status}', P1='{self.player1_name}', P2='{self.player2_name}')>"


class Matriz(Base):
    """
    Informações da matriz / estado do tabuleiro (Tapatan 3x3 ou variantes).
    Armazena a disposição das peças nas 9 casas, controle da Casa 5 (centro)
    e metadados da grade. Serve como superclasse para GameMove (Joined Table Inheritance).
    """
    __tablename__ = "matriz"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    tipo = Column(String(50), nullable=False, default="matriz", index=True)
    board_state = Column(sa.JSON, nullable=True)  # Disposição das 9 casas
    casa_centro = Column(Integer, nullable=False, default=0)  # Casa 5 (0=vazio, 1=P1, 2=P2)
    rows = Column(Integer, nullable=False, default=3)
    cols = Column(Integer, nullable=False, default=3)
    casas_ativas = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)

    __mapper_args__ = {
        "polymorphic_on": tipo,
        "polymorphic_identity": "matriz",
    }

    def __repr__(self) -> str:
        return f"<Matriz(id={self.id}, tipo='{self.tipo}', centro={self.casa_centro})>"


class GameMove(Matriz):
    """
    Registro individual de cada movimento executado numa partida.
    HERDA da classe e tabela 'matriz' (Joined Table Inheritance via ForeignKey).
    Armazena o estado do tabuleiro antes e depois para análise de replays
    e aprendizado do algoritmo da IA.
    """
    __tablename__ = "game_moves"

    id = Column(sa.Uuid(), ForeignKey("matriz.id", ondelete="CASCADE"), primary_key=True)
    game_id = Column(sa.Uuid(), ForeignKey("game_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    step = Column(Integer, nullable=False, index=True)
    player = Column(Integer, nullable=False)  # 1 ou 2
    from_casa = Column(Integer, nullable=False)  # 1 a 9
    to_casa = Column(Integer, nullable=False)    # 1 a 9
    board_before = Column(sa.JSON, nullable=True)
    board_after = Column(sa.JSON, nullable=True)
    tactical_intent = Column(String(255), nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": "move",
    }

    game = relationship("GameSession", back_populates="moves")

    def __repr__(self) -> str:
        return f"<GameMove(game_id={self.game_id}, step={self.step}, P{self.player}: {self.from_casa}->{self.to_casa})>"


class GameChat(Base):
    """
    Mensagens do chat em tempo real trocadas entre os jogadores durante uma partida online.
    """
    __tablename__ = "game_chats"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    game_id = Column(sa.Uuid(), ForeignKey("game_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    sender_player = Column(Integer, nullable=False, default=1)  # 1 = P1, 2 = P2, 0 = Sistema
    sender_name = Column(String(100), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)

    game = relationship("GameSession", back_populates="chats")

    def __repr__(self) -> str:
        return f"<GameChat(game_id={self.game_id}, sender='{self.sender_name}', msg='{self.message[:20]}')>"


class GamePlayerStats(Base):
    """
    Estatísticas consolidadas e histórico de vitórias/derrotas de jogadores.
    Pode ser indexado por user_id (usuários autenticados) ou nome de jogador.
    """
    __tablename__ = "game_player_stats"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, unique=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)

    total_games = Column(Integer, nullable=False, default=0)
    wins_local = Column(Integer, nullable=False, default=0)
    wins_online = Column(Integer, nullable=False, default=0)
    wins_vs_ai = Column(Integer, nullable=False, default=0)
    losses = Column(Integer, nullable=False, default=0)
    draws = Column(Integer, nullable=False, default=0)
    total_moves = Column(Integer, nullable=False, default=0)
    center_captures = Column(Integer, nullable=False, default=0)  # Vezes que ocupou a Casa 5
    win_streak = Column(Integer, nullable=False, default=0)
    best_win_streak = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        total_wins = self.wins_local + self.wins_online + self.wins_vs_ai
        return f"<GamePlayerStats(player='{self.player_name}', total={self.total_games}, wins={total_wins})>"
