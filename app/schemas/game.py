"""
Schemas Pydantic v2 para validação e serialização do Jogo Tapatan 3x3.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class GameMoveCreate(BaseModel):
    step: int = Field(..., ge=1, description="Número do turno sequencial")
    player: int = Field(..., ge=1, le=2, description="1 para Jogador 1, 2 para Jogador 2")
    from_casa: int = Field(..., ge=1, le=9, description="Casa de origem (1 a 9)")
    to_casa: int = Field(..., ge=1, le=9, description="Casa de destino (1 a 9)")
    board_before: Optional[Any] = Field(default=None, description="Estado do tabuleiro antes do movimento")
    board_after: Optional[Any] = Field(default=None, description="Estado do tabuleiro após o movimento")
    tactical_intent: Optional[str] = Field(default=None, description="Intenção estratégica da jogada")


class MatrizResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tipo: str = "matriz"
    board_state: Optional[Any] = None
    casa_centro: int = 0
    rows: int = 3
    cols: int = 3
    casas_ativas: int = 0
    created_at: datetime


class GameMoveResponse(MatrizResponse):
    game_id: uuid.UUID
    step: int
    player: int
    from_casa: int
    to_casa: int
    board_before: Optional[Any] = None
    board_after: Optional[Any] = None
    tactical_intent: Optional[str] = None


class GameChatCreate(BaseModel):
    sender_player: int = Field(default=1, ge=0, le=2, description="0=Sistema, 1=P1, 2=P2")
    sender_name: str = Field(..., min_length=1, max_length=100)
    message: str = Field(..., min_length=1, max_length=500)


class GameChatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    game_id: uuid.UUID
    sender_player: int
    sender_name: str
    message: str
    created_at: datetime


class SalaGameCreate(BaseModel):
    nome_sala: Optional[str] = Field(default="Sala Tapatan", max_length=100)
    is_private: bool = Field(default=False)
    password: Optional[str] = Field(default=None, max_length=50)
    max_players: int = Field(default=2, ge=2, le=4)
    mode: str = Field(default="online")


class SalaGameResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    room_code: str
    nome_sala: str
    host_user_id: Optional[int] = None
    guest_user_id: Optional[int] = None
    status: str
    is_private: bool
    max_players: int
    mode: str
    created_at: datetime
    updated_at: datetime


class GameSessionCreate(BaseModel):
    mode: str = Field(default="local", description="Modo: 'local', 'online', 'ai'")
    player1_name: str = Field(default="Jogador 1", min_length=1, max_length=100)
    player2_name: str = Field(default="Jogador 2", min_length=1, max_length=100)
    difficulty: Optional[str] = Field(default="medium", description="Dificuldade da IA: 'easy', 'medium', 'hard'")
    sala_id: Optional[uuid.UUID] = Field(default=None, description="ID da sala_game associada")


class GameFinishRequest(BaseModel):
    winner_player: Optional[int] = Field(default=None, description="1=P1, 2=P2, 0=Empate")
    winning_line: Optional[str] = Field(default=None, description="Trinca vencedora, ex: '[2, 5, 8]'")
    duration_seconds: int = Field(default=0, ge=0)
    total_moves: int = Field(default=0, ge=0)
    board_state: Optional[Any] = None


class GameSessionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    sala_id: Optional[uuid.UUID] = None
    room_code: Optional[str] = None
    mode: str
    status: str
    player1_name: str
    player2_name: str
    current_turn: int
    winner_player: Optional[int] = None
    winning_line: Optional[str] = None
    difficulty: Optional[str] = None
    board_state: Optional[Any] = None
    total_moves: int
    duration_seconds: int
    created_at: datetime
    finished_at: Optional[datetime] = None


class GameSessionDetailResponse(GameSessionResponse):
    moves: List[GameMoveResponse] = []
    chats: List[GameChatResponse] = []


class GameStatsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    player_name: str
    total_games: int = 0
    wins_local: int = 0
    wins_online: int = 0
    wins_vs_ai: int = 0
    total_wins: int = 0
    losses: int = 0
    draws: int = 0
    win_rate: float = 0.0
    total_moves: int = 0
    center_captures: int = 0
    win_streak: int = 0
    best_win_streak: int = 0


class GameGlobalStatsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total_games: int = 0
    total_local_games: int = 0
    total_online_games: int = 0
    total_ai_games: int = 0
    total_moves: int = 0
    total_players: int = 0
    active_online_rooms: int = 0
    top_players: List[GameStatsResponse] = []
    cache_ttl_seconds: int = 60


class AIMoveRequest(BaseModel):
    board_state: Any = Field(..., description="Matriz ou mapa das 9 casas do tabuleiro")
    player: int = Field(default=2, ge=1, le=2, description="Jogador que a IA está controlando")
    difficulty: str = Field(default="medium", description="'easy', 'medium', ou 'hard'")


class AIMoveResponse(BaseModel):
    from_casa: int
    to_casa: int
    tactical_intent: str
    source: str = Field(..., description="'database_learned', 'knowledge_file', ou 'heuristic'")


class RoomCreateResponse(BaseModel):
    room_code: str
    game_id: uuid.UUID
    mode: str
    status: str
    ws_url: str


# ══════════════════════════════════════════════════════════
# ⚡ SCHEMAS DE VALIDAÇÃO PYDANTIC PARA WEBSOCKET
# ══════════════════════════════════════════════════════════


class WSMoveMessage(BaseModel):
    """Valida o payload de movimento de uma peça enviado pelo cliente via WebSocket."""
    from_casa: int = Field(..., ge=1, le=9, description="Casa de origem (1 a 9)")
    to_casa: int = Field(..., ge=1, le=9, description="Casa de destino (1 a 9)")


class WSChatMessage(BaseModel):
    """Valida mensagens de chat enviadas durante a partida online."""
    message: str = Field(..., min_length=1, max_length=500, description="Texto da mensagem")


class WSDrawMessage(BaseModel):
    """Valida ações do protocolo de empate mútuo."""
    action: str = Field(default="propose", pattern="^(propose|accept|reject)$")
