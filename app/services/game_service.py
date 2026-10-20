"""
Serviço principal de regras de negócio do Jogo Tapatan 3x3.
Gerencia partidas locais, duelos online, finalizações, persistência de jogadas e estatísticas.
"""
from __future__ import annotations

import time
import random
import string
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from sqlalchemy.orm import selectinload

from app.config.redis import delete_cache, read_cache, write_cache
from app.core.config import GAME_STATS_CACHE_TTL_SECONDS
from app.models.game_models import (
    GameChat,
    GameMove,
    GamePlayerStats,
    GameSession,
    Matriz,
    SalaGame,
)
from app.schemas.game import (
    AIMoveRequest,
    AIMoveResponse,
    GameChatCreate,
    GameFinishRequest,
    GameMoveCreate,
    GameSessionCreate,
    SalaGameCreate,
)
from app.services.game_ai_service import game_ai_service, normalize_board_to_dict
from app.services.game_websocket_manager import game_ws_manager
from app.ultils.logger import log_message


def generate_room_code() -> str:
    """Gera um código de sala amigável no formato 'TAP-XXXX'."""
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return f"TAP-{suffix}"


class GameSessionNotFoundError(Exception):
    """Lançada quando a sessão de jogo não é encontrada no banco de dados."""
    pass


class InvalidGameMoveError(Exception):
    """Lançada quando um movimento viola as regras do jogo."""
    pass


class GameService:
    """Regras de negócio para partidas do Tapatan."""

    def __init__(self):
        # Cache em memória (RAM) como fallback caso o Redis esteja indisponível
        self._stats_memory_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _get_cache_key(self, player_name: str) -> str:
        clean = (player_name or "").strip().lower()
        return f"game:stats:{clean}"

    def get_cached_stats(self, player_name: str) -> Optional[Dict[str, Any]]:
        """Busca estatísticas em cache com TTL de 1 minuto (Redis + Fallback RAM)."""
        cache_key = self._get_cache_key(player_name)

        # 1. Tenta obter do Redis
        try:
            cached = read_cache(cache_key)
            if cached and isinstance(cached, dict):
                return cached
        except Exception as e:
            log_message(f"⚠️ Erro ao consultar Redis para {cache_key}: {e}", "warning")

        # 2. Fallback de memória local em RAM
        now = time.time()
        mem_entry = self._stats_memory_cache.get(cache_key)
        if mem_entry:
            expires_at, data = mem_entry
            if now < expires_at:
                return data
            else:
                self._stats_memory_cache.pop(cache_key, None)

        return None

    def set_cached_stats(
        self,
        player_name: str,
        stats_data: Dict[str, Any],
        ttl: int = GAME_STATS_CACHE_TTL_SECONDS,
    ) -> None:
        """Armazena as estatísticas no cache por 1 minuto (Redis + RAM)."""
        cache_key = self._get_cache_key(player_name)

        # 1. Salva no Redis
        try:
            write_cache(cache_key, stats_data, ttl=ttl)
        except Exception as e:
            log_message(f"⚠️ Erro ao gravar stats no Redis: {e}", "warning")

        # 2. Salva na memória local
        expires_at = time.time() + ttl
        self._stats_memory_cache[cache_key] = (expires_at, stats_data)

    def invalidate_cached_stats(self, player_name: str) -> None:
        """Invalida o cache das estatísticas de um jogador."""
        cache_key = self._get_cache_key(player_name)
        try:
            delete_cache(cache_key)
        except Exception:
            pass
        self._stats_memory_cache.pop(cache_key, None)

    async def create_session(
        self,
        db: AsyncSession,
        data: GameSessionCreate,
        user_id: Optional[int] = None,
    ) -> GameSession:
        """Cria uma nova sessão de jogo."""
        room_code = generate_room_code() if data.mode == "online" else None

        initial_board = {
            1: 1, 2: 1, 3: 1,
            4: 0, 5: 0, 6: 0,
            7: 2, 8: 2, 9: 2,
        }

        sala = None
        if data.mode == "online" and room_code:
            sala = SalaGame(
                room_code=room_code,
                nome_sala=f"Sala {room_code}",
                host_user_id=user_id,
                mode="online",
                status="waiting",
            )
            db.add(sala)
            await db.flush()

        session = GameSession(
            sala_id=sala.id if sala else data.sala_id,
            room_code=room_code,
            mode=data.mode,
            status="waiting" if data.mode == "online" else "active",
            player1_id=user_id,
            player1_name=data.player1_name or "Jogador 1",
            player2_id=None,
            player2_name=data.player2_name or ("IA Alpha" if data.mode == "ai" else "Jogador 2"),
            difficulty=data.difficulty or "medium",
            board_state=initial_board,
            current_turn=1,
        )
        db.add(session)
        await db.commit()
        await db.refresh(session)

        # Se for online, registra a sala no gerenciador WebSocket
        if data.mode == "online" and room_code:
            await game_ws_manager.create_or_get_room(
                room_code=room_code,
                game_id=session.id,
                player1_name=session.player1_name,
                user_id=user_id,
            )

        return session

    async def record_move(
        self,
        db: AsyncSession,
        game_id: uuid.UUID,
        move_data: GameMoveCreate,
    ) -> GameMove:
        """Registra uma jogada executada num jogo local ou vs IA, persistindo na tabela matriz."""
        session = await db.get(GameSession, game_id)
        if not session:
            raise GameSessionNotFoundError(f"Sessão de jogo '{game_id}' não encontrada.")

        board_before = normalize_board_to_dict(move_data.board_before)
        board_after = normalize_board_to_dict(move_data.board_after)
        centro = int(board_after.get("5", board_after.get(5, 0))) if board_after else 0
        ativas = sum(1 for v in board_after.values() if v in (1, 2)) if board_after else 0

        move = GameMove(
            game_id=game_id,
            step=move_data.step,
            player=move_data.player,
            from_casa=move_data.from_casa,
            to_casa=move_data.to_casa,
            board_before=board_before,
            board_after=board_after,
            board_state=board_after,
            casa_centro=centro,
            rows=3,
            cols=3,
            casas_ativas=ativas,
            tactical_intent=move_data.tactical_intent or ("Controle central" if move_data.to_casa == 5 else f"Movimento {move_data.from_casa}->{move_data.to_casa}"),
        )
        db.add(move)

        session.total_moves = move_data.step
        session.board_state = board_after
        session.current_turn = 2 if move_data.player == 1 else 1

        await db.commit()
        await db.refresh(move)
        return move

    async def finish_session(
        self,
        db: AsyncSession,
        game_id: uuid.UUID,
        finish_data: GameFinishRequest,
    ) -> GameSession:
        """Conclui a partida e atualiza os placares/estatísticas."""
        session = await db.get(GameSession, game_id)
        if not session:
            raise GameSessionNotFoundError(f"Sessão de jogo '{game_id}' não encontrada.")

        session.status = "finished"
        session.winner_player = finish_data.winner_player
        session.winning_line = finish_data.winning_line
        session.duration_seconds = finish_data.duration_seconds
        session.total_moves = finish_data.total_moves
        session.finished_at = datetime.utcnow()
        if finish_data.board_state is not None:
            session.board_state = normalize_board_to_dict(finish_data.board_state)

        # Atualiza estatísticas do Jogador 1
        await self._update_stats(
            db,
            session.player1_name,
            session.player1_id,
            mode=session.mode,
            is_win=(finish_data.winner_player == 1),
            is_draw=(finish_data.winner_player == 0),
            moves=finish_data.total_moves,
        )

        # Atualiza estatísticas do Jogador 2
        await self._update_stats(
            db,
            session.player2_name,
            session.player2_id,
            mode=session.mode,
            is_win=(finish_data.winner_player == 2),
            is_draw=(finish_data.winner_player == 0),
            moves=finish_data.total_moves,
        )

        await db.commit()
        await db.refresh(session)
        return session

    async def _update_stats(
        self,
        db: AsyncSession,
        player_name: str,
        user_id: Optional[int],
        mode: str,
        is_win: bool,
        is_draw: bool,
        moves: int,
    ) -> None:
        """Atualiza ou cria a ficha de estatísticas do jogador."""
        stmt = select(GamePlayerStats).where(GamePlayerStats.player_name == player_name)
        result = await db.execute(stmt)
        stats = result.scalar_one_or_none()

        if not stats:
            stats = GamePlayerStats(
                player_name=player_name,
                user_id=user_id,
                total_games=0,
                wins_local=0,
                wins_online=0,
                wins_vs_ai=0,
                losses=0,
                draws=0,
                total_moves=0,
                center_captures=0,
                win_streak=0,
                best_win_streak=0,
            )
            db.add(stats)

        stats.total_games = (stats.total_games or 0) + 1
        stats.total_moves = (stats.total_moves or 0) + moves

        if is_win:
            if mode == "local":
                stats.wins_local = (stats.wins_local or 0) + 1
            elif mode == "online":
                stats.wins_online = (stats.wins_online or 0) + 1
            else:
                stats.wins_vs_ai = (stats.wins_vs_ai or 0) + 1
            current_streak = (stats.win_streak or 0) + 1
            stats.win_streak = current_streak
            if current_streak > (stats.best_win_streak or 0):
                stats.best_win_streak = current_streak
        elif is_draw:
            stats.draws = (stats.draws or 0) + 1
            stats.win_streak = 0
        else:
            stats.losses = (stats.losses or 0) + 1
            stats.win_streak = 0

        # Invalida o cache do jogador e o cache global para que as estatísticas reflitam o novo jogo
        self.invalidate_cached_stats(player_name)
        self.invalidate_cached_stats("__global__")


    async def get_session_detail(
        self, db: AsyncSession, game_id: uuid.UUID
    ) -> Optional[GameSession]:
        """Retorna detalhes da partida com movimentos e mensagens de chat."""
        stmt = (
            select(GameSession)
            .where(GameSession.id == game_id)
            .options(
                selectinload(GameSession.moves),
                selectinload(GameSession.chats),
            )
        )
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_session_by_room(
        self, db: AsyncSession, room_code: str
    ) -> Optional[GameSession]:
        """Busca partida ativa pelo código da sala online, validando se não expirou."""
        stmt = select(GameSession).where(GameSession.room_code == room_code.upper())
        result = await db.execute(stmt)
        session = result.scalar_one_or_none()
        if not session:
            return None
        # Partidas finalizadas ou abandonadas não estão mais disponíveis para conexão
        if session.status in ["finished", "abandoned"]:
            return None
        # Se estiver em espera há mais de 24 horas, considera expirada
        if session.status == "waiting" and session.created_at:
            from datetime import timedelta
            if (datetime.utcnow() - session.created_at) > timedelta(hours=24):
                session.status = "abandoned"
                await db.commit()
                return None
        return session

    async def list_recent_sessions(
        self, db: AsyncSession, limit: int = 15
    ) -> List[GameSession]:
        """Lista partidas recentes para o mural de duelos."""
        stmt = (
            select(GameSession)
            .order_by(desc(GameSession.created_at))
            .limit(limit)
        )
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_stats(
        self, db: AsyncSession, player_name: str, force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Retorna estatísticas consolidadas de um jogador.
        Os dados são cacheados por 1 minuto (GAME_STATS_CACHE_TTL_SECONDS = 60s)
        no Redis (com fallback automático em RAM).
        """
        if not force_refresh:
            cached = self.get_cached_stats(player_name)
            if cached is not None:
                return cached

        stmt = select(GamePlayerStats).where(GamePlayerStats.player_name == player_name)
        result = await db.execute(stmt)
        stats = result.scalar_one_or_none()

        if not stats:
            return None

        total_wins = stats.wins_local + stats.wins_online + stats.wins_vs_ai
        win_rate = round((total_wins / stats.total_games * 100), 1) if stats.total_games > 0 else 0.0

        stats_data = {
            "player_name": stats.player_name,
            "total_games": stats.total_games,
            "wins_local": stats.wins_local,
            "wins_online": stats.wins_online,
            "wins_vs_ai": stats.wins_vs_ai,
            "total_wins": total_wins,
            "losses": stats.losses,
            "draws": stats.draws,
            "win_rate": win_rate,
            "total_moves": stats.total_moves,
            "center_captures": stats.center_captures,
            "win_streak": stats.win_streak,
            "best_win_streak": stats.best_win_streak,
        }

        # Armazena em cache por 1 minuto (configurável no .env)
        self.set_cached_stats(player_name, stats_data, ttl=GAME_STATS_CACHE_TTL_SECONDS)

        return stats_data

    async def get_stats_with_status(
        self, db: AsyncSession, player_name: str, force_refresh: bool = False
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Retorna estatísticas consolidadas de um jogador e indica se foi obtido do cache (HIT=True) ou banco (MISS=False).
        Cache configurado para 1 minuto (GAME_STATS_CACHE_TTL_SECONDS = 60s).
        """
        if not force_refresh:
            cached = self.get_cached_stats(player_name)
            if cached is not None:
                return cached, True

        stats_data = await self.get_stats(db, player_name, force_refresh=True)
        return stats_data, False

    async def get_global_stats(
        self, db: AsyncSession, force_refresh: bool = False
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Retorna estatísticas globais e consolidadas da plataforma do jogo Tapatan.
        Cacheadas por 1 minuto (GAME_STATS_CACHE_TTL_SECONDS = 60s) no Redis + RAM.
        """
        if not force_refresh:
            cached = self.get_cached_stats("__global__")
            if cached is not None:
                return cached, True

        # 1. Total partidas e contagens por modo
        stmt_sessions = select(
            func.count(GameSession.id).label("total"),
            func.count(GameSession.id).filter(GameSession.mode == "local").label("local"),
            func.count(GameSession.id).filter(GameSession.mode == "online").label("online"),
            func.count(GameSession.id).filter(GameSession.mode == "ai").label("ai"),
        )
        res_sessions = await db.execute(stmt_sessions)
        row_sessions = res_sessions.one()

        # 2. Total de movimentos registrados
        stmt_moves = select(func.count(GameMove.id))
        total_moves = (await db.execute(stmt_moves)).scalar() or 0

        # 3. Total de jogadores com estatísticas
        stmt_players = select(func.count(GamePlayerStats.id))
        total_players = (await db.execute(stmt_players)).scalar() or 0

        # 4. Top 10 jogadores (ranking por total de vitórias)
        stmt_top = (
            select(GamePlayerStats)
            .order_by(
                desc(GamePlayerStats.wins_local + GamePlayerStats.wins_online + GamePlayerStats.wins_vs_ai),
                desc(GamePlayerStats.best_win_streak),
            )
            .limit(10)
        )
        res_top = await db.execute(stmt_top)
        top_records = res_top.scalars().all()

        top_players = []
        for s in top_records:
            tot_w = (s.wins_local or 0) + (s.wins_online or 0) + (s.wins_vs_ai or 0)
            wr = round((tot_w / s.total_games * 100), 1) if s.total_games > 0 else 0.0
            top_players.append({
                "player_name": s.player_name,
                "total_games": s.total_games or 0,
                "wins_local": s.wins_local or 0,
                "wins_online": s.wins_online or 0,
                "wins_vs_ai": s.wins_vs_ai or 0,
                "total_wins": tot_w,
                "losses": s.losses or 0,
                "draws": s.draws or 0,
                "win_rate": wr,
                "total_moves": s.total_moves or 0,
                "center_captures": s.center_captures or 0,
                "win_streak": s.win_streak or 0,
                "best_win_streak": s.best_win_streak or 0,
            })

        active_rooms = len(game_ws_manager.rooms)

        stats_data = {
            "total_games": row_sessions.total or 0,
            "total_local_games": row_sessions.local or 0,
            "total_online_games": row_sessions.online or 0,
            "total_ai_games": row_sessions.ai or 0,
            "total_moves": total_moves,
            "total_players": total_players,
            "active_online_rooms": active_rooms,
            "top_players": top_players,
            "cache_ttl_seconds": GAME_STATS_CACHE_TTL_SECONDS,
        }

        # Armazena em cache por 1 minuto (60s)
        self.set_cached_stats("__global__", stats_data, ttl=GAME_STATS_CACHE_TTL_SECONDS)
        return stats_data, False

    async def compute_ai_move(
        self, db: AsyncSession, req: AIMoveRequest
    ) -> AIMoveResponse:
        """Gera jogada calculada da IA utilizando o motor tri-camada."""
        result = await game_ai_service.get_best_move(
            db=db,
            board_input=req.board_state,
            ai_player=req.player,
            difficulty=req.difficulty,
        )
        return AIMoveResponse(
            from_casa=result["from_casa"],
            to_casa=result["to_casa"],
            tactical_intent=result["tactical_intent"],
            source=result["source"],
        )

    async def create_room(
        self,
        db: AsyncSession,
        data: SalaGameCreate,
        host_user_id: Optional[int] = None,
    ) -> SalaGame:
        """Cria uma sala de jogo na tabela sala_game."""
        room_code = generate_room_code()
        sala = SalaGame(
            room_code=room_code,
            nome_sala=data.nome_sala or f"Sala {room_code}",
            host_user_id=host_user_id,
            status="waiting",
            is_private=data.is_private,
            password_hash=data.password,  # se desejar hash, aplicar antes
            max_players=data.max_players,
            mode=data.mode,
        )
        db.add(sala)
        await db.commit()
        await db.refresh(sala)
        return sala

    async def get_room_by_code(
        self, db: AsyncSession, room_code: str
    ) -> Optional[SalaGame]:
        """Obtém os detalhes da sala_game pelo código da sala."""
        stmt = select(SalaGame).where(SalaGame.room_code == room_code.upper())
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_rooms(
        self,
        db: AsyncSession,
        status_filter: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[SalaGame]:
        """Lista salas_game disponíveis."""
        stmt = select(SalaGame).order_by(desc(SalaGame.created_at)).limit(limit).offset(offset)
        if status_filter:
            stmt = stmt.where(SalaGame.status == status_filter)
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_matriz_by_id(
        self, db: AsyncSession, matriz_id: uuid.UUID
    ) -> Optional[Matriz]:
        """Busca o registro do tabuleiro ou jogada na tabela matriz."""
        return await db.get(Matriz, matriz_id)


game_service = GameService()
