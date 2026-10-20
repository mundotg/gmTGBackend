"""
Gerenciador de Conexões WebSocket e Salas de Jogo em Tempo Real para o Tapatan 3x3.
Suporta:
- Criação e pareamento de salas online
- Validação estrita de movimentos no servidor
- Sincronização em tempo real de estados, jogadas e turnos
- Chat ao vivo entre os jogadores com persistência no PostgreSQL
- Proposta de empate, rendição e revanche
"""
from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket, WebSocketDisconnect

from app.core.config import (
    GAME_MAX_CHAT_HISTORY,
    GAME_RECONNECT_TIMEOUT_SECONDS,
    GAME_TURN_TIMEOUT_SECONDS,
)
from app.database import AsyncSessionLocal
from app.models.game_models import GameChat, GameMove, GamePlayerStats, GameSession
from app.services.game_ai_service import (
    CONDICOES_DE_VITORIA,
    MOVIMENTOS_VALIDOS,
    check_winner,
    get_legal_moves,
    normalize_board_to_dict,
)
from app.ultils.logger import log_message


class GameRoom:
    """Representa uma sala ativa de jogo online via WebSocket."""

    def __init__(self, room_code: str, game_id: uuid.UUID):
        self.room_code = room_code
        self.game_id = game_id

        # Conexões WebSocket
        self.player1_ws: Optional[WebSocket] = None
        self.player1_name: str = "Jogador 1"
        self.player1_id: Optional[int] = None

        self.player2_ws: Optional[WebSocket] = None
        self.player2_name: str = "Jogador 2"
        self.player2_id: Optional[int] = None

        self.spectators: Set[WebSocket] = set()

        # Estado da partida
        # 1 a 3: P1, 4 a 6: vazias, 7 a 9: P2
        self.board_state: Dict[int, int] = {
            1: 1, 2: 1, 3: 1,
            4: 0, 5: 0, 6: 0,
            7: 2, 8: 2, 9: 2,
        }
        self.current_turn: int = 1
        self.status: str = "waiting"  # waiting, active, finished, abandoned
        self.total_moves: int = 0
        self.start_time: Optional[datetime] = None
        self.draw_offered_by: Optional[int] = None
        self.rematch_votes: Set[int] = set()

        # Tarefas de desconexão (reconectar)
        self.reconnect_tasks: Dict[int, asyncio.Task] = {}

    @property
    def is_full(self) -> bool:
        return self.player1_ws is not None and self.player2_ws is not None

    def get_player_number(self, ws: WebSocket) -> Optional[int]:
        if ws == self.player1_ws:
            return 1
        if ws == self.player2_ws:
            return 2
        return None

    def get_player_name(self, player_num: int) -> str:
        return self.player1_name if player_num == 1 else self.player2_name

    def to_state_dict(self) -> Dict[str, Any]:
        """Serializa o estado atual da sala para os clientes."""
        return {
            "room_code": self.room_code,
            "game_id": str(self.game_id),
            "status": self.status,
            "current_turn": self.current_turn,
            "player1_name": self.player1_name,
            "player2_name": self.player2_name,
            "player1_connected": self.player1_ws is not None,
            "player2_connected": self.player2_ws is not None,
            "board_state": self.board_state,
            "total_moves": self.total_moves,
            "spectators_count": len(self.spectators),
            "turn_timeout": GAME_TURN_TIMEOUT_SECONDS,
        }


class GameWebSocketManager:
    """Gerencia todas as salas ativas de jogos e os WebSockets associados."""

    def __init__(self):
        self.rooms: Dict[str, GameRoom] = {}
        self.lock = asyncio.Lock()

    def get_room(self, room_code: str) -> Optional[GameRoom]:
        return self.rooms.get(room_code.upper())

    async def get_or_restore_room(
        self, room_code: str, db: AsyncSession
    ) -> Optional[GameRoom]:
        """
        Busca a sala na memória de forma atômica com lock contra race conditions,
        restaurando do banco de dados se a instância foi reiniciada.
        """
        async with self.lock:
            code = room_code.upper()
            if code in self.rooms:
                return self.rooms[code]

            from app.models.game_models import GameSession
            from sqlalchemy import select
            stmt = select(GameSession).where(GameSession.room_code == code)
            result = await db.execute(stmt)
            db_game = result.scalar_one_or_none()
            if not db_game:
                return None
            if db_game.status in ["finished", "abandoned"]:
                return None

            room = GameRoom(room_code=code, game_id=db_game.id)
            room.player1_name = db_game.player1_name
            room.player1_id = db_game.player1_id
            self.rooms[code] = room
            return room

    async def create_or_get_room(
        self, room_code: str, game_id: uuid.UUID, player1_name: str, user_id: Optional[Any] = None
    ) -> GameRoom:
        async with self.lock:
            code = room_code.upper()
            if code not in self.rooms:
                room = GameRoom(room_code=code, game_id=game_id)
                room.player1_name = player1_name
                room.player1_id = user_id
                self.rooms[code] = room
            return self.rooms[code]

    async def broadcast_to_room(
        self,
        room: GameRoom,
        message: Dict[str, Any],
        exclude_ws: Optional[WebSocket] = None,
    ) -> None:
        """Envia mensagem JSON a todos os participantes e espectadores da sala."""
        recipients = []
        if room.player1_ws and room.player1_ws != exclude_ws:
            recipients.append(room.player1_ws)
        if room.player2_ws and room.player2_ws != exclude_ws:
            recipients.append(room.player2_ws)
        for s_ws in room.spectators:
            if s_ws != exclude_ws:
                recipients.append(s_ws)

        dead_connections = []
        for ws in recipients:
            try:
                await ws.send_json(message)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            if ws in room.spectators:
                room.spectators.discard(ws)

    async def handle_connect(
        self,
        websocket: WebSocket,
        room_code: str,
        player_name: str,
        user_id: Optional[Any] = None,
    ) -> Optional[int]:
        """
        Adiciona o jogador à sala como Jogador 1, Jogador 2 ou Espectador.
        Retorna o número do jogador (1 ou 2) ou None para espectador.
        """
        room = self.get_room(room_code)
        if not room:
            try:
                await websocket.send_json({"type": "error", "message": "Sala não encontrada ou expirada."})
            except Exception:
                pass
            finally:
                try:
                    await websocket.close(code=4404)
                except Exception:
                    pass
            return None

        # Cancela tarefas de desconexão prévia se estiver reconectando
        player_num: Optional[int] = None

        # Atribuição de vaga com comparação segura de ID (int, UUID ou str)
        is_p1 = False
        if room.player1_ws is None:
            if user_id is not None and room.player1_id is not None:
                is_p1 = (str(room.player1_id) == str(user_id))
            else:
                is_p1 = (room.player1_name == player_name or room.player2_ws is not None)

        if is_p1:
            room.player1_ws = websocket
            room.player1_name = player_name
            room.player1_id = user_id or room.player1_id
            player_num = 1
        elif room.player2_ws is None and room.player1_ws != websocket:
            room.player2_ws = websocket
            room.player2_name = player_name
            room.player2_id = user_id or room.player2_id
            player_num = 2
        else:
            # Sala cheia -> entra como espectador
            room.spectators.add(websocket)

        if player_num and player_num in room.reconnect_tasks:
            room.reconnect_tasks[player_num].cancel()
            del room.reconnect_tasks[player_num]

        # Envia confirmação inicial com dados do jogador conectado
        await websocket.send_json({
            "type": "connected",
            "player_number": player_num,
            "role": "player" if player_num else "spectator",
            "state": room.to_state_dict(),
        })

        # Notifica explicitamente a atribuição de jogador
        await websocket.send_json({
            "type": "assigned",
            "player": player_num,
            "role": "player" if player_num else "spectator",
        })

        # Verifica início de partida
        if room.player1_ws and room.player2_ws and room.status == "waiting":
            room.status = "active"
            room.start_time = datetime.utcnow()
            await self._update_session_status(room.game_id, "active", room.player1_name, room.player2_name)
            await self.broadcast_to_room(
                room,
                {
                    "type": "game_start",
                    "message": f"Partida iniciada! Duelo entre {room.player1_name} (Azul) e {room.player2_name} (Vermelho).",
                    "state": room.to_state_dict(),
                },
            )
        else:
            await self.broadcast_to_room(
                room,
                {
                    "type": "player_joined",
                    "player_number": player_num,
                    "player_name": player_name,
                    "role": "player" if player_num else "spectator",
                    "state": room.to_state_dict(),
                },
                exclude_ws=websocket,
            )

        return player_num

    async def handle_disconnect(self, websocket: WebSocket, room_code: str) -> None:
        """Trata desconexão inesperada e inicia contagem regressiva de reconexão."""
        room = self.get_room(room_code)
        if not room:
            return

        player_num = room.get_player_number(websocket)
        if player_num == 1:
            room.player1_ws = None
        elif player_num == 2:
            room.player2_ws = None
        else:
            room.spectators.discard(websocket)
            return

        await self.broadcast_to_room(
            room,
            {
                "type": "player_disconnected",
                "player_number": player_num,
                "player_name": room.get_player_name(player_num),
                "reconnect_timeout": GAME_RECONNECT_TIMEOUT_SECONDS,
                "state": room.to_state_dict(),
            },
        )

        # Se a partida estiver ativa, inicia timer de abandono
        if room.status == "active":
            task = asyncio.create_task(self._handle_reconnect_timeout(room, player_num))
            room.reconnect_tasks[player_num] = task

    async def _handle_reconnect_timeout(self, room: GameRoom, disconnected_player: int) -> None:
        """Se o jogador não retornar dentro do tempo estipulado no .env, concede vitória por W.O."""
        try:
            await asyncio.sleep(GAME_RECONNECT_TIMEOUT_SECONDS)
            if room.status == "active":
                winner_num = 2 if disconnected_player == 1 else 1
                winner_name = room.get_player_name(winner_num)
                room.status = "finished"

                await self._finish_game_session(
                    room.game_id,
                    winner_num,
                    winning_line=None,
                    total_moves=room.total_moves,
                    board_state=room.board_state,
                )

                await self.broadcast_to_room(
                    room,
                    {
                        "type": "game_over",
                        "winner_player": winner_num,
                        "winner_name": winner_name,
                        "reason": f"Vitória por abandono de {room.get_player_name(disconnected_player)}.",
                        "state": room.to_state_dict(),
                    },
                )
        except asyncio.CancelledError:
            pass

    async def process_move(
        self, websocket: WebSocket, room_code: str, from_casa: int, to_casa: int
    ) -> None:
        """Valida e processa um movimento feito por um dos jogadores."""
        room = self.get_room(room_code)
        if not room or room.status != "active":
            await websocket.send_json({"type": "error", "message": "Partida não está ativa."})
            return

        player_num = room.get_player_number(websocket)
        if not player_num:
            await websocket.send_json({"type": "error", "message": "Apenas jogadores podem mover peças."})
            return

        if player_num != room.current_turn:
            await websocket.send_json({"type": "error", "message": "Não é o seu turno de jogar."})
            return

        # 1. Valida se a casa de origem pertence ao jogador
        if room.board_state.get(from_casa) != player_num:
            await websocket.send_json({"type": "error", "message": "A peça escolhida não lhe pertence."})
            return

        # 2. Valida se o destino é vizinho válido e está vago
        valid_destinations = MOVIMENTOS_VALIDOS.get(from_casa, [])
        if to_casa not in valid_destinations or room.board_state.get(to_casa) != 0:
            await websocket.send_json({"type": "error", "message": "Movimento ilegal ou casa ocupada."})
            return

        # 3. Executa a jogada
        board_before = dict(room.board_state)
        room.board_state[from_casa] = 0
        room.board_state[to_casa] = player_num
        room.total_moves += 1
        board_after = dict(room.board_state)

        # 4. Salva o movimento no banco de dados
        await self._persist_move(
            room.game_id,
            room.total_moves,
            player_num,
            from_casa,
            to_casa,
            board_before,
            board_after,
        )

        # 5. Verifica se venceu (Regra de Ouro: trinca passando por 5)
        has_won = check_winner(room.board_state, player_num)
        if has_won:
            room.status = "finished"
            winning_line = next(
                (list(line) for line in CONDICOES_DE_VITORIA if all(room.board_state.get(c) == player_num for c in line)),
                [from_casa, to_casa]
            )

            await self._finish_game_session(
                room.game_id,
                player_num,
                winning_line=str(winning_line),
                total_moves=room.total_moves,
                board_state=room.board_state,
            )

            await self.broadcast_to_room(
                room,
                {
                    "type": "game_over",
                    "winner_player": player_num,
                    "winner_name": room.get_player_name(player_num),
                    "winning_line": winning_line,
                    "reason": "Alinhamento vencedor cruzando a Casa Central (5)!",
                    "state": room.to_state_dict(),
                },
            )
            return

        # 6. Verifica Afogamento / Bloqueio Total (Empate)
        next_player = 2 if player_num == 1 else 1
        legal_next = get_legal_moves(room.board_state, next_player)
        if not legal_next:
            room.status = "finished"
            await self._finish_game_session(
                room.game_id,
                winner_player=0,
                winning_line=None,
                total_moves=room.total_moves,
                board_state=room.board_state,
            )

            await self.broadcast_to_room(
                room,
                {
                    "type": "game_over",
                    "winner_player": 0,
                    "winner_name": None,
                    "reason": f"Empate por bloqueio: {room.get_player_name(next_player)} não tem movimentos válidos.",
                    "state": room.to_state_dict(),
                },
            )
            return

        # 7. Alterna turno e envia atualização
        room.current_turn = next_player
        await self.broadcast_to_room(
            room,
            {
                "type": "move_made",
                "player": player_num,
                "from_casa": from_casa,
                "to_casa": to_casa,
                "state": room.to_state_dict(),
            },
        )

    async def process_chat(self, websocket: WebSocket, room_code: str, message_text: str) -> None:
        """Trata mensagens de chat enviadas em tempo real e persiste no BD."""
        room = self.get_room(room_code)
        if not room:
            return

        player_num = room.get_player_number(websocket) or 0
        sender_name = room.get_player_name(player_num) if player_num else "Espectador"
        clean_msg = message_text.strip()[:500]
        if not clean_msg:
            return

        timestamp = datetime.utcnow().strftime("%H:%M:%S")

        # Salva no banco de dados
        user_id = room.player1_id if player_num == 1 else (room.player2_id if player_num == 2 else None)
        await self._persist_chat(room.game_id, player_num, sender_name, clean_msg, user_id)

        # Transmite aos clientes
        await self.broadcast_to_room(
            room,
            {
                "type": "chat_message",
                "sender_player": player_num,
                "sender_name": sender_name,
                "message": clean_msg,
                "timestamp": timestamp,
            },
        )

    async def process_surrender(self, websocket: WebSocket, room_code: str) -> None:
        """Trata a desistência/rendição de um jogador."""
        room = self.get_room(room_code)
        if not room or room.status != "active":
            return

        surrendering_player = room.get_player_number(websocket)
        if not surrendering_player:
            return

        winner_player = 2 if surrendering_player == 1 else 1
        room.status = "finished"

        await self._finish_game_session(
            room.game_id,
            winner_player,
            winning_line=None,
            total_moves=room.total_moves,
            board_state=room.board_state,
        )

        await self.broadcast_to_room(
            room,
            {
                "type": "game_over",
                "winner_player": winner_player,
                "winner_name": room.get_player_name(winner_player),
                "reason": f"{room.get_player_name(surrendering_player)} rendeu-se.",
                "state": room.to_state_dict(),
            },
        )

    async def process_draw_proposal(self, websocket: WebSocket, room_code: str, action: str) -> None:
        """Trata proposta e aceitação de empate mútuo."""
        room = self.get_room(room_code)
        if not room or room.status != "active":
            return

        player_num = room.get_player_number(websocket)
        if not player_num:
            return

        if action == "propose":
            room.draw_offered_by = player_num
            await self.broadcast_to_room(
                room,
                {
                    "type": "draw_offered",
                    "proposing_player": player_num,
                    "proposing_name": room.get_player_name(player_num),
                },
            )
        elif action == "accept" and room.draw_offered_by and room.draw_offered_by != player_num:
            room.status = "finished"
            await self._finish_game_session(
                room.game_id,
                winner_player=0,
                winning_line=None,
                total_moves=room.total_moves,
                board_state=room.board_state,
            )

            await self.broadcast_to_room(
                room,
                {
                    "type": "game_over",
                    "winner_player": 0,
                    "winner_name": None,
                    "reason": "Empate acordado mutuamente entre os jogadores.",
                    "state": room.to_state_dict(),
                },
            )
        elif action == "reject":
            room.draw_offered_by = None
            await self.broadcast_to_room(
                room,
                {
                    "type": "draw_rejected",
                    "rejected_by": player_num,
                    "message": "A proposta de empate foi rejeitada.",
                },
            )

    async def process_rematch(self, websocket: WebSocket, room_code: str) -> None:
        """Reinicia a partida quando ambos os jogadores votam por revanche."""
        room = self.get_room(room_code)
        if not room or room.status != "finished":
            return

        player_num = room.get_player_number(websocket)
        if not player_num:
            return

        room.rematch_votes.add(player_num)

        if len(room.rematch_votes) >= 2:
            # Ambos concordaram -> Reseta o tabuleiro
            room.board_state = {
                1: 1, 2: 1, 3: 1,
                4: 0, 5: 0, 6: 0,
                7: 2, 8: 2, 9: 2,
            }
            room.current_turn = 1
            room.status = "active"
            room.total_moves = 0
            room.draw_offered_by = None
            room.rematch_votes.clear()
            room.start_time = datetime.utcnow()

            # Cria nova sessão no BD para a revanche
            new_game_id = uuid.uuid4()
            room.game_id = new_game_id
            await self._create_rematch_session(room)

            await self.broadcast_to_room(
                room,
                {
                    "type": "game_start",
                    "message": "Revanche aceita! Novo duelo iniciado.",
                    "state": room.to_state_dict(),
                },
            )
        else:
            await self.broadcast_to_room(
                room,
                {
                    "type": "rematch_requested",
                    "player": player_num,
                    "player_name": room.get_player_name(player_num),
                    "message": f"{room.get_player_name(player_num)} solicitou revanche. Aguardando adversário...",
                },
            )

    # ══════════════════════════════════════════════════════════
    # 💾 MÉTODOS DE PERSISTÊNCIA ASSÍNCRONA NO BANCO DE DADOS
    # ══════════════════════════════════════════════════════════

    async def _update_session_status(self, game_id: uuid.UUID, status: str, p1: str, p2: str) -> None:
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    game = await session.get(GameSession, game_id)
                    if game:
                        game.status = status
                        game.player1_name = p1
                        game.player2_name = p2
        except Exception as e:
            log_message(f"⚠️ Erro ao atualizar status da sessão {game_id}: {e}", "warning")

    async def _persist_move(
        self,
        game_id: uuid.UUID,
        step: int,
        player: int,
        from_casa: int,
        to_casa: int,
        board_before: Dict[int, int],
        board_after: Dict[int, int],
    ) -> None:
        try:
            intent = "Controle central" if to_casa == 5 else f"Avanço de {from_casa} para {to_casa}"
            centro = int(board_after.get(5, board_after.get("5", 0))) if board_after else 0
            ativas = sum(1 for v in board_after.values() if v in (1, 2)) if board_after else 0
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    move = GameMove(
                        game_id=game_id,
                        step=step,
                        player=player,
                        from_casa=from_casa,
                        to_casa=to_casa,
                        board_before=board_before,
                        board_after=board_after,
                        board_state=board_after,
                        casa_centro=centro,
                        rows=3,
                        cols=3,
                        casas_ativas=ativas,
                        tactical_intent=intent,
                    )
                    session.add(move)

                    game = await session.get(GameSession, game_id)
                    if game:
                        game.total_moves = step
                        game.board_state = board_after
                        game.current_turn = 2 if player == 1 else 1
        except Exception as e:
            log_message(f"⚠️ Erro ao persistir movimento no jogo {game_id}: {e}", "warning")

    async def _persist_chat(
        self,
        game_id: uuid.UUID,
        sender_player: int,
        sender_name: str,
        message: str,
        user_id: Optional[int],
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    chat = GameChat(
                        game_id=game_id,
                        sender_player=sender_player,
                        sender_name=sender_name,
                        user_id=user_id,
                        message=message,
                    )
                    session.add(chat)
        except Exception as e:
            log_message(f"⚠️ Erro ao persistir chat: {e}", "warning")

    async def _finish_game_session(
        self,
        game_id: uuid.UUID,
        winner_player: int,
        winning_line: Optional[str],
        total_moves: int,
        board_state: Dict[int, int],
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    game = await session.get(GameSession, game_id)
                    if game:
                        game.status = "finished"
                        game.winner_player = winner_player
                        game.winning_line = winning_line
                        game.total_moves = total_moves
                        game.board_state = board_state
                        game.finished_at = datetime.utcnow()
                        if game.created_at:
                            delta = (game.finished_at - game.created_at).total_seconds()
                            game.duration_seconds = max(int(delta), 1)

                        # Atualiza estatísticas do Jogador 1
                        await self._update_player_stats(session, game.player1_name, game.player1_id, is_win=(winner_player == 1), is_draw=(winner_player == 0), moves=total_moves)
                        # Atualiza estatísticas do Jogador 2
                        await self._update_player_stats(session, game.player2_name, game.player2_id, is_win=(winner_player == 2), is_draw=(winner_player == 0), moves=total_moves)
        except Exception as e:
            log_message(f"⚠️ Erro ao finalizar sessão {game_id}: {e}", "warning")

    async def _update_player_stats(
        self,
        session: Any,
        player_name: str,
        user_id: Optional[int],
        is_win: bool,
        is_draw: bool,
        moves: int,
    ) -> None:
        try:
            stmt = select(GamePlayerStats).where(GamePlayerStats.player_name == player_name)
            result = await session.execute(stmt)
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
                session.add(stats)

            stats.total_games = (stats.total_games or 0) + 1
            stats.total_moves = (stats.total_moves or 0) + moves

            if is_win:
                stats.wins_online = (stats.wins_online or 0) + 1
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

            # Invalida o cache de estatísticas no Redis para refletir o novo resultado
            from app.config.redis import delete_cache
            delete_cache(f"game:stats:{player_name.lower().strip()}")
            delete_cache("game:stats:__global__")
        except Exception as e:
            log_message(f"⚠️ Erro ao atualizar estatísticas de {player_name}: {e}", "warning")

    async def _create_rematch_session(self, room: GameRoom) -> None:
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    sala_id = None
                    if room.room_code:
                        from app.models.game_models import SalaGame
                        stmt_s = select(SalaGame.id).where(SalaGame.room_code == room.room_code.upper())
                        res_s = await session.execute(stmt_s)
                        sala_id = res_s.scalar_one_or_none()

                    new_game = GameSession(
                        id=room.game_id,
                        sala_id=sala_id,
                        room_code=room.room_code,
                        mode="online",
                        status="active",
                        player1_id=room.player1_id,
                        player1_name=room.player1_name,
                        player2_id=room.player2_id,
                        player2_name=room.player2_name,
                        current_turn=1,
                        board_state=room.board_state,
                    )
                    session.add(new_game)

        except Exception as e:
            log_message(f"⚠️ Erro ao criar revanche no BD: {e}", "warning")


game_ws_manager = GameWebSocketManager()
