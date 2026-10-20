"""
Controlador REST e WebSocket do Jogo Tapatan 3x3.
Oferece endpoints para gestão de partidas, gravação de jogadas, consulta de estatísticas,
geração de jogada pela IA e o canal WebSocket em tempo real para duelos online e chat.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import List, Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import GAME_STATS_CACHE_TTL_SECONDS, GAME_WS_PING_INTERVAL
from app.database import AsyncSessionLocal, get_db_async
from app.models.user_model import User
from app.schemas.game import (
    AIMoveRequest,
    AIMoveResponse,
    GameFinishRequest,
    GameGlobalStatsResponse,
    GameMoveCreate,
    GameMoveResponse,
    GameSessionCreate,
    GameSessionDetailResponse,
    GameSessionResponse,
    GameStatsResponse,
    MatrizResponse,
    SalaGameCreate,
    SalaGameResponse,
    WSChatMessage,
    WSDrawMessage,
    WSMoveMessage,
)
from app.services.game_service import (
    GameSessionNotFoundError,
    InvalidGameMoveError,
    game_service,
)
from app.services.game_websocket_manager import game_ws_manager
from app.ultils.get_id_by_token import ws_user_id
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user


# Router REST principal
router = APIRouter(
    prefix="/api/v1/game",
    tags=["Tapatan Game"],
)

# Router WebSocket dedicado
ws_router = APIRouter(
    tags=["Tapatan Game WebSocket"],
)


# ══════════════════════════════════════════════════════════
# 🌐 ENDPOINTS REST
# ══════════════════════════════════════════════════════════


@router.post(
    "/session",
    response_model=GameSessionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cria ou inicializa uma nova sessão de jogo",
)
async def create_session(
    data: GameSessionCreate,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Cria uma nova partida nos modos:
    - `local`: Duelo na mesma máquina (2 jogadores)
    - `online`: Cria uma sala multiplayer com `room_code` único
    - `ai`: Humano contra o motor de Inteligência Artificial
    """
    try:
        if not data.player1_name or data.player1_name == "Jogador 1":
            data.player1_name = current_user.nome or current_user.apelido or "Jogador 1"
        game_session = await game_service.create_session(db, data, user_id=current_user.id)
        return game_session
    except Exception as e:
        log_message(f"❌ Erro ao criar sessão de jogo: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível iniciar a partida.",
        )


@router.post(
    "/{game_id}/move",
    response_model=GameMoveResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Grava um movimento executado na partida",
)
async def record_move(
    game_id: uuid.UUID,
    move_data: GameMoveCreate,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Registra um movimento individual com os estados antes e depois do tabuleiro,
    enriquecendo o histórico e alimentando a base de aprendizado da IA.
    """
    try:
        move = await game_service.record_move(db, game_id, move_data)
        return move
    except GameSessionNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (InvalidGameMoveError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        log_message(f"❌ Erro ao gravar movimento: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível gravar o movimento.",
        )


@router.post(
    "/{game_id}/finish",
    response_model=GameSessionResponse,
    summary="Finaliza uma partida e atualiza as estatísticas globais",
)
async def finish_session(
    game_id: uuid.UUID,
    finish_data: GameFinishRequest,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Conclui a partida, grava o vencedor ou empate, calcula a duração
    e atualiza as vitórias/derrotas nas estatísticas dos jogadores.
    """
    try:
        game_session = await game_service.finish_session(db, game_id, finish_data)
        return game_session
    except GameSessionNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        log_message(f"❌ Erro ao finalizar partida: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível finalizar a partida.",
        )


@router.get(
    "/rooms/{room_code}",
    response_model=GameSessionResponse,
    summary="Consulta o status de uma sala online pelo código",
)
async def get_room_status(
    room_code: str,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Verifica se uma sala online existe e se ainda é válida para conexão."""
    game_session = await game_service.get_session_by_room(db, room_code)
    if not game_session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sala com código '{room_code}' não encontrada ou já expirada.",
        )
    return game_session


@router.post(
    "/salas",
    response_model=SalaGameResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cria uma nova sala de jogo na tabela sala_game",
)
async def create_sala(
    data: SalaGameCreate,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Cria uma sala dedicada no banco com metadados de privacidade, anfitrião e código amigável."""
    return await game_service.create_room(db, data, host_user_id=current_user.id)


@router.get(
    "/salas",
    response_model=List[SalaGameResponse],
    summary="Lista salas disponíveis na tabela sala_game",
)
async def list_salas(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Lista as salas de jogo com paginação e filtro por status."""
    return await game_service.list_rooms(db, status_filter=status_filter, limit=limit, offset=offset)


@router.get(
    "/salas/{room_code}",
    response_model=SalaGameResponse,
    summary="Obtém informações da sala de jogo pelo código",
)
async def get_sala_info(
    room_code: str,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Retorna detalhes da sala_game com base no código da sala."""
    sala = await game_service.get_room_by_code(db, room_code)
    if not sala:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sala '{room_code}' não encontrada na tabela sala_game.",
        )
    return sala


@router.get(
    "/matriz/{matriz_id}",
    response_model=MatrizResponse,
    summary="Consulta o estado de matriz do tabuleiro ou jogada herdada",
)
async def get_matriz_state(
    matriz_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Retorna a informação de estado de tabuleiro da tabela matriz (da qual game_moves herda)."""
    matriz = await game_service.get_matriz_by_id(db, matriz_id)
    if not matriz:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Matriz '{matriz_id}' não encontrada.",
        )
    return matriz


@router.get(
    "/history",
    response_model=List[GameSessionResponse],
    summary="Lista as partidas mais recentes",
)
async def list_recent_games(
    limit: int = Query(default=15, ge=1, le=50),
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Retorna o histórico recente de duelos concluídos ou ativos."""
    return await game_service.list_recent_sessions(db, limit)


@router.get(
    "/stats",
    response_model=GameGlobalStatsResponse,
    summary="Consulta as estatísticas gerais da plataforma (cacheado por 1 min)",
)
async def get_global_game_stats(
    response: Response,
    refresh: bool = Query(default=False, description="Forçar atualização ignorando o cache de 1 minuto"),
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Retorna métricas consolidadas da plataforma (partidas por modo, movimentos, jogadores e top ranking).
    Os dados são estritamente cacheados por 1 minuto (GAME_STATS_CACHE_TTL_SECONDS = 60s).
    """
    stats, is_hit = await game_service.get_global_stats(db, force_refresh=refresh)
    response.headers["Cache-Control"] = f"public, max-age={GAME_STATS_CACHE_TTL_SECONDS}"
    response.headers["X-Cache"] = "HIT" if is_hit else "MISS"
    response.headers["X-Cache-TTL"] = str(GAME_STATS_CACHE_TTL_SECONDS)
    return stats


@router.get(
    "/stats/{player_name}",
    response_model=GameStatsResponse,
    summary="Consulta as estatísticas detalhadas de um jogador (cacheado por 1 min)",
)
async def get_player_stats(
    player_name: str,
    response: Response,
    refresh: bool = Query(default=False, description="Forçar atualização ignorando o cache de 1 minuto"),
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Retorna vitórias locais, online, vs IA, derrotas, empates e win rate do jogador.
    Os resultados são cacheados por 1 minuto (GAME_STATS_CACHE_TTL_SECONDS = 60s).
    """
    stats, is_hit = await game_service.get_stats_with_status(db, player_name, force_refresh=refresh)
    response.headers["Cache-Control"] = f"public, max-age={GAME_STATS_CACHE_TTL_SECONDS}"
    response.headers["X-Cache"] = "HIT" if is_hit else "MISS"
    response.headers["X-Cache-TTL"] = str(GAME_STATS_CACHE_TTL_SECONDS)
    if not stats:
        # Retorna zerado se ainda não jogou
        return GameStatsResponse(
            player_name=player_name,
            total_games=0,
            wins_local=0,
            wins_online=0,
            wins_vs_ai=0,
            total_wins=0,
            losses=0,
            draws=0,
            win_rate=0.0,
            total_moves=0,
            center_captures=0,
            win_streak=0,
            best_win_streak=0,
        )
    return stats


@router.get(
    "/{game_id}",
    response_model=GameSessionDetailResponse,
    summary="Obtém os detalhes completos da partida incluindo jogadas e mensagens de chat",
)
async def get_game_details(
    game_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """Retorna a sessão completa com lista ordenada de jogadas e chat para replay."""
    game_session = await game_service.get_session_detail(db, game_id)
    if not game_session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Partida não encontrada.",
        )
    return game_session


@router.post(
    "/ai-move",
    response_model=AIMoveResponse,
    summary="Calcula a melhor jogada da IA com base no histórico do BD ou arquivo de conhecimento",
)
async def compute_ai_move(
    req: AIMoveRequest,
    db: AsyncSession = Depends(get_db_async),
    current_user: User = Depends(get_current_user),
):
    """
    Gera o movimento da IA utilizando:
    1. Histórico de vitórias no banco de dados
    2. Base de conhecimento persistida em arquivo (game_knowledge_base.json)
    3. Motor heurístico tático com defesa e conquista do centro (Casa 5)
    """
    try:
        move = await game_service.compute_ai_move(db, req)
        return move
    except Exception as e:
        log_message(f"❌ Erro ao calcular movimento da IA: {e}", "error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível calcular o movimento da IA.",
        )


# ══════════════════════════════════════════════════════════
# ⚡ WEBSOCKET (MULTIJOGADOR ONLINE & CHAT EM TEMPO REAL)
# ══════════════════════════════════════════════════════════


@ws_router.websocket("/ws/game/{room_code}")
@ws_router.websocket("/api/v1/ws/game/{room_code}")
async def game_websocket_endpoint(
    websocket: WebSocket,
    room_code: str,
    player_name: str = Query(default="Jogador", max_length=30),
    user_id: Optional[str] = Query(default=None),
):
    """
    Canal WebSocket bidirecional para partidas online do Tapatan 3x3.
    Mensagens suportadas pelo cliente:
    - `{"type": "move", "from_casa": 2, "to_casa": 5}`
    - `{"type": "chat", "message": "Boa jogada!"}`
    - `{"type": "surrender"}`
    - `{"type": "draw", "action": "propose" | "accept" | "reject"}`
    - `{"type": "rematch"}`
    - `{"type": "ping"}` -> responde `{"type": "pong"}`
    """
    await websocket.accept()

    # 1. Autenticação opcional do usuário pelo token JWT da sessão ou query param
    auth_user_id = ws_user_id(websocket)
    if not auth_user_id and user_id:
        try:
            auth_user_id = int(user_id)
        except (ValueError, TypeError):
            auth_user_id = user_id

    # 2. Obtenção atômica da sala (memória ou BD com proteção de lock)
    async with AsyncSessionLocal() as db_session:
        room = await game_ws_manager.get_or_restore_room(room_code, db_session)

    if not room:
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Sala '{room_code}' não encontrada ou já expirada.",
            })
        except Exception:
            pass
        finally:
            try:
                await websocket.close(code=4404)
            except Exception:
                pass
        return

    # 3. Conexão do jogador / espectador com tratamento seguro
    try:
        player_num = await game_ws_manager.handle_connect(
            websocket=websocket,
            room_code=room_code,
            player_name=player_name,
            user_id=auth_user_id,
        )
    except Exception as e:
        log_message(f"⚠️ Erro ao registrar conexão na sala {room_code}: {e}", "warning")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
        return

    # 4. Loop de mensagens com timeout (heartbeat) e validação Pydantic
    ws_timeout = max(GAME_WS_PING_INTERVAL * 4, 60)

    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=ws_timeout)
            except asyncio.TimeoutError:
                # Heartbeat de verificação de vida
                try:
                    await websocket.send_json({"type": "ping"})
                    data = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
                except Exception:
                    log_message(f"⏱️ Cliente inativo desconectado por timeout na sala {room_code}", "info")
                    break
            except json.JSONDecodeError:
                try:
                    await websocket.send_json({"type": "error", "message": "JSON inválido ou malformado."})
                except Exception:
                    break
                continue

            if not isinstance(data, dict):
                try:
                    await websocket.send_json({"type": "error", "message": "O payload deve ser um objeto JSON."})
                except Exception:
                    break
                continue

            msg_type = data.get("type")

            if msg_type == "move":
                try:
                    move_req = WSMoveMessage(**data)
                    await game_ws_manager.process_move(
                        websocket, room_code, move_req.from_casa, move_req.to_casa
                    )
                except ValidationError as ve:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Dados de movimento inválidos (casas de 1 a 9).",
                        "details": ve.errors(),
                    })

            elif msg_type == "chat":
                try:
                    chat_req = WSChatMessage(**data)
                    await game_ws_manager.process_chat(websocket, room_code, chat_req.message)
                except ValidationError as ve:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Mensagem de chat inválida.",
                        "details": ve.errors(),
                    })

            elif msg_type == "surrender":
                await game_ws_manager.process_surrender(websocket, room_code)

            elif msg_type == "draw":
                try:
                    draw_req = WSDrawMessage(**data)
                    await game_ws_manager.process_draw_proposal(websocket, room_code, draw_req.action)
                except ValidationError as ve:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Ação de empate inválida ('propose', 'accept', 'reject').",
                        "details": ve.errors(),
                    })

            elif msg_type == "rematch":
                await game_ws_manager.process_rematch(websocket, room_code)

            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})

            else:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Tipo de mensagem desconhecido: '{msg_type}'",
                })

    except WebSocketDisconnect:
        await game_ws_manager.handle_disconnect(websocket, room_code)
    except Exception as e:
        log_message(f"⚠️ Erro inesperado no WebSocket do jogo: {e}", "warning")
        await game_ws_manager.handle_disconnect(websocket, room_code)
