"""
Serviço de Inteligência Artificial para o Jogo Tapatan 3x3.
Implementa o motor de tomada de decisão com três camadas:
1. Aprendizado por Histórico de Partidas no Banco de Dados (PostgreSQL)
2. Consulta à Base de Conhecimento em Arquivo (game_knowledge_base.json)
3. Motor Heurístico Tático com Validação Estrita da Casa Central (5)
"""
from __future__ import annotations

import json
import os
import random
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import GAME_KNOWLEDGE_BASE_PATH
from app.models.game_models import GameMove, GameSession
from app.ultils.logger import log_message


# Grafo oficial de conectividade do Tapatan 3x3
MOVIMENTOS_VALIDOS: Dict[int, List[int]] = {
    1: [2, 4, 5],
    2: [1, 3, 5],
    3: [2, 5, 6],
    4: [1, 5, 7],
    5: [1, 2, 3, 4, 6, 7, 8, 9],  # Centro especial
    6: [3, 5, 9],
    7: [4, 5, 8],
    8: [5, 7, 9],
    9: [5, 6, 8],
}

# Trincas de vitória válidas (Passam obrigatoriamente pela Casa 5)
CONDICOES_DE_VITORIA: List[Tuple[int, int, int]] = [
    (4, 5, 6),  # Linha central horizontal
    (2, 5, 8),  # Coluna central vertical
    (1, 5, 9),  # Diagonal principal
    (3, 5, 7),  # Diagonal secundária
]


def normalize_board_to_dict(board_input: Any) -> Dict[int, int]:
    """
    Normaliza qualquer formato de entrada (dict, lista ou matriz 3x3)
    para um dicionário indexado de 1 a 9 com os valores 0 (vazio), 1 (P1), 2 (P2).
    """
    board: Dict[int, int] = {i: 0 for i in range(1, 10)}

    if isinstance(board_input, dict):
        for k, v in board_input.items():
            try:
                casa = int(k)
                if 1 <= casa <= 9:
                    board[casa] = int(v) if v is not None else 0
            except (ValueError, TypeError):
                continue
        return board

    if isinstance(board_input, list):
        # Matriz 3x3 de objetos ou números
        if len(board_input) == 3 and isinstance(board_input[0], list):
            casa = 1
            for row in board_input:
                for cell in row:
                    if isinstance(cell, dict):
                        player = cell.get("player", 0)
                        board[casa] = int(player) if player is not None else 0
                    else:
                        board[casa] = int(cell) if cell is not None else 0
                    casa += 1
            return board
        # Lista linear de 9 itens
        if len(board_input) >= 9:
            for idx in range(9):
                val = board_input[idx]
                if isinstance(val, dict):
                    board[idx + 1] = int(val.get("player", 0))
                else:
                    board[idx + 1] = int(val) if val is not None else 0
            return board

    return board


def board_to_hash(board: Dict[int, int]) -> str:
    """Gera uma string hash de 9 caracteres representando o tabuleiro (ex: '111000222')."""
    return "".join(str(board.get(i, 0)) for i in range(1, 10))


def check_winner(board: Dict[int, int], player: int) -> bool:
    """Verifica se o jogador formou uma linha vencedora passando pela Casa 5."""
    if board.get(5) != player:
        return False
    for p1, p2, p3 in CONDICOES_DE_VITORIA:
        if board.get(p1) == player and board.get(p2) == player and board.get(p3) == player:
            return True
    return False


def get_legal_moves(board: Dict[int, int], player: int) -> List[Tuple[int, int]]:
    """Retorna todas as jogadas legais (de_casa, para_casa) para o jogador."""
    moves: List[Tuple[int, int]] = []
    for casa, occupant in board.items():
        if occupant == player:
            neighbors = MOVIMENTOS_VALIDOS.get(casa, [])
            for dest in neighbors:
                if board.get(dest) == 0:  # Casa vazia
                    moves.append((casa, dest))
    return moves


class GameAIService:
    """
    Motor unificado da IA do Tapatan.
    Aprende com vitórias salvas no banco de dados e utiliza o arquivo de conhecimento como fallback.
    """

    def __init__(self, knowledge_file_path: Optional[str] = None):
        self.file_path = knowledge_file_path or GAME_KNOWLEDGE_BASE_PATH
        self._knowledge_cache: Optional[Dict[str, Any]] = None
        self._ensure_knowledge_file()

    def _ensure_knowledge_file(self) -> None:
        """Garante a existência física do arquivo de base de conhecimento."""
        path = Path(self.file_path)
        if not path.is_file():
            path.parent.mkdir(parents=True, exist_ok=True)
            initial_data = {
                "version": "1.0.0",
                "name": "Tapatan 3x3 Knowledge Base",
                "openings": [],
                "learned_transitions": {},
            }
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(initial_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                log_message(f"⚠️ Erro ao criar knowledge file: {e}", "warning")

    def _load_knowledge_file(self) -> Dict[str, Any]:
        """Carrega o conteúdo do arquivo de conhecimento."""
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"openings": [], "learned_transitions": {}}

    def _save_knowledge_file(self, data: Dict[str, Any]) -> None:
        """Persiste novos padrões aprendidos no arquivo de conhecimento."""
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log_message(f"⚠️ Erro ao salvar knowledge file: {e}", "warning")

    async def _query_database_learned_move(
        self, db: AsyncSession, board_hash: str, ai_player: int
    ) -> Optional[Tuple[int, int, str]]:
        """
        1ª Camada: Consulta o banco de dados por jogadas vencedoras
        históricas a partir exatamente do mesmo estado de tabuleiro.
        """
        try:
            query = (
                select(GameMove.from_casa, GameMove.to_casa)
                .join(GameSession, GameMove.game_id == GameSession.id)
                .where(
                    GameSession.status == "finished",
                    GameSession.winner_player == ai_player,
                    GameMove.player == ai_player,
                    sa.cast(GameMove.board_before, sa.String).contains(board_hash),
                )
                .limit(20)
            )
            result = await db.execute(query)
            rows = result.all()

            if rows:
                counter = Counter((r[0], r[1]) for r in rows)
                best_move, _ = counter.most_common(1)[0]
                return (
                    best_move[0],
                    best_move[1],
                    "Jogada aprendida com vitórias históricas registradas no banco de dados",
                )
        except Exception as e:
            log_message(f"⚠️ Consulta ao aprendizado de BD falhou: {e}", "debug")

        return None

    def _query_file_knowledge_move(
        self, board_hash: str, ai_player: int, legal_moves: List[Tuple[int, int]]
    ) -> Optional[Tuple[int, int, str]]:
        """
        2ª Camada: Consulta a base de conhecimento estática/persistida em JSON.
        """
        knowledge = self._load_knowledge_file()

        # Verifica aberturas
        for op in knowledge.get("openings", []):
            if op.get("board_hash") == board_hash and op.get("player") == ai_player:
                rec = op.get("recommended_move", {})
                f_casa, t_casa = rec.get("from"), rec.get("to")
                if (f_casa, t_casa) in legal_moves:
                    return (
                        f_casa,
                        t_casa,
                        rec.get("intent", "Movimento de abertura estratégico cadastrado"),
                    )

        # Verifica transições aprendidas
        transitions = knowledge.get("learned_transitions", {})
        if board_hash in transitions:
            candidates = transitions[board_hash]
            for cand in candidates:
                f_casa, t_casa = cand.get("from"), cand.get("to")
                if (f_casa, t_casa) in legal_moves:
                    return (
                        f_casa,
                        t_casa,
                        cand.get("intent", "Padrão posicional resgatado da base de conhecimento"),
                    )

        return None

    def _compute_heuristic_move(
        self,
        board: Dict[int, int],
        ai_player: int,
        difficulty: str,
        legal_moves: List[Tuple[int, int]],
    ) -> Tuple[int, int, str]:
        """
        3ª Camada: Motor Heurístico Heurístico Tático
        Avalia taticamente vitórias imediatas, bloqueios e controle do centro 5.
        """
        opponent = 1 if ai_player == 2 else 2

        # No modo fácil, permite escolhas aleatórias ocasionais
        if difficulty == "easy" and random.random() < 0.45:
            move = random.choice(legal_moves)
            return (move[0], move[1], "Movimentação tática exploratória (Nível Fácil)")

        # Prioridade 1: Vitória Imediata (Completa 3 peças passando por 5)
        for de, para in legal_moves:
            simulated = dict(board)
            simulated[de] = 0
            simulated[para] = ai_player
            if check_winner(simulated, ai_player):
                return (de, para, "Finalização de partida com alinhamento vencedor pelo Centro (5)")

        # Prioridade 2: Bloqueio Imediato do Adversário
        opponent_moves = get_legal_moves(board, opponent)
        critical_threat_houses = set()
        for op_de, op_para in opponent_moves:
            sim_op = dict(board)
            sim_op[op_de] = 0
            sim_op[op_para] = opponent
            if check_winner(sim_op, opponent):
                critical_threat_houses.add(op_para)

        if critical_threat_houses:
            for de, para in legal_moves:
                if para in critical_threat_houses:
                    return (de, para, "Bloqueio crítico de alinhamento vencedor adversário")

        # Prioridade 3: Conquistar o Centro (Casa 5) se estiver vago
        if board.get(5) == 0:
            for de, para in legal_moves:
                if para == 5:
                    return (de, para, "Controle tático da Casa Central (5)")

        # Prioridade 4: Manter ocupação da Casa 5 se já possuir
        # Evita tirar peça da casa 5 a menos que seja para vencer
        moves_not_leaving_center = [m for m in legal_moves if m[0] != 5]
        pool = moves_not_leaving_center if moves_not_leaving_center else legal_moves

        # Prioridade 5: Movimentação para casa adjacente que mira a Casa 5
        scored_moves: List[Tuple[Tuple[int, int], float, str]] = []
        for de, para in pool:
            score = 0.0
            intent = "Reposicionamento tático de flanco"

            # Bônus por vizinhança ao centro
            if 5 in MOVIMENTOS_VALIDOS.get(para, []):
                score += 2.0
                intent = "Aproximação com pressão sobre a Casa Central"

            # Bônus de mobilidade (quantos movimentos a peça terá no destino)
            dest_mobility = len([n for n in MOVIMENTOS_VALIDOS.get(para, []) if board.get(n) == 0])
            score += dest_mobility * 0.5

            scored_moves.append(((de, para), score, intent))

        scored_moves.sort(key=lambda x: x[1], reverse=True)
        best = scored_moves[0]
        return (best[0][0], best[0][1], best[2])

    async def get_best_move(
        self,
        db: AsyncSession,
        board_input: Any,
        ai_player: int = 2,
        difficulty: str = "medium",
    ) -> Dict[str, Any]:
        """
        Retorna a melhor jogada calculada unindo as três camadas.
        """
        board = normalize_board_to_dict(board_input)
        board_hash = board_to_hash(board)
        legal_moves = get_legal_moves(board, ai_player)

        if not legal_moves:
            raise ValueError("Não existem movimentos legais disponíveis para a IA no tabuleiro.")

        # 1. Tenta banco de dados
        db_move = await self._query_database_learned_move(db, board_hash, ai_player)
        if db_move and (db_move[0], db_move[1]) in legal_moves:
            return {
                "from_casa": db_move[0],
                "to_casa": db_move[1],
                "tactical_intent": db_move[2],
                "source": "database_learned",
            }

        # 2. Tenta arquivo de conhecimento
        file_move = self._query_file_knowledge_move(board_hash, ai_player, legal_moves)
        if file_move:
            return {
                "from_casa": file_move[0],
                "to_casa": file_move[1],
                "tactical_intent": file_move[2],
                "source": "knowledge_file",
            }

        # 3. Fallback Heurístico
        heuristic_move = self._compute_heuristic_move(board, ai_player, difficulty, legal_moves)

        # Salva o novo aprendizado no arquivo JSON para expandir a inteligência
        self._record_new_transition(board_hash, ai_player, heuristic_move[0], heuristic_move[1], heuristic_move[2])

        return {
            "from_casa": heuristic_move[0],
            "to_casa": heuristic_move[1],
            "tactical_intent": heuristic_move[2],
            "source": "heuristic",
        }

    def _record_new_transition(
        self, board_hash: str, player: int, from_casa: int, to_casa: int, intent: str
    ) -> None:
        """Adiciona e persiste a transição gerada no arquivo JSON de conhecimento."""
        try:
            knowledge = self._load_knowledge_file()
            transitions = knowledge.setdefault("learned_transitions", {})
            entries = transitions.setdefault(board_hash, [])

            if not any(e.get("from") == from_casa and e.get("to") == to_casa for e in entries):
                entries.append({
                    "player": player,
                    "from": from_casa,
                    "to": to_casa,
                    "intent": intent,
                })
                # Limita a no máximo 5 variantes por estado
                transitions[board_hash] = entries[:5]
                self._save_knowledge_file(knowledge)
        except Exception:
            pass


game_ai_service = GameAIService()
