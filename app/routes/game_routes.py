"""
Módulo de rotas do jogo Tapatan 3x3 (compatibilidade com a estrutura app.routes do MustaInfo).
Expõe tanto as rotas REST quanto o endpoint de WebSocket em tempo real.
"""
from app.controllers.game_controller import router, ws_router

__all__ = ["router", "ws_router"]
