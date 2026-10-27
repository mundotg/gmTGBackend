import asyncio
from typing import Dict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.ultils.logger import log_message
# Assumindo que você tem uma função para decodificar o token manualmente, 
# já que em WebSockets não enviamos header de Authorization facilmente pelo Go
from app.routes.connection_routes import decode_jwt_token 

router = APIRouter(
    tags=["Túnel de Banco de Dados Local"],
)

# Gerenciador global de túneis ativos na memória do servidor
# Mapeia: user_id -> WebSocket ativo
active_tunnels: Dict[int, WebSocket] = {}

# Função que copia dados bidirecionalmente entre SQLAlchemy (TCP) e Agente Go (WS)
async def handle_tcp_to_ws_bridge(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, user_id: int):
    ws = active_tunnels.get(user_id)
    if not ws:
        writer.close()
        return

    log_message(f"🔗 Nova conexão do EngineManager recebida. Trafegando para o Agente Go do user {user_id}", level="info")

    async def tcp_to_ws():
        """Lê os comandos SQL do EngineManager e envia para o PC do usuário"""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                await ws.send_bytes(data)
        except Exception as e:
            log_message(f"Aviso no TCP->WS: {e}", level="warning")
        finally:
            writer.close()

    async def ws_to_tcp():
        """Lê as respostas do banco no PC do usuário e devolve para o EngineManager"""
        try:
            while True:
                data = await ws.receive_bytes()
                writer.write(data)
                await writer.drain()
        except Exception as e:
            log_message(f"Aviso no WS->TCP: {e}", level="warning")
        finally:
            writer.close()

    # Executa ambas as cópias simultaneamente
    await asyncio.gather(tcp_to_ws(), ws_to_tcp())


@router.websocket("/ws/tunnel/agent")
async def database_tunnel_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="Token de autenticação gerado para o agente Go"),
):
    """
    🚀 Ponto de conexão do Agente Go (PC Normal).
    O Agente deve conectar usando: ws://sua-api.com/ws/tunnel/agent?token=JTW_AQUI
    """
    # 1. Autenticação (Como não há middleware HTTP padrão, validamos o token aqui)
    try:
        # Substitua pela sua lógica real de decodificação de token
        # user = decode_jwt_token(token)
        # user_id = user.id
        user_id = 1 # Simulando a extração do ID do usuário logado
    except Exception:
        await websocket.close(code=1008, reason="Token inválido")
        return

    await websocket.accept()
    active_tunnels[user_id] = websocket
    
    # 2. Definição da Porta Local do Proxy
    # Para evitar conflito entre usuários, cada usuário ganha uma porta TCP exclusiva
    # Ex: User 1 = 50001, User 2 = 50002
    local_proxy_port = 50000 + user_id
    
    log_message(f"🟢 Túnel estabelecido para o usuário {user_id}. Proxy TCP abrindo na porta {local_proxy_port}", level="info")

    try:
        # 3. Inicia o servidor TCP Falso no próprio servidor Cloud
        tcp_server = await asyncio.start_server(
            lambda r, w: handle_tcp_to_ws_bridge(r, w, user_id),
            '127.0.0.1',
            local_proxy_port
        )
        
        # Mantém o WebSocket e o Servidor TCP vivos até o cliente Go fechar o terminal
        async with tcp_server:
            await tcp_server.serve_forever()

    except WebSocketDisconnect:
        log_message(f"🔴 Agente Go desconectado (User ID: {user_id})", level="warning")
    except Exception as e:
        log_message(f"❌ Erro fatal no túnel WS: {e}", level="error")
    finally:
        # Limpeza de memória quando o usuário fecha o app no PC
        if user_id in active_tunnels:
            del active_tunnels[user_id]
        if 'tcp_server' in locals():
            tcp_server.close()
            await tcp_server.wait_closed()
            
            
# =========================================================
# 🚇 Túnel WebSocket para Bases de Dados Locais (PC do Usuário)
# =========================================================

# Gerenciador global de túneis na memória
# Mapeia: user_id -> WebSocket ativo
ACTIVE_TUNNELS: dict[int, WebSocket] = {}

async def _handle_tcp_to_ws_bridge(
    reader: asyncio.StreamReader, 
    writer: asyncio.StreamWriter, 
    websocket: WebSocket, 
    user_id: int
) -> None:
    """
    Ponte bidirecional pura: copia os bytes do SQLAlchemy (TCP) para o Agente Go (WS) e vice-versa.
    """
    log_message(f"🔗 Conexão do EngineManager interceptada. Roteando para o túnel do usuário {user_id}", level="info")

    async def tcp_to_ws():
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                await websocket.send_bytes(data)
        except Exception as e:
            log_message(f"⚠️ Aviso na via TCP -> WS (User {user_id}): {e}", level="warning")
        finally:
            writer.close()

    async def ws_to_tcp():
        try:
            while True:
                data = await websocket.receive_bytes()
                writer.write(data)
                await writer.drain()
        except Exception as e:
            log_message(f"⚠️ Aviso na via WS -> TCP (User {user_id}): {e}", level="warning")
        finally:
            writer.close()

    # Roda as duas direções simultaneamente
    await asyncio.gather(tcp_to_ws(), ws_to_tcp())


@router.websocket("/agent-tunnel")
async def connect_local_agent(
    websocket: WebSocket,
    token: str = Query(..., description="Token JWT do utilizador para autenticar o Agente Go"),
):
    """
    🚀 Ponto de conexão para o db-tunnel-agent (Golang).
    Cria uma porta local dinâmica no servidor Cloud que espelha a base de dados do PC do usuário.
    """
    # 1. Validação do Token (Como WS não usa headers Authorization por padrão)
    try:
        # Aqui você deve importar e usar sua função real de decodificação JWT
        # user_id = decode_jwt_token(token).get("sub") 
        user_id = 1  # <- SUBSTITUA PELA SUA LÓGICA DE EXTRAIR O ID DO TOKEN
    except Exception as e:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token inválido")
        return

    await websocket.accept()
    ACTIVE_TUNNELS[user_id] = websocket
    
    # 2. Define a porta TCP Exclusiva do utilizador no servidor Cloud
    # Ex: User ID 5 -> Porta 50005
    proxy_port = 50000 + int(user_id)
    tcp_server = None

    log_message(
        f"🟢 Agente do usuário {user_id} conectado. "
        f"Abrindo porta local {proxy_port} para receber conexões do EngineManager.", 
        level="info"
    )

    try:
        # 3. Inicia o servidor TCP Falso na Cloud para enganar o SQLAlchemy
        tcp_server = await asyncio.start_server(
            lambda r, w: _handle_tcp_to_ws_bridge(r, w, websocket, user_id),
            '127.0.0.1',
            proxy_port
        )
        
        # Mantém o loop do WebSocket e do servidor TCP vivos
        async with tcp_server:
            # Ouve mensagens de "ping" do agente Go para manter a conexão viva, se necessário
            while True:
                # Na arquitetura TCP Proxy, os dados crus vêm em bytes. 
                # Esta leitura mantém o túnel aberto caso o Go feche inesperadamente.
                await websocket.receive_bytes()

    except WebSocketDisconnect:
        log_message(f"🔴 Agente Go do usuário {user_id} desconectou.", level="info")
    except Exception as e:
        log_message(f"❌ Erro crítico no túnel do usuário {user_id}: {str(e)}", level="error")
    finally:
        # 4. Limpeza e encerramento seguro
        if user_id in ACTIVE_TUNNELS:
            del ACTIVE_TUNNELS[user_id]
        if tcp_server:
            tcp_server.close()
            await tcp_server.wait_closed()
            log_message(f"🔌 Porta local {proxy_port} libertada.", level="info")