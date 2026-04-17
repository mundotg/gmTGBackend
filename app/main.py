import asyncio
import io
import sys
import os
import faulthandler
from contextlib import asynccontextmanager  # 🔥 1. Importação nova

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.cache_scheduler import schedule_cache_cleanup
from app.config.dotenv import get_env, get_env_list_cors
from app.config.startup_reset import init_on_startup
from app.database import SessionLocal
from app.schemas import project_analytics_routes
from app.seed_new import seed_data

from app.routes import (
    ai_chat_routes,
    analytics_db_routes,
    auth_routes,
    backup_restore_routes,
    database_operations_routes,
    delete_registro_routes,
    logs_routes,
    ocr_routes,
    projects_task_routes,
    sprint_task_routes,
    storage_routes,
    transfer_data_routes,
    user_routes,
    geral_routes,
    connection_routes,
    dbInfo_routes,
    query_routes,
    task_routes,
    dbstatistics_routes,
    connection_logs_routes,
    gerar_relatorio_routes,
    database_intro_routes,
    deadlock_monitory_route,
    queryhistory_routes,
)

# ------------------------------------------------------------
# 🔧 Correção de PATH (importante pro PyInstaller)
# ------------------------------------------------------------
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# ------------------------------------------------------------
# Debug e compatibilidade
# ------------------------------------------------------------
faulthandler.enable()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

os.environ["DISABLE_MODEL_SOURCE_CHECK"] = "True"


# ------------------------------------------------------------
# Lifespan (Substitui o @app.on_event("startup" / "shutdown"))
# ------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 🔥 2. Tudo que vem ANTES do yield é o "Startup"
    print(" Inicializando aplicação...")
    init_on_startup()

    db = SessionLocal()
    seed_data(db)
    db.close()

    schedule_cache_cleanup()

    # O FastAPI roda a aplicação aqui
    yield

    # 🔥 Se você precisar fechar conexões ou limpar recursos no futuro,
    # coloque o código de "Shutdown" aqui (após o yield).


# ------------------------------------------------------------
# App principal
# ------------------------------------------------------------
# 🔥 3. O lifespan é passado diretamente na inicialização do app
app = FastAPI(title="API de Autenticação com FastAPI", lifespan=lifespan)

# ------------------------------------------------------------
# CORS
# ------------------------------------------------------------
origins = get_env_list_cors("BACKEND_CORS_ORIGINS", ["http://localhost:3000"])
allow_all = "*" in origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all else origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# Rotas
# ------------------------------------------------------------
app.include_router(auth_routes.router)
app.include_router(user_routes.router)
app.include_router(geral_routes.router)
app.include_router(connection_routes.router)
app.include_router(dbInfo_routes.router)
app.include_router(query_routes.router)
app.include_router(task_routes.router)
app.include_router(dbstatistics_routes.router)
app.include_router(delete_registro_routes.router)
app.include_router(connection_logs_routes.router)
app.include_router(sprint_task_routes.router)
app.include_router(projects_task_routes.router)
app.include_router(gerar_relatorio_routes.router)
app.include_router(database_intro_routes.router)
app.include_router(deadlock_monitory_route.router)
app.include_router(ocr_routes.router)
app.include_router(transfer_data_routes.router)
app.include_router(database_operations_routes.router)
app.include_router(backup_restore_routes.router)
app.include_router(queryhistory_routes.router)
app.include_router(analytics_db_routes.router)
app.include_router(project_analytics_routes.router)
app.include_router(ai_chat_routes.router)
app.include_router(logs_routes.router)
app.include_router(storage_routes.router)

# ------------------------------------------------------------
# Execução (IMPORTANTE pro .exe)
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    host = get_env("HOST", "0.0.0.0") or "127.0.0.1"
    port = int(get_env("PORT") or "8000")

    print(f" Servidor rodando em: http://{host}:{port}")

    # ✅ NÃO usar string nem reload
    try:
        uvicorn.run(app, host=host, port=port)
    except KeyboardInterrupt:
        print("Sinal de paragem recebido. A encerrar o servidor graciosamente...")
