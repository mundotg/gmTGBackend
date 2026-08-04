import asyncio
import faulthandler
import os
import sys
from contextlib import asynccontextmanager  # 🔥 1. Importação nova

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware

from app.config.cache_scheduler import schedule_cache_cleanup
from app.config.dotenv import get_env, get_env_list_cors
from app.config.startup_reset import init_on_startup
from app.database import SessionLocal, check_database_health
from app.middleware import RequestContextMiddleware, register_exception_handlers
from app.routes import (
    ai_chat_routes,
    analytics_db_routes,
    auth_routes,
    backup_restore_routes,
    connection_logs_routes,
    connection_routes,
    database_intro_routes,
    database_operations_routes,
    dbInfo_routes,
    dbstatistics_routes,
    deadlock_monitory_route,
    delete_registro_routes,
    geral_routes,
    gerar_relatorio_routes,
    logs_routes,
    ocr_routes,
    projects_task_routes,
    query_routes,
    queryhistory_routes,
    sprint_task_routes,
    storage_routes,
    task_routes,
    transfer_data_routes,
    user_routes,
)
from app.schemas import project_analytics_routes
from app.seed_new import seed_data
from app.ultils.logger import log_message

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
ENV = (get_env("ENV", "development") or "development").lower()
IS_PRODUCTION = ENV == "production"

app = FastAPI(
    title="API de Autenticação com FastAPI",
    lifespan=lifespan,
    # Em produção não se publica o schema interno da API.
    docs_url=None if IS_PRODUCTION else "/docs",
    redoc_url=None if IS_PRODUCTION else "/redoc",
    openapi_url=None if IS_PRODUCTION else "/openapi.json",
)

# ------------------------------------------------------------
# Middlewares
# ------------------------------------------------------------
# Fica dentro do CORS (adicionado a seguir, portanto mais exterior), para que
# as respostas de erro e os preflight OPTIONS levem sempre os headers de CORS.
app.add_middleware(RequestContextMiddleware)

# ------------------------------------------------------------
# CORS
# ------------------------------------------------------------
origins = get_env_list_cors("BACKEND_CORS_ORIGINS", ["http://localhost:3000"])
allow_all = "*" in origins

# ⚠️ "allow_origins=*" com "allow_credentials=True" é rejeitado pelos browsers:
# a spec proíbe responder "Access-Control-Allow-Origin: *" quando as cookies
# viajam no pedido. Como a autenticação desta API é por cookie, o wildcard
# partiria o login em vez de o liberalizar.
if allow_all:
    if IS_PRODUCTION:
        raise RuntimeError(
            "BACKEND_CORS_ORIGINS='*' não é aceitável em produção: a API "
            "autentica por cookie. Define as origens explicitamente, "
            "ex.: BACKEND_CORS_ORIGINS=https://app.exemplo.com"
        )

    log_message(
        "⚠️ CORS com wildcard: credenciais desativadas (o login por cookie "
        "não funcionará). Define BACKEND_CORS_ORIGINS com as origens reais.",
        "warning",
        withBd=True,
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all else origins,
    # Só é possível ter um dos dois: wildcard OU cookies.
    allow_credentials=not allow_all,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)

# ------------------------------------------------------------
# Handlers globais de erro
# ------------------------------------------------------------
register_exception_handlers(app)

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
# Health checks
# ------------------------------------------------------------
# Separados de propósito: o orquestrador não deve reiniciar o contentor
# só porque a base de dados está em baixo — isso resolve-se tirando-o do
# balanceador (readiness), não matando o processo (liveness).


@app.get("/health/live", tags=["Health"])
async def liveness():
    """O processo está vivo e a responder. Usar como livenessProbe."""
    return {"status": "ok"}


@app.get("/health/ready", tags=["Health"])
async def readiness(response: Response):
    """
    O serviço consegue servir tráfego (BD alcançável). Usar como readinessProbe.
    Devolve 503 quando a base de dados não responde.
    """
    db_ok, detail = check_database_health()

    if not db_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ok" if db_ok else "degraded",
        "database": {"reachable": db_ok, "detail": detail},
        "env": ENV,
    }


@app.get("/health", tags=["Health"])
async def health(response: Response):
    """Atalho conveniente — equivalente a /health/ready."""
    return await readiness(response)


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
