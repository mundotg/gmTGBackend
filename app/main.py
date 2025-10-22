from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.cache_scheduler import schedule_cache_cleanup
from app.config.dotenv import get_env, get_env_list_cors
from app.config.startup_reset import init_on_startup
from app.database import SessionLocal
from app.routes import (
    auth_routes,
    user_routes,
    geral_routes,
    connection_routes,
    dbInfo_routes,
    query_routes,
    task_routes,
    dbstatistics_routes,
    delete_routes,
    connection_logs_routes,
    userTask_routes,
    sprint_routes,
    projects_routes,
    gerar_relatorio_routes,
)
from app.seed_admin import seed_data

# ------------------------------------------------------------

# Configuração principal do aplicativo

# ------------------------------------------------------------

app = FastAPI(title="API de Autenticação com FastAPI")

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

# Registro das rotas

# ------------------------------------------------------------

app.include_router(auth_routes.router)
app.include_router(user_routes.router)
app.include_router(geral_routes.router)
app.include_router(connection_routes.router)
app.include_router(dbInfo_routes.router)
app.include_router(query_routes.router)
app.include_router(task_routes.router)
app.include_router(dbstatistics_routes.router)
app.include_router(delete_routes.router)
app.include_router(connection_logs_routes.router)
app.include_router(userTask_routes.router)
app.include_router(sprint_routes.router)
app.include_router(projects_routes.router)
app.include_router(gerar_relatorio_routes.router)

# ------------------------------------------------------------

# Evento de inicialização da aplicação

# ------------------------------------------------------------


@app.on_event("startup")
def on_startup():
    print("🚀 Inicializando aplicação...")
    # Sincroniza models automaticamente (apenas em dev)
    init_on_startup()

    # Popula dados iniciais (como admin padrão)
    db = SessionLocal()
    seed_data(db)
    db.close()

    # Agenda limpeza automática de cache
    schedule_cache_cleanup()


# ------------------------------------------------------------

# Execução local (modo desenvolvimento)

# ------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = get_env("HOST", "0.0.0.0")
    port = int(get_env("PORT", 8000))
    print(f"📡 Servidor rodando em: http://{host}:{port}")
    uvicorn.run("app.main:app", host=host, port=port, reload=True)
