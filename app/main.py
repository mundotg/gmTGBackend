from fastapi import FastAPI
from app.config.cache_scheduler import schedule_cache_cleanup
from app.config.dotenv import get_env, get_env_list
from app.config.startup_reset import (
    already_initialized,
    mark_initialized,
    reset_database,
)
from app.database import SessionLocal
from app.routes import (
    auth_routes,
    connection_logs_routes,
    connection_routes,
    dbInfo_routes,
    dbstatistics_routes,
    delete_routes,
    gerar_relatorio_routes,
    projects_routes,
    query_routes,
    sprint_routes,
    task_routes,
    user_routes,
    geral_routes,
)
from fastapi.middleware.cors import CORSMiddleware

from app.routes import userTask_routes
from app.seed_admin import seed_data

app = FastAPI(title="API de Autenticação com FastAPI")

# CORS CONFIG
origins = get_env_list("BACKEND_CORS_ORIGINS", ["http://localhost:3000"])
# origins = ["http://localhost:3000", "http://192.168.54.68:3000"]
allow_all = "*" in origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all else origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ROTAS
app.include_router(auth_routes.router)
app.include_router(user_routes.router)
app.include_router(geral_routes.router)
app.include_router(connection_routes.router)
app.include_router(dbInfo_routes.router)
app.include_router(query_routes.router)
app.include_router(task_routes.router)
app.include_router(dbstatistics_routes.router)
app.include_router(delete_routes.router)
app.include_router(connection_logs_routes.router)  # Adiciona as rotas de logs de conexão
app.include_router(userTask_routes.router)  # Adiciona as rotas de usuários e tarefas
app.include_router(sprint_routes.router)
app.include_router(projects_routes.router)
app.include_router(gerar_relatorio_routes.router)


# EVENTO DE STARTUP
@app.on_event("startup")
def on_startup():
    print("🚀 Inicializando aplicação...")
    
    
   
    if get_env("ENV", "dev") == "dev":
        if not already_initialized():
            reset_database()
            mark_initialized()
            print("✅ Banco de dados resetado com sucesso!")
        else:
            print("🔒 Banco de dados já inicializado anteriormente.")
    db = SessionLocal()
    seed_data(db)
    db.close()
    schedule_cache_cleanup()


# INICIALIZAÇÃO DO SERVIDOR
if __name__ == "__main__":
    import uvicorn

    host = get_env("HOST", "0.0.0.0")
    port = int(get_env("PORT", 8000))
    uvicorn.run("app.main:app", host=host, port=port, reload=True)
