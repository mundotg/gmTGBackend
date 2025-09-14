from fastapi import FastAPI
from app.config.dotenv import get_env
from app.config.startup_reset import already_initialized, mark_initialized, reset_database
from app.routes import auth_routes, connection_routes, dbInfo_routes, dbstatistics_routes, query_routes, task_routes, user_routes, geral_routes
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API de Autenticação com FastAPI")

# CORS CONFIG
origins = [
    "http://localhost:3000",  
    "http://192.168.54.68:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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

# INICIALIZAÇÃO DO SERVIDOR
if __name__ == "__main__":
    import uvicorn
    host = get_env("HOST", "0.0.0.0")
    port = int(get_env("PORT", 8000))
    uvicorn.run("app.main:app", host=host, port=port, reload=True)
