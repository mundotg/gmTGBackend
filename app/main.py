from fastapi import FastAPI
from app.config.dotenv import get_env
from app.config.startup_reset import already_initialized, mark_initialized, reset_database
from app.routes import auth_routes, connection_routes, dbInfo_routes, query_routes, user_routes,geral_routes
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API de Autenticação com FastAPI")

# CORS CONFIG
origins = [
    "http://localhost:3000",  # frontend local
    # você pode adicionar outros domínios depois, como:
    # "https://seusite.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # ou use ["*"] para liberar todos (não recomendado em produção)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth_routes.router)
app.include_router(user_routes.router)
app.include_router(geral_routes.router)
app.include_router(connection_routes.router)
app.include_router(dbInfo_routes.router)
app.include_router(query_routes.router)

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
