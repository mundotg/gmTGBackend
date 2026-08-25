import asyncio
import faulthandler
import os
import sys
from contextlib import asynccontextmanager  # 🔥 1. Importação nova
from pathlib import Path

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

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
    datascience_routes,
    database_operations_routes,
    dbInfo_routes,
    dbstatistics_routes,
    deadlock_monitory_route,
    delete_registro_routes,
    empresa_routes,
    geral_routes,
    gerar_relatorio_routes,
    logs_routes,
    ocr_routes,
    pentest_routes,
    projects_task_routes,
    query_routes,
    queryhistory_routes,
    sprint_task_routes,
    sql_editor_routes,
    storage_routes,
    task_routes,
    transfer_data_routes,
    user_routes,
)
from app.schemas import project_analytics_routes
from app.seed_new import seed_data
from app.ultils.logger import log_message
from app.version import get_version_info

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
    info = get_version_info()

    # Identifica o código carregado. Como o servidor corre sem --reload
    # (start.bat usa --workers 4), um processo antigo continua a servir
    # código antigo e os tracebacks apontam para linhas que já mudaram.
    print(
        f" Inicializando aplicação... commit={info['commit']} "
        f"python={info['python']}"
    )

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

# Por omissão a documentação segue o ambiente: ligada fora de produção,
# desligada em produção (não se publica o schema interno da API).
#
# ENABLE_DOCS força o valor nos dois sentidos. Existe porque as duas decisões
# são independentes e estavam presas uma à outra: uma máquina de
# desenvolvimento a correr com ENV=production ficava sem Swagger, e a única
# forma de o recuperar era mudar o ENV — que também governa a rejeição do
# wildcard de CORS. Agora liga-se a documentação sem mexer no resto.
_enable_docs = get_env("ENABLE_DOCS")
DOCS_ATIVOS = (
    _enable_docs.strip().lower() in {"1", "true", "yes", "on"}
    if _enable_docs
    else not IS_PRODUCTION
)
OPENAPI_URL = "/openapi.json" if DOCS_ATIVOS else None

# Ordena os grupos no Swagger e dá contexto a cada um. Tags não listadas aqui
# aparecem no fim, por ordem de registo.
OPENAPI_TAGS = [
    {"name": "Auth", "description": "Login, refresh, sessão e logout. **Começa por aqui.**"},
    {"name": "Users", "description": "Perfil do utilizador autenticado."},
    {"name": "connections", "description": "CRUD e teste das ligações a bases de dados externas."},
    {"name": "connections_log", "description": "Histórico de ligações."},
    {"name": "Consulta de Banco de Dados", "description": "Metadados: tabelas, colunas, contagens e cache de schema."},
    {"name": "executeQuery", "description": "Execução de queries — síncrona, paginada e via SSE."},
    {"name": "Database Schema (DDL)", "description": "Criação e alteração de tabelas e colunas."},
    {"name": "Database Operations", "description": "Operações sobre a base ligada e canais de progresso."},
    {"name": "Data Deletion", "description": "Remoção de registos, individual e em lote."},
    {"name": "Backup & Restore (SSE)", "description": "Backup e restauro com progresso em streaming."},
    {"name": "Geral", "description": "Paginação universal, definições e estatísticas."},
    {"name": "Projects", "description": "CRUD de projetos."},
    {"name": "Tasks", "description": "Tarefas, delegação e validação."},
    {"name": "Sprints", "description": "CRUD de sprints e alternância de estado."},
    {"name": "ProjectAnalytics", "description": "Métricas agregadas de projetos."},
    {"name": "DatabaseAnalytics", "description": "Métricas agregadas da base de dados."},
    {"name": "Chat", "description": "Sessões e mensagens do assistente de IA."},
    {"name": "ocr", "description": "Extração de texto de imagens, PDFs e folhas de cálculo."},
    {"name": "Storage", "description": "Upload, listagem e download de ficheiros."},
    {"name": "gerar relatorio", "description": "Geração de relatórios em PDF e Excel."},
    {"name": "AuditLog", "description": "Logs de auditoria da aplicação."},
    {"name": "Health", "description": "Sondas de liveness e readiness."},
]

DESCRICAO = """
API do OrionForgeNexus.

### Autenticação

A API autentica por **cookie**. Para usar o "Try it out" desta página:

1. Abre `POST /auth/login` e envia as credenciais.
2. O cookie `access_token` fica guardado no browser — o Swagger é servido pela
   própria API, por isso o cookie viaja sozinho nos pedidos seguintes.
3. Não é preciso preencher nada no botão **Authorize**, a não ser que prefiras
   `Authorization: Bearer <token>` (esquema `bearerAuth`).
"""

app = FastAPI(
    title="OrionForgeNexus API",
    description=DESCRICAO,
    version=get_version_info()["commit"],
    openapi_tags=OPENAPI_TAGS,
    lifespan=lifespan,
    # Em produção não se publica o schema interno da API.
    openapi_url=OPENAPI_URL,
    # As páginas embutidas carregam JS/CSS de um CDN e ficam em branco sem
    # internet. Registamos versões próprias mais abaixo, servidas localmente.
    docs_url=None,
    redoc_url=None,
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

# ⚠️ A spec proíbe responder "Access-Control-Allow-Origin: *" quando as cookies
# viajam no pedido, e esta API autentica por cookie. Com BACKEND_CORS_ORIGINS='*'
# usa-se por isso `allow_origin_regex`: o middleware devolve a origem EXATA do
# pedido em vez do literal `*`, o que mantém o login a funcionar.
#
# O custo é real: qualquer site passa a poder fazer pedidos autenticados com as
# cookies do utilizador. Em produção, preferir a lista explícita de origens.
if allow_all:
    log_message(
        "⚠️ CORS aberto a qualquer origem (BACKEND_CORS_ORIGINS='*'): a origem "
        "do pedido é refletida para as cookies continuarem a funcionar. "
        "Define as origens reais para fechar esta porta.",
        "warning",
        withBd=True,
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=[] if allow_all else origins,
    allow_origin_regex=".*" if allow_all else None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)

# ------------------------------------------------------------
# Documentação (Swagger / ReDoc)
# ------------------------------------------------------------
# O `/docs` embutido do FastAPI puxa swagger-ui-bundle.js e swagger-ui.css do
# cdn.jsdelivr.net. Sem internet — contentor fechado, rede da empresa, .exe do
# PyInstaller numa máquina isolada — a página carrega vazia e parece que o
# Swagger "não funciona". Servimos os ficheiros a partir do disco e só caímos
# no CDN se não estiverem lá.
_STATIC_DIR = Path(__file__).resolve().parent / "static"
_SWAGGER_DIR = _STATIC_DIR / "swagger"

_CDN = "https://cdn.jsdelivr.net/npm"
_CDN_ASSETS = {
    "swagger-ui-bundle.js": f"{_CDN}/swagger-ui-dist@5/swagger-ui-bundle.js",
    "swagger-ui.css": f"{_CDN}/swagger-ui-dist@5/swagger-ui.css",
    "redoc.standalone.js": f"{_CDN}/redoc@2/bundles/redoc.standalone.js",
}


def _asset(nome: str) -> str:
    """URL local do ficheiro se existir em disco; senão, o CDN."""
    if (_SWAGGER_DIR / nome).is_file():
        return f"/static/swagger/{nome}"
    return _CDN_ASSETS[nome]


if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

if DOCS_ATIVOS:

    @app.get("/docs", include_in_schema=False)
    async def swagger_ui() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=OPENAPI_URL,
            title=f"{app.title} — Swagger",
            swagger_js_url=_asset("swagger-ui-bundle.js"),
            swagger_css_url=_asset("swagger-ui.css"),
            swagger_favicon_url="/static/swagger/favicon.png"
            if (_SWAGGER_DIR / "favicon.png").is_file()
            else "https://fastapi.tiangolo.com/img/favicon.png",
            swagger_ui_parameters={
                # Sem isto, o token do botão Authorize perde-se a cada F5.
                "persistAuthorization": True,
                # 117 endpoints: abrir tudo expandido torna a página ilegível.
                "docExpansion": "none",
                "filter": True,
                "tryItOutEnabled": True,
                "displayRequestDuration": True,
            },
        )

    @app.get("/redoc", include_in_schema=False)
    async def redoc_ui() -> HTMLResponse:
        return get_redoc_html(
            openapi_url=OPENAPI_URL,
            title=f"{app.title} — ReDoc",
            redoc_js_url=_asset("redoc.standalone.js"),
            with_google_fonts=False,
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
app.include_router(empresa_routes.router)
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
app.include_router(pentest_routes.router)
app.include_router(sql_editor_routes.router)
app.include_router(datascience_routes.router)


# ------------------------------------------------------------
# Health checks
# ------------------------------------------------------------
# Separados de propósito: o orquestrador não deve reiniciar o contentor
# só porque a base de dados está em baixo — isso resolve-se tirando-o do
# balanceador (readiness), não matando o processo (liveness).


@app.get("/health/live", tags=["Health"])
async def liveness():
    """O processo está vivo e a responder. Usar como livenessProbe."""
    return {"status": "ok", **get_version_info()}


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
        **get_version_info(),
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
