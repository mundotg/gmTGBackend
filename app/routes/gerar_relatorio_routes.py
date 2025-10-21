import json
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session

from app.config.cache_manager import cache_result
from app.config.dotenv import get_env, get_env_int
from app.database import get_db
from app.relatorio.TaskReportBuilder import gerar_relatorio_tarefas
from app.relatorio.gerador_relatorio_excel import ExcelReportGenerator
from app.relatorio.gerar_estrutura_tabela import ReportStructureBuilder
from app.relatorio.gerar_resultado_consulta import QueryReportBuilder
from app.relatorio.gerarpdf import GenericReportGenerator
from app.schemas.dbstructure_schema import MetadataTableResponse
from app.schemas.query_select_upAndInsert_schema import (
    ParametrosRelatorioSchema,
    QueryResultType,
)
from app.ultils.get_current_user_id_task import get_current_user_id_task
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# =============================
# ⚙️ CONFIGURAÇÕES
# =============================

router = APIRouter(prefix="/relatorio", tags=["gerar relatorio"])

REPORT_TEMP_DIR = get_env("REPORT_TEMP_DIR", "temp_reports")
CACHE_TTL = get_env_int("CACHE_TTL", 900)  # 15 minutos
BASE_DIR = Path(__file__).resolve().parent.parent
LOGO_PATH = BASE_DIR / "relatorio" / get_env("LOGO_PATH", "fotor-ai-20250218134257.jpg")

FORMATO_PDF = "pdf"
FORMATO_EXCEL = "excel"
FORMATOS_VALIDOS = [FORMATO_PDF, FORMATO_EXCEL]

os.makedirs(REPORT_TEMP_DIR, exist_ok=True)


# =============================
# 🛠️ FUNÇÕES AUXILIARES
# =============================

def _get_media_type(formato: str) -> str:
    """Retorna o media type baseado no formato."""
    return {
        FORMATO_PDF: "application/pdf",
        FORMATO_EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(formato, "application/octet-stream")


def _criar_resposta_arquivo(file_path: str, tempo: float, formato: str) -> FileResponse:
    """Cria resposta com arquivo para download."""
    media_type = _get_media_type(formato)
    filename = os.path.basename(file_path)

    headers = {
        "X-Tempo-Geracao": f"{tempo:.2f}s",
        "X-Formato": formato,
        "Content-Disposition": f'attachment; filename="{filename}"',
    }

    log_message(
        f"📦 Preparando resposta do arquivo:\n"
        f" • Caminho: {file_path}\n"
        f" • Tipo MIME: {media_type}\n"
        f" • Nome do Arquivo: {filename}\n"
        f" • Cabeçalhos: {json.dumps(headers, indent=2, ensure_ascii=False)}"
    )
    
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
        headers=headers,
    )


def _gerar_nome_arquivo(tipo: str, user_id: int, extensao: str) -> str:
    """Gera nome de arquivo único para relatório."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"relatorio_{tipo}_{user_id}_{timestamp}.{extensao}"


# =============================
# 🧹 LIMPEZA DE ARQUIVOS
# =============================

def limpar_arquivos_temporarios(dias_retencao: int = 1):
    """Remove relatórios antigos do diretório temporário."""
    try:
        agora = datetime.now()
        extensoes = ["*.pdf", "*.xlsx"]
        total_removidos = 0

        for extensao in extensoes:
            for arquivo in Path(REPORT_TEMP_DIR).glob(extensao):
                if arquivo.is_file():
                    tempo_criacao = datetime.fromtimestamp(arquivo.stat().st_ctime)
                    if (agora - tempo_criacao).days > dias_retencao:
                        arquivo.unlink(missing_ok=True)
                        total_removidos += 1
                        log_message(f"🧹 Removido: {arquivo.name}")

        if total_removidos:
            log_message(f"🧹 Total de arquivos removidos: {total_removidos}")

    except Exception as e:
        log_message(f"⚠️ Erro na limpeza: {e}\n{traceback.format_exc()}", "warning")


def _executar_limpeza_periodica(user_id: int):
    """Executa limpeza periódica baseada no ID do usuário."""
    if hash(str(user_id)) % 10 == 0:
        limpar_arquivos_temporarios()


# =============================
# 🧩 RELATÓRIO DE ESTRUTURA
# =============================

@cache_result(ttl=CACHE_TTL, user_id="user_{user_id}")
def gerar_relatorio_com_cache(
    metadata_raw: List[MetadataTableResponse],
    user_id: int,
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    """Gera relatório de estrutura de banco de dados com cache por usuário."""
    log_message(f"📥 Processando relatório de estrutura (formato: {formato})...")

    _validar_dados_estrutura(metadata_raw, formato)

    try:
        return (
            _gerar_estrutura_pdf(metadata_raw, user_id)
            if formato == FORMATO_PDF
            else _gerar_estrutura_excel(metadata_raw, user_id)
        )
    except Exception as e:
        log_message(f"❌ Erro ao gerar relatório: {e}\n{traceback.format_exc()}", "error")
        raise RuntimeError(f"Falha na geração do relatório: {e}")


def _validar_dados_estrutura(metadata_raw: List[MetadataTableResponse], formato: str):
    """Valida dados para relatório de estrutura."""
    if not isinstance(metadata_raw, list):
        raise TypeError("O campo 'metadata' deve ser uma lista.")
    if not metadata_raw:
        raise ValueError("A lista de metadados não pode estar vazia.")
    if not all(isinstance(t, dict) for t in metadata_raw):
        raise TypeError("Todos os elementos de 'metadata' devem ser objetos JSON.")
    if formato not in FORMATOS_VALIDOS:
        raise ValueError(f"Formato inválido. Use: {', '.join(FORMATOS_VALIDOS)}")


def _gerar_estrutura_pdf(metadata_raw: List[MetadataTableResponse], user_id: int) -> str:
    """Gera relatório de estrutura em PDF."""
    builder = ReportStructureBuilder(metadata_raw)
    estrutura_pdf = builder.build()

    filename = _gerar_nome_arquivo("metadados", user_id, "pdf")
    file_path = os.path.join(REPORT_TEMP_DIR, filename)

    generator = GenericReportGenerator(logo_path=LOGO_PATH)
    generator.generate_report(file_path, estrutura_pdf)

    log_message(f"✅ PDF gerado: {filename}")
    return file_path


def _gerar_estrutura_excel(metadata_raw: List[MetadataTableResponse], user_id: int) -> str:
    """Gera relatório de estrutura em Excel."""
    filename = _gerar_nome_arquivo("metadados", user_id, "xlsx")
    file_path = os.path.join(REPORT_TEMP_DIR, filename)

    generator = ExcelReportGenerator(logo_path=LOGO_PATH)
    generator.generate_structure_report(file_path, metadata_raw)

    log_message(f"✅ Excel gerado: {filename}")
    return file_path


# =============================
# 🧩 RELATÓRIO DE RESULTADO DE QUERY
# =============================

@cache_result(ttl=CACHE_TTL, user_id="user_{user_id}")
def gerar_relatorio_query_com_cache(
    query_result: QueryResultType,
    user_id: int,
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    """Gera relatório de resultado de query SQL com cache por usuário."""
    log_message(f"📥 Processando relatório de query (formato: {formato})...")
    _validar_dados_query(query_result, formato)

    try:
        return (
            _gerar_query_pdf(query_result, user_id)
            if formato == FORMATO_PDF
            else _gerar_query_excel(query_result, user_id)
        )
    except Exception as e:
        log_message(f"❌ Erro ao gerar relatório: {e}\n{traceback.format_exc()}", "error")
        raise RuntimeError(f"Falha na geração do relatório: {e}")


def _validar_dados_query(query_result: QueryResultType, formato: str):
    """Valida dados para relatório de query."""
    if not isinstance(query_result, dict):
        raise TypeError("O campo 'query_result' deve ser um objeto JSON.")
    if formato not in FORMATOS_VALIDOS:
        raise ValueError(f"Formato inválido. Use: {', '.join(FORMATOS_VALIDOS)}")


def _gerar_query_pdf(query_result: QueryResultType, user_id: int) -> str:
    """Gera relatório de query em PDF."""
    builder = QueryReportBuilder(query_result)
    estrutura_pdf = builder.build()

    filename = _gerar_nome_arquivo("query", user_id, "pdf")
    file_path = os.path.join(REPORT_TEMP_DIR, filename)

    generator = GenericReportGenerator(logo_path=LOGO_PATH)
    generator.generate_report(file_path, estrutura_pdf)

    log_message(f"✅ PDF de query gerado: {filename}")
    return file_path


def _gerar_query_excel(query_result: QueryResultType, user_id: int) -> str:
    """Gera relatório de query em Excel."""
    filename = _gerar_nome_arquivo("query", user_id, "xlsx")
    file_path = os.path.join(REPORT_TEMP_DIR, filename)

    generator = ExcelReportGenerator(logo_path=LOGO_PATH)
    generator.generate_query_report(file_path, query_result)

    log_message(f"✅ Excel de query gerado: {filename}")
    return file_path


# =============================
# 📊 RELATÓRIO DE TAREFAS
# =============================

def gerar_relatorio_tarefas_com_cache(
    stats: dict,
    user_id: int,
    project: Optional[dict],
    sprint: Optional[dict],
    tasks: Optional[List[dict]],
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    """Gera relatório de tarefas (PDF ou Excel)."""
    log_message(f"📥 Processando relatório de tarefas (formato: {formato})...")
    _validar_dados_tarefas(stats, formato)

    try:
        return (
            _gerar_tarefas_pdf(stats, project, sprint, tasks, user_id)
            # if formato == FORMATO_PDF
            # else _gerar_tarefas_excel(stats, project, sprint, tasks, user_id)
        )
    except Exception as e:
        log_message(f"❌ Erro ao gerar relatório de tarefas: {e}\n{traceback.format_exc()}", "error")
        raise RuntimeError(f"Falha na geração do relatório de tarefas: {e}")


def _validar_dados_tarefas(stats: dict, formato: str):
    if not isinstance(stats, dict):
        raise TypeError("O campo 'stats' deve ser um objeto JSON.")
    if formato not in FORMATOS_VALIDOS:
        raise ValueError(f"Formato inválido. Use: {', '.join(FORMATOS_VALIDOS)}")


def _gerar_tarefas_pdf(stats, project, sprint, tasks, user_id) -> str:
    filename = _gerar_nome_arquivo("tarefas", user_id, "pdf")
    file_path = os.path.join(REPORT_TEMP_DIR, filename)

    gerar_relatorio_tarefas(stats, project, sprint, tasks, logo_path=str(LOGO_PATH))

    generated_file = f"relatorio_tarefas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    if os.path.exists(generated_file):
        os.rename(generated_file, file_path)

    log_message(f"✅ PDF de tarefas gerado: {filename}")
    return file_path



# =============================
# 📄 ENDPOINTS
# =============================

async def _processar_requisicao_relatorio(
    request: Request,
    user_id: int,
    db: Session,
    tipo_relatorio: str,
    gerar_relatorio_fn,
):
    """Processa requisição de relatório de forma genérica."""
    start = datetime.now()
    try:
        payload = await request.json()
        if not payload:
            raise ValueError("Corpo da requisição vazio")
        if "body" not in payload:
            raise ValueError("O campo 'body' é obrigatório")

        _executar_limpeza_periodica(user_id)

        parametros = ParametrosRelatorioSchema(**payload.get("parametros", {}))
        dados_relatorio = payload["body"]

        file_path = gerar_relatorio_fn(dados_relatorio, user_id, db, parametros.formato)
        tempo = (datetime.now() - start).total_seconds()

        log_message(f"✅ Relatório {tipo_relatorio} {parametros.formato.upper()} entregue em {tempo:.2f}s")
        return _criar_resposta_arquivo(file_path, tempo, parametros.formato)

    except (ValueError, TypeError) as e:
        tipo = "Validação" if isinstance(e, ValueError) else "Tipo de Dados"
        log_message(f"❌ Erro de {tipo}: {e}\n{traceback.format_exc()}", "error")
        return JSONResponse(
            status_code=400 if isinstance(e, ValueError) else 422,
            content={"erro": f"Erro de {tipo}", "mensagem": str(e)},
        )
    except Exception as e:
        log_message(f"💥 Erro inesperado: {e}\n{traceback.format_exc()}", "error")
        return JSONResponse(
            status_code=500,
            content={
                "erro": "Erro interno",
                "mensagem": str(e),
                "detalhes": traceback.format_exc().splitlines()[-3:],
            },
        )


@router.post("/gerar-relatorio")
async def gerar_relatorio(request: Request, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return await _processar_requisicao_relatorio(request, user_id, db, "estrutura", gerar_relatorio_com_cache)


@router.post("/gerar-relatorio-query")
async def gerar_relatorio_query(request: Request, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    """Gera relatório de resultado de query SQL."""
    return await _processar_requisicao_relatorio(request, user_id, db, "query", gerar_relatorio_query_com_cache)


@router.post("/gerar-relatorio-tarefas")
async def gerar_relatorio_tarefas_endpoint(
    request: Request,
    db: Session = Depends(get_db),
    # user_id: int = Depends(get_current_user_id),
    user_id: int = Depends(get_current_user_id_task),
):
    """Gera relatório completo de gestão de tarefas."""
    start = datetime.now()
    try:
        payload = await request.json()
        if not payload:
            raise ValueError("Corpo da requisição vazio")

        body = payload.get("body", {})
        stats = body.get("stats")
        if not stats:
            raise ValueError("O campo 'stats' é obrigatório")

        project = body.get("project")
        sprint = body.get("sprint")
        tasks = body.get("tasks")
        parametros = ParametrosRelatorioSchema(**payload.get("parametros", {}))

        _executar_limpeza_periodica(user_id)

        file_path = gerar_relatorio_tarefas_com_cache(
            stats=stats,
            user_id=user_id,
            project=project,
            sprint=sprint,
            tasks=tasks,
            db=db,
            formato=parametros.formato,
        )
# 
        tempo = (datetime.now() - start).total_seconds()
        log_message(f"✅ Relatório de tarefas {parametros.formato.upper()} entregue em {tempo:.2f}s")
        return _criar_resposta_arquivo(file_path, tempo, parametros.formato)

    except (ValueError, TypeError) as e:
        tipo = "Validação" if isinstance(e, ValueError) else "Tipo de Dados"
        log_message(f"❌ Erro de {tipo}: {e}\n{traceback.format_exc()}", "error")
        return JSONResponse(
            status_code=400 if isinstance(e, ValueError) else 422,
            content={"erro": f"Erro de {tipo}", "mensagem": str(e)},
        )
    except Exception as e:
        log_message(f"💥 Erro no endpoint de tarefas: {e}\n{traceback.format_exc()}", "error")
        return JSONResponse(
            status_code=500,
            content={
                "erro": "Falha ao gerar relatório de tarefas",
                "mensagem": str(e),
                "detalhes": traceback.format_exc().splitlines()[-3:],
            },
        )
