from __future__ import annotations

import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, List, Optional

from fastapi import APIRouter, Depends, Request
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
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message


router = APIRouter(prefix="/relatorio", tags=["gerar relatorio"])

REPORT_TEMP_DIR = Path(get_env("REPORT_TEMP_DIR", "temp_reports"))
CACHE_TTL = get_env_int("CACHE_TTL", 900)
BASE_DIR = Path(__file__).resolve().parent.parent
LOGO_PATH = BASE_DIR / "relatorio" / get_env("LOGO_PATH", "fotor-ai-20250218134257.jpg")

FORMATO_PDF = "pdf"
FORMATO_EXCEL = "excel"
FORMATOS_VALIDOS = {FORMATO_PDF, FORMATO_EXCEL}

REPORT_TEMP_DIR.mkdir(parents=True, exist_ok=True)


# =============================
# Helpers genéricos
# =============================

def _get_media_type(formato: str) -> str:
    return {
        FORMATO_PDF: "application/pdf",
        FORMATO_EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(formato, "application/octet-stream")


def _criar_resposta_arquivo(
    file_path: Path,
    tempo: float,
    formato: str,
) -> FileResponse:
    media_type = _get_media_type(formato)
    filename = file_path.name

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
        path=str(file_path),
        media_type=media_type,
        filename=filename,
        headers=headers,
    )


def _gerar_nome_arquivo(tipo: str, user_id: int, extensao: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"relatorio_{tipo}_{user_id}_{timestamp}.{extensao}"


def _validar_formato(formato: str) -> None:
    if formato not in FORMATOS_VALIDOS:
        raise ValueError(f"Formato inválido. Use: {', '.join(sorted(FORMATOS_VALIDOS))}")


def _executar_limpeza_periodica(user_id: int) -> None:
    """
    Executa limpeza ocasional.
    Usa uma regra simples e previsível.
    """
    if user_id % 10 == 0:
        limpar_arquivos_temporarios()


def limpar_arquivos_temporarios(dias_retencao: int = 1) -> None:
    try:
        agora = datetime.now()
        limite = agora - timedelta(days=dias_retencao)
        total_removidos = 0

        for pattern in ("*.pdf", "*.xlsx"):
            for arquivo in REPORT_TEMP_DIR.glob(pattern):
                if not arquivo.is_file():
                    continue

                criado_em = datetime.fromtimestamp(arquivo.stat().st_ctime)
                if criado_em < limite:
                    arquivo.unlink(missing_ok=True)
                    total_removidos += 1
                    log_message(f"🧹 Removido: {arquivo.name}")

        if total_removidos:
            log_message(f"🧹 Total de arquivos removidos: {total_removidos}")

    except Exception as exc:
        log_message(
            f"⚠️ Erro na limpeza: {exc}\n{traceback.format_exc()}",
            "warning",
        )


# =============================
# Relatório de estrutura
# =============================

@cache_result(ttl=CACHE_TTL, user_id="user_relatorio_{user_id}")
def gerar_relatorio_estrutura_com_cache(
    metadata_raw: List[dict[str, Any]],
    user_id: int,
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    log_message(f"📥 Processando relatório de estrutura (formato: {formato})...")

    _validar_dados_estrutura(metadata_raw, formato)

    try:
        file_path = (
            _gerar_estrutura_pdf(metadata_raw, user_id)
            if formato == FORMATO_PDF
            else _gerar_estrutura_excel(metadata_raw, user_id)
        )
        return str(file_path)
    except Exception as exc:
        log_message(
            f"❌ Erro ao gerar relatório de estrutura: {exc}\n{traceback.format_exc()}",
            "error",
        )
        raise RuntimeError(f"Falha na geração do relatório de estrutura: {exc}")


def _validar_dados_estrutura(
    metadata_raw: List[dict[str, Any]],
    formato: str,
) -> None:
    if not isinstance(metadata_raw, list):
        raise TypeError("O campo 'body' deve ser uma lista.")
    if not metadata_raw:
        raise ValueError("A lista de metadados não pode estar vazia.")
    if not all(isinstance(item, dict) for item in metadata_raw):
        raise TypeError("Todos os elementos da lista devem ser objetos JSON.")
    _validar_formato(formato)


def _gerar_estrutura_pdf(
    metadata_raw: List[dict[str, Any]],
    user_id: int,
) -> Path:
    builder = ReportStructureBuilder(metadata_raw)
    estrutura_pdf = builder.build()

    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("metadados", user_id, "pdf")

    generator = GenericReportGenerator(logo_path=LOGO_PATH)
    generator.generate_report(str(file_path), estrutura_pdf)

    log_message(f"✅ PDF de estrutura gerado: {file_path.name}")
    return file_path


def _gerar_estrutura_excel(
    metadata_raw: List[dict[str, Any]],
    user_id: int,
) -> Path:
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("metadados", user_id, "xlsx")

    generator = ExcelReportGenerator(logo_path=LOGO_PATH)
    generator.generate_structure_report(str(file_path), metadata_raw)

    log_message(f"✅ Excel de estrutura gerado: {file_path.name}")
    return file_path


# =============================
# Relatório de query
# =============================

@cache_result(ttl=CACHE_TTL, user_id="user_relatorio_query_{user_id}")
def gerar_relatorio_query_com_cache(
    query_result: QueryResultType,
    user_id: int,
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    log_message(f"📥 Processando relatório de query (formato: {formato})...")

    _validar_dados_query(query_result, formato)

    try:
        file_path = (
            _gerar_query_pdf(query_result, user_id)
            if formato == FORMATO_PDF
            else _gerar_query_excel(query_result, user_id)
        )
        return str(file_path)
    except Exception as exc:
        log_message(
            f"❌ Erro ao gerar relatório de query: {exc}\n{traceback.format_exc()}",
            "error",
        )
        raise RuntimeError(f"Falha na geração do relatório de query: {exc}")


def _validar_dados_query(query_result: QueryResultType, formato: str) -> None:
    if not isinstance(query_result, dict):
        raise TypeError("O campo 'body' deve ser um objeto JSON.")
    _validar_formato(formato)


def _gerar_query_pdf(query_result: QueryResultType, user_id: int) -> Path:
    builder = QueryReportBuilder(query_result)
    estrutura_pdf = builder.build()

    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("query", user_id, "pdf")

    generator = GenericReportGenerator(logo_path=LOGO_PATH)
    generator.generate_report(str(file_path), estrutura_pdf)

    log_message(f"✅ PDF de query gerado: {file_path.name}")
    return file_path


def _gerar_query_excel(query_result: QueryResultType, user_id: int) -> Path:
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("query", user_id, "xlsx")

    generator = ExcelReportGenerator(logo_path=LOGO_PATH)
    generator.generate_query_report(str(file_path), query_result)

    log_message(f"✅ Excel de query gerado: {file_path.name}")
    return file_path


# =============================
# Relatório de tarefas
# =============================

def gerar_relatorio_tarefas_com_cache(
    stats: dict[str, Any],
    user_id: int,
    project: Optional[dict[str, Any]],
    sprint: Optional[dict[str, Any]],
    tasks: Optional[List[dict[str, Any]]],
    db: Session,
    formato: str = FORMATO_PDF,
) -> str:
    log_message(f"📥 Processando relatório de tarefas (formato: {formato})...")

    _validar_dados_tarefas(stats, formato)

    try:
        if formato != FORMATO_PDF:
            raise ValueError("Relatório de tarefas atualmente suporta apenas PDF.")

        file_path = _gerar_tarefas_pdf(stats, project, sprint, tasks, user_id)
        return str(file_path)

    except Exception as exc:
        log_message(
            f"❌ Erro ao gerar relatório de tarefas: {exc}\n{traceback.format_exc()}",
            "error",
        )
        raise RuntimeError(f"Falha na geração do relatório de tarefas: {exc}")


def _validar_dados_tarefas(stats: dict[str, Any], formato: str) -> None:
    if not isinstance(stats, dict):
        raise TypeError("O campo 'stats' deve ser um objeto JSON.")
    _validar_formato(formato)


def _gerar_tarefas_pdf(
    stats: dict[str, Any],
    project: Optional[dict[str, Any]],
    sprint: Optional[dict[str, Any]],
    tasks: Optional[List[dict[str, Any]]],
    user_id: int,
) -> Path:
    """
    Gera PDF de tarefas.

    Idealmente, `gerar_relatorio_tarefas` deveria aceitar `output_path`.
    Aqui mantive tua lógica, mas com verificação defensiva.
    """
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("tarefas", user_id, "pdf")

    antes = {p.name for p in Path(".").glob("relatorio_tarefas_*.pdf")}

    gerar_relatorio_tarefas(
        stats,
        project,
        sprint,
        tasks,
        logo_path=str(LOGO_PATH),
    )

    depois = {p.name for p in Path(".").glob("relatorio_tarefas_*.pdf")}
    novos = sorted(depois - antes)

    if not novos:
        raise FileNotFoundError("O gerador de relatório de tarefas não produziu arquivo PDF detectável.")

    arquivo_gerado = Path(novos[-1])
    arquivo_gerado.rename(file_path)

    log_message(f"✅ PDF de tarefas gerado: {file_path.name}")
    return file_path


# =============================
# Processamento genérico
# =============================

def _resposta_erro_validacao(exc: Exception) -> JSONResponse:
    tipo = "Validação" if isinstance(exc, ValueError) else "Tipo de Dados"
    return JSONResponse(
        status_code=400 if isinstance(exc, ValueError) else 422,
        content={
            "erro": f"Erro de {tipo}",
            "mensagem": str(exc),
        },
    )


def _resposta_erro_interno(exc: Exception, mensagem: str) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "erro": mensagem,
            "mensagem": str(exc),
            "detalhes": traceback.format_exc().splitlines()[-3:],
        },
    )


async def _ler_payload_json(request: Request) -> dict[str, Any]:
    payload = await request.json()
    if not payload:
        raise ValueError("Corpo da requisição vazio")
    return payload


async def _processar_requisicao_relatorio(
    request: Request,
    user_id: int,
    db: Session,
    tipo_relatorio: str,
    gerar_relatorio_fn: Callable[[Any, int, Session, str], str],
) -> FileResponse | JSONResponse:
    start = datetime.now()

    try:
        payload = await _ler_payload_json(request)

        if "body" not in payload:
            raise ValueError("O campo 'body' é obrigatório")

        parametros = ParametrosRelatorioSchema(**payload.get("parametros", {}))
        dados_relatorio = payload["body"]

        _executar_limpeza_periodica(user_id)

        file_path = Path(
            gerar_relatorio_fn(
                dados_relatorio,
                user_id,
                db,
                parametros.formato,
            )
        )

        tempo = (datetime.now() - start).total_seconds()
        log_message(
            f"✅ Relatório {tipo_relatorio} {parametros.formato.upper()} entregue em {tempo:.2f}s"
        )
        return _criar_resposta_arquivo(file_path, tempo, parametros.formato)

    except (ValueError, TypeError) as exc:
        log_message(
            f"❌ Erro de validação em relatório {tipo_relatorio}: {exc}\n{traceback.format_exc()}",
            "error",
        )
        return _resposta_erro_validacao(exc)

    except Exception as exc:
        log_message(
            f"💥 Erro inesperado em relatório {tipo_relatorio}: {exc}\n{traceback.format_exc()}",
            "error",
        )
        return _resposta_erro_interno(exc, "Erro interno")


async def _processar_requisicao_relatorio_tarefas(
    request: Request,
    user_id: int,
    db: Session,
) -> FileResponse | JSONResponse:
    start = datetime.now()

    try:
        payload = await _ler_payload_json(request)

        body = payload.get("body", {})
        stats = body.get("stats")
        if not stats:
            raise ValueError("O campo 'stats' é obrigatório")

        parametros = ParametrosRelatorioSchema(**payload.get("parametros", {}))
        project = body.get("project")
        sprint = body.get("sprint")
        tasks = body.get("tasks")

        _executar_limpeza_periodica(user_id)

        file_path = Path(
            gerar_relatorio_tarefas_com_cache(
                stats=stats,
                user_id=user_id,
                project=project,
                sprint=sprint,
                tasks=tasks,
                db=db,
                formato=parametros.formato,
            )
        )

        tempo = (datetime.now() - start).total_seconds()
        log_message(
            f"✅ Relatório de tarefas {parametros.formato.upper()} entregue em {tempo:.2f}s"
        )
        return _criar_resposta_arquivo(file_path, tempo, parametros.formato)

    except (ValueError, TypeError) as exc:
        log_message(
            f"❌ Erro de validação em relatório de tarefas: {exc}\n{traceback.format_exc()}",
            "error",
        )
        return _resposta_erro_validacao(exc)

    except Exception as exc:
        log_message(
            f"💥 Erro no endpoint de tarefas: {exc}\n{traceback.format_exc()}",
            "error",
        )
        return _resposta_erro_interno(exc, "Falha ao gerar relatório de tarefas")


# =============================
# Endpoints
# =============================

@router.post("/gerar-relatorio")
async def gerar_relatorio(
    request: Request,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    return await _processar_requisicao_relatorio(
        request=request,
        user_id=user_id,
        db=db,
        tipo_relatorio="estrutura",
        gerar_relatorio_fn=gerar_relatorio_estrutura_com_cache,
    )


@router.post("/gerar-relatorio-query")
async def gerar_relatorio_query(
    request: Request,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    return await _processar_requisicao_relatorio(
        request=request,
        user_id=user_id,
        db=db,
        tipo_relatorio="query",
        gerar_relatorio_fn=gerar_relatorio_query_com_cache,
    )


@router.post("/gerar-relatorio-tarefas")
async def gerar_relatorio_tarefas_endpoint(
    request: Request,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    return await _processar_requisicao_relatorio_tarefas(
        request=request,
        user_id=user_id,
        db=db,
    )