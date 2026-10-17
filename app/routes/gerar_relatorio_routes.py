from __future__ import annotations

import csv
import inspect
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any,  Dict, List, Optional

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
from app.schemas.query_select_upAndInsert_schema import ParametrosRelatorioSchema, QueryPayload, QueryResultType
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message


router = APIRouter(prefix="/relatorio", tags=["gerar relatorio"])

REPORT_TEMP_DIR = Path(get_env("REPORT_TEMP_DIR", "temp_reports"))
CACHE_TTL = get_env_int("CACHE_TTL", 900)
BASE_DIR = Path(__file__).resolve().parent.parent
LOGO_PATH = BASE_DIR / "relatorio" / get_env("LOGO_PATH", "fotor-ai-20250218134257.jpg")

FORMATO_PDF = "pdf"
FORMATO_EXCEL = "excel"
FORMATO_CSV = "csv"
FORMATO_JSON = "json"
FORMATOS_VALIDOS = {FORMATO_PDF, FORMATO_EXCEL, FORMATO_CSV, FORMATO_JSON}

REPORT_TEMP_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# Helpers Genéricos
# =============================

def _criar_resposta_arquivo(file_path: Path, tempo: float, formato: str) -> FileResponse:
    media_map = {
        FORMATO_PDF: "application/pdf",
        FORMATO_EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        FORMATO_CSV: "text/csv; charset=utf-8",
        FORMATO_JSON: "application/json; charset=utf-8",
    }
    media_type = media_map.get(formato, "application/octet-stream")
    headers = {
        "X-Tempo-Geracao": f"{tempo:.2f}s",
        "X-Formato": formato,
        "Content-Disposition": f'attachment; filename="{file_path.name}"',
    }
    log_message(f"📦 Enviando arquivo: {file_path.name} | Formato: {formato.upper()} | Tempo: {tempo:.2f}s")
    return FileResponse(path=str(file_path), media_type=media_type, filename=file_path.name, headers=headers)


def _gerar_nome_arquivo(tipo: str, user_id: int, extensao: str) -> str:
    return f"relatorio_{tipo}_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{extensao}"


def limpar_arquivos_temporarios(dias_retencao: int = 1) -> None:
    try:
        limite = datetime.now().timestamp() - (dias_retencao * 86400)
        removidos = [
            f.unlink(missing_ok=True) or f.name 
            for pattern in ("*.pdf", "*.xlsx", "*.csv", "*.json") 
            for f in REPORT_TEMP_DIR.glob(pattern) 
            if f.is_file() and f.stat().st_ctime < limite
        ]
        if removidos:
            log_message(f"🧹 Total de arquivos temporários removidos: {len(removidos)}")
    except Exception as exc:
        log_message(f"⚠️ Erro na limpeza: {exc}", "warning")


def _validar_formato(formato: str) -> None:
    if formato not in FORMATOS_VALIDOS:
        raise ValueError(f"Formato inválido. Use: {', '.join(FORMATOS_VALIDOS)}")


# =============================
# Módulos de Geração (Core)
# =============================

@cache_result(ttl=CACHE_TTL, user_id="user_relatorio_{user_id}")
def gerar_relatorio_estrutura_com_cache(metadata_raw: List[Dict[str, Any]], user_id: int, db: Session, formato: str = FORMATO_PDF) -> str:
    _validar_formato(formato)
    if not metadata_raw or not isinstance(metadata_raw, list):
        raise ValueError("Lista de metadados vazia ou inválida.")
    
    extensao_map = {
        FORMATO_PDF: "pdf",
        FORMATO_EXCEL: "xlsx",
        FORMATO_CSV: "csv",
        FORMATO_JSON: "json",
    }
    extensao = extensao_map.get(formato, "bin")
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("metadados", user_id, extensao)
    
    if formato == FORMATO_PDF:
        GenericReportGenerator(logo_path=LOGO_PATH).generate_report(str(file_path), ReportStructureBuilder(metadata_raw).build())
    elif formato == FORMATO_EXCEL:
        ExcelReportGenerator(logo_path=LOGO_PATH).generate_structure_report(str(file_path), metadata_raw)
    elif formato == FORMATO_CSV:
        with open(file_path, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["tabela", "coluna", "tipo", "nulo", "chave_primaria", "padrao"])
            for item in metadata_raw:
                if isinstance(item, dict):
                    tbl = item.get("table_name") or item.get("tabela") or item.get("name") or "tabela"
                    cols = item.get("columns") or item.get("colunas") or []
                    if cols:
                        for col in cols:
                            if isinstance(col, dict):
                                writer.writerow([
                                    tbl,
                                    col.get("name") or col.get("nome") or "",
                                    col.get("type") or col.get("tipo") or "",
                                    col.get("nullable", col.get("nulo", True)),
                                    col.get("primary_key", col.get("pk", False)),
                                    col.get("default") or col.get("padrao") or "",
                                ])
                    else:
                        writer.writerow([tbl, "", "", "", "", ""])
    elif formato == FORMATO_JSON:
        with open(file_path, mode="w", encoding="utf-8") as f:
            json.dump({
                "total_tabelas": len(metadata_raw),
                "metadados": metadata_raw,
                "gerado_em": datetime.now().isoformat(),
            }, f, ensure_ascii=False, indent=2, default=str)
        
    return str(file_path)


# @cache_result(ttl=CACHE_TTL, user_id="user_relatorio_query_{user_id}")
def _build_query_context(query_result: dict, preview: list) -> dict:
    """Contexto de variáveis para um relatório de CONSULTA (usado no template)."""
    now = datetime.now()
    columns = query_result.get("columns") or (list(preview[0].keys()) if preview else [])
    return {
        "data": {
            "hoje": now.strftime("%d/%m/%Y"),
            "hora": now.strftime("%H:%M"),
            "iso": now.isoformat(timespec="seconds"),
        },
        "query": {
            "total": len(preview),
            "colunas": columns,
            "linhas": preview,  # lista de dicts → binding de tabela
            "duracao_ms": query_result.get("duration_ms", 0),
            "sql": query_result.get("query", ""),
        },
    }


async def gerar_relatorio_query_com_cache(query_result: dict, user_id: int, db: Session, formato: str = FORMATO_PDF) -> str:
    _validar_formato(formato)

    # Template opcional do construtor (page `createtamplete`). Se presente, é
    # ele que define o relatório, com as variáveis `{{...}}` preenchidas.
    user_template = query_result.pop("template", None)

    payload_obj = QueryPayload(**(query_result.get("QueryPayload") or query_result.get("queryPayload", {})))
    payload_obj.limit = None
    rs_dict = await QueryExecutionService().execute_query(payload_obj, db, user_id)

    preview = rs_dict.get("preview", [])
    query_result["preview"] = preview
    query_result["duration_ms"] = rs_dict.get("duration_ms", query_result.get("duration_ms", 0))

    # 🚀 A CORREÇÃO ESTÁ AQUI: Remover AMBAS as chaves e NÃO voltar a injetar o payload_obj
    query_result.pop("QueryPayload", None)
    query_result.pop("queryPayload", None)

    resultado_tipado = QueryResultType(**query_result)
    extensao_map = {
        FORMATO_PDF: "pdf",
        FORMATO_EXCEL: "xlsx",
        FORMATO_CSV: "csv",
        FORMATO_JSON: "json",
    }
    extensao = extensao_map.get(formato, "bin")
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("query", user_id, extensao)

    if formato == FORMATO_PDF:
        gen = GenericReportGenerator(logo_path=LOGO_PATH)
        if user_template and isinstance(user_template, list):
            # Usa o TEMPLATE do utilizador + contexto (variáveis + binding).
            context = _build_query_context(query_result, preview)
            gen.generate_report(str(file_path), user_template, context=context)
        else:
            gen.generate_report(str(file_path), QueryReportBuilder(resultado_tipado).build())
    elif formato == FORMATO_EXCEL:
        ExcelReportGenerator(logo_path=LOGO_PATH).generate_query_report(str(file_path), resultado_tipado)
    elif formato == FORMATO_CSV:
        with open(file_path, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            cols = query_result.get("columns") or (list(preview[0].keys()) if preview else [])
            if cols:
                writer.writerow(cols)
                for row in preview:
                    if isinstance(row, dict):
                        writer.writerow([row.get(c, "") for c in cols])
                    elif isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    else:
                        writer.writerow([str(row)])
    elif formato == FORMATO_JSON:
        with open(file_path, mode="w", encoding="utf-8") as f:
            json.dump({
                "total": len(preview),
                "duracao_ms": query_result.get("duration_ms", 0),
                "colunas": query_result.get("columns") or (list(preview[0].keys()) if preview else []),
                "linhas": preview,
                "gerado_em": datetime.now().isoformat(),
            }, f, ensure_ascii=False, indent=2, default=str)

    return str(file_path)

def gerar_relatorio_tarefas_com_cache(stats: dict, user_id: int, project: Optional[dict], sprint: Optional[dict], tasks: Optional[list], db: Session, formato: str = FORMATO_PDF) -> str:
    _validar_formato(formato)
    extensao_map = {
        FORMATO_PDF: "pdf",
        FORMATO_EXCEL: "xlsx",
        FORMATO_CSV: "csv",
        FORMATO_JSON: "json",
    }
    extensao = extensao_map.get(formato, "bin")
    file_path = REPORT_TEMP_DIR / _gerar_nome_arquivo("tarefas", user_id, extensao)
    REPORT_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    if formato == FORMATO_PDF:
        gerar_relatorio_tarefas(
            stats,
            project,
            sprint,
            tasks,
            logo_path=str(LOGO_PATH),
            output_path=str(file_path),
        )
    elif formato == FORMATO_CSV:
        with open(file_path, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "titulo", "status", "prioridade", "responsavel"])
            for t in (tasks or []):
                if isinstance(t, dict):
                    writer.writerow([
                        t.get("id", ""),
                        t.get("titulo") or t.get("title") or "",
                        t.get("status", ""),
                        t.get("prioridade") or t.get("priority") or "",
                        t.get("user") or t.get("responsavel") or "",
                    ])
    elif formato == FORMATO_JSON:
        with open(file_path, mode="w", encoding="utf-8") as f:
            json.dump({
                "estatisticas": stats,
                "projeto": project,
                "sprint": sprint,
                "tarefas": tasks or [],
                "gerado_em": datetime.now().isoformat(),
            }, f, ensure_ascii=False, indent=2, default=str)
    else:
        raise ValueError(f"Formato '{formato}' não implementado para relatório de tarefas (use pdf, csv ou json).")

    if not file_path.exists():
        raise FileNotFoundError(f"O gerador não produziu o ficheiro {extensao}.")

    return str(file_path)


# =============================
# Processador Unificado Central
# =============================

# Dicionário que mapeia o tipo de relatório para a função geradora e a forma de extrair os argumentos
PROCESSORS = {
    "estrutura": (gerar_relatorio_estrutura_com_cache, lambda b: {"metadata_raw": b}),
    "query": (gerar_relatorio_query_com_cache, lambda b: {"query_result": b}),
    "tarefas": (gerar_relatorio_tarefas_com_cache, lambda b: {
        "stats": b.get("stats"), "project": b.get("project"), "sprint": b.get("sprint"), "tasks": b.get("tasks")
    })
}

async def _processar_relatorio_unificado(request: Request, user_id: int, db: Session, tipo: str) -> FileResponse | JSONResponse:
    """Motor central: Lida com cache, timing, logs e erros de forma unificada para todos os endpoints."""
    start = datetime.now()
    try:
        payload = await request.json()
        if not payload or "body" not in payload:
            raise ValueError("O campo 'body' é obrigatório no corpo da requisição")

        if user_id % 10 == 0: limpar_arquivos_temporarios()

        body = payload["body"]
        parametros = ParametrosRelatorioSchema(**payload.get("parametros", {}))
        
        # Obtém a função correta e constrói os parâmetros de injeção dinamicamente
        gerar_fn, kwargs_builder = PROCESSORS[tipo]
        if tipo == "tarefas" and not body.get("stats"):
            raise ValueError("O campo 'stats' é obrigatório no body para tarefas")

        # Invoca a função de geração de relatório (Sync ou Async)
        resultado_fn = gerar_fn(**kwargs_builder(body), user_id=user_id, db=db, formato=parametros.formato)
        
        caminho_string = await resultado_fn if inspect.isawaitable(resultado_fn) else resultado_fn

        tempo = (datetime.now() - start).total_seconds()
        log_message(f"✅ Sucesso: Relatório {tipo} {parametros.formato.upper()} finalizado.")
        
        return _criar_resposta_arquivo(Path(caminho_string), tempo, parametros.formato)

    except (ValueError, TypeError) as exc:
        log_message(f"❌ Validação falhou ({tipo}): {exc}\n{traceback.format_exc()}", "error")
        return JSONResponse(status_code=400, content={"erro": "Erro de Validação", "mensagem": str(exc)})
    
    except Exception as exc:
        log_message(f"💥 Erro interno ({tipo}): {exc}\n{traceback.format_exc()}", "error")
        return JSONResponse(status_code=500, content={"erro": "Erro interno do servidor", "mensagem": str(exc)})


# =============================
# Endpoints
# =============================

@router.post("/gerar-relatorio")
async def gerar_relatorio(request: Request, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return await _processar_relatorio_unificado(request, user_id, db, "estrutura")

@router.post("/gerar-relatorio-query")
async def gerar_relatorio_query(request: Request, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return await _processar_relatorio_unificado(request, user_id, db, "query")

@router.post("/gerar-relatorio-tarefas")
async def gerar_relatorio_tarefas_endpoint(request: Request, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return await _processar_relatorio_unificado(request, user_id, db, "tarefas")