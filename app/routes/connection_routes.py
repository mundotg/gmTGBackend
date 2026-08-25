from __future__ import annotations

import json
import os
import traceback
from typing import Any, Optional, cast
from fastapi.concurrency import run_in_threadpool
import pandas as pd
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
    status,
)
from sqlalchemy.orm import Session

from app.config.cache_manager import CACHE_PREFIX, cache_result, clear_cache
from app.config.dependencies import get_session_by_connection
from app.config.engine_manager_cache import EngineManager
from app.cruds.connection_cruds import (
    assert_connection_access,
    create_connection_log,
    create_db_connection,
    delete_connection,
    desactivate_all_connections,
    disconnect_active_connection,
    get_active_connection_by_connid,
    get_active_connection_by_userid,
    get_connection_logs,
    get_connection_logs_pagination,
    get_connection_or_404,
    get_db_connection_by_id,
    get_db_connections,
    get_db_connections_pagination_v1,
    list_connection_shares,
    map_status,
    query_connections_simple,
    revoke_connection_share,
    set_active_connection,
    share_connection,
    upsert_db_connection,
)
from app.database import get_db
from app.models.connection_models import DBConnection
from app.models.user_model import User
from app.schemas.connetion_schema import (
    ConnectionAccessLevel,
    ConnectionAccessOut,
    ConnectionPaginationOutput,
    ConnectionPassUserOut,
    ConnectionRequest,
    ConnectionShareCreate,
    ConnectionShareOut,
    ConnectionShareUpdate,
    DBConnectionBase,
    DbConnectionOutput,
    SavedConnectionBase,
)
from app.services.crypto_utils import secret_encrypt, to_wire
from app.services.dataset_service import (
    read_dataframe,
    read_dataset_source,
    save_dataframe_to_sqlite,
)
from app.ultils.conect_database import close_engine
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user, is_superadmin

router = APIRouter(prefix="/conn", tags=["connections"])


# =========================================================
# Cache
# =========================================================


# ⚠️ Estas funções TÊM de devolver dados simples (dicts, datas em ISO),
# nunca objetos ORM.
#
# O `cache_result` devolve o resultado convertido por `_to_cacheable` nos
# hits, mas o objeto original nos misses. Uma função que devolva objetos
# ORM entrega, portanto, formas diferentes conforme haja cache ou não: o
# primeiro pedido funciona e os seguintes rebentam com
# "'str' object has no attribute 'id'", porque desempacotar um dict dá as
# suas chaves. Normalizar aqui torna as duas formas iguais.


@cache_result(ttl=300, user_id="user_{user_id}")
async def get_db_connections_pagination_cached(
    db: Session, user_id: int, page: int, limit: int
) -> dict[str, Any]:
    dados = get_db_connections_pagination_v1(db, user_id, page, limit)

    return {
        "page": dados["page"],
        "limit": dados["limit"],
        "total": dados["total"],
        "results": [
            {
                "id": conn.id,
                "name": conn.name,
                # Repouso → transporte: o valor guardado está cifrado com a
                # chave-mestra, que o frontend não tem.
                "host": to_wire(conn.host),
                "database": conn.database_name,
                "type": conn.type,
                "status": conn.status,
                "last_used": last_used.isoformat() if last_used else None,
            }
            for conn, last_used in dados["results"]
        ],
    }


@cache_result(ttl=1800, user_id="user_{user_id}")
async def get_db_connection_credentials_cached(
    db: Session, conn_id: int
) -> dict[str, Any] | None:
    conn = get_db_connection_by_id(db, conn_id)

    if not conn:
        return None

    return {
        "id": conn.id,
        "password": to_wire(conn.password),
        "username": to_wire(conn.username),
        "service": conn.service,
        "sslmode": conn.sslmode,
        "trustServerCertificate": conn.trustServerCertificate,
        # Vazio nas conexões por campos separados; é o que diz ao formulário
        # se deve abrir no modo URL.
        "url": to_wire(conn.url) if conn.url else "",
    }


# =========================================================
# Helpers
# =========================================================


def _log_and_raise(
    *,
    db: Session,
    user_id: int,
    action: str,
    message: str,
    status_code: int,
    connection_id: Optional[int] = None,
    details: Optional[dict[str, Any]] = None,
    level: str = "error",
) -> None:
    """
    Padroniza log técnico + log de conexão + exceção HTTP.
    """
    log_message(f"{message} : {details}", level=level)

    try:
        create_connection_log(
            db,
            connection_id=connection_id,
            action=action,
            status="error" if status_code >= 400 else "success",
            details=details or {"message": message},
            user_id=user_id,
        )
    except Exception as log_error:
        log_message(
            f"⚠️ Falha ao registrar log de conexão: {str(log_error)}{details}",
            level="warning",
        )

    raise HTTPException(status_code=status_code, detail=message)


def _cleanup_engine(user_id: int) -> None:
    """
    Descarta engine ativo do usuário, se existir.
    """
    try:
        current_engine = EngineManager.get(user_id)
        if current_engine:
            # close_engine em vez de .dispose(): num MongoClient o pymongo
            # resolveria ".dispose" como o nome de uma base de dados e o erro
            # seria "'Database' object is not callable".
            close_engine(current_engine)
            EngineManager.remove(user_id)
            log_message(
                f"🔁 Engine do usuário {user_id} descartado com sucesso.", level="info"
            )
    except ValueError:
        log_message(f"ℹ️ Nenhum engine ativo para o usuário {user_id}.", level="info")
    except Exception as e:
        log_message(
            f"⚠️ Erro ao limpar engine do usuário {user_id}: {str(e)}", level="warning"
        )


def _validate_connection_owner(
    conn: Optional[DBConnection], conn_id: int
) -> DBConnection:
    """
    Garante que a conexão existe.
    """
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conexão com ID {conn_id} não encontrada.",
        )
    return conn


def _invalidate_connections_cache() -> None:
    """
    Descarta o cache da listagem de conexões.

    Sem isto, quem acabou de receber acesso a uma conexão só a via aparecer
    até 5 minutos depois (TTL de `get_db_connections_pagination_cached`) — e
    isso lê-se como "a partilha não funcionou".

    O padrão inclui o nome da função, por isso em Redis só esta entrada cai;
    o cache em memória é curto e reconstrói-se ao primeiro pedido.
    """
    try:
        clear_cache(f"{CACHE_PREFIX}get_db_connections_pagination_cached:*")
    except Exception as e:  # o cache nunca deve derrubar a operação principal
        log_message(f"⚠️ Falha ao invalidar cache de conexões: {e}", level="warning")


def _build_connection_output(
    connection_id: int, message: str, connected: bool
) -> DbConnectionOutput:
    return DbConnectionOutput(
        connection_id=connection_id,
        message=message,
        connect=connected,
    )


# =========================================================
# Endpoints de conexão
# =========================================================


@router.post("/salvarconnections/", response_model=SavedConnectionBase)
async def save_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Salva uma nova conexão de banco de dados.
    """
    try:
        saved_conn = create_db_connection(db, user_id, conn_data)

        create_connection_log(
            db,
            connection_id=cast(int, saved_conn.id) or 0,
            action="Conexão salva",
            status="success",
            details={
                "host": conn_data.host,
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return saved_conn

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao salvar conexão",
            message="Erro ao salvar conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.post("/connect/", response_model=DbConnectionOutput)
async def test_and_connect(
    request: ConnectionRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Testa a conexão de forma assíncrona, salva ou atualiza e define como ativa.
    """
    db_conn = None

    try:
        conn_data = request.conn_data
        action_type = request.tipo

        # 1. Limpeza prévia da engine antiga no EngineManager
        _cleanup_engine(user_id)

        # 2. TESTE DE CONEXÃO (Ponto crítico de bloqueio)
        # Executamos em threadpool para que o teste do driver (ex: pyodbc) não trave o backend
        engine = await run_in_threadpool(get_session_by_connection, conn_data)

        if not engine:
            raise ValueError("Não foi possível criar a Engine de conexão.")

        # 3. Persistência e Lógica de Negócio
        # Usamos blocos separados para garantir que a engine só vá para o cache se o DB local salvar
        if action_type == "con":
            db_conn = create_db_connection(db, user_id, conn_data)
        elif action_type == "upsert":
            db_conn = upsert_db_connection(db, user_id, conn_data)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tipo de operação inválido. Use 'con' ou 'upsert'.",
            )

        if not db_conn or not db_conn.id:
            raise ValueError("Falha ao persistir dados da conexão no banco local.")

        # 4. Ativação da Conexão
        # Definimos no Cache de memória (EngineManager) e no Banco de Dados
        EngineManager.set(engine, user_id, connection_id=db_conn.id)
        desactivate_all_connections(db, user_id)
        set_active_connection(db, user_id, db_conn.id)

        # 5. Log de Auditoria
        # No modo "por URL" os campos separados chegam vazios (o host e a base
        # ficam derivados no registo), por isso usa-se o que ficou guardado.
        create_connection_log(
            db,
            connection_id=db_conn.id,
            action="Conexão testada e ativada",
            status="success",
            details={
                "database": conn_data.database_name or db_conn.database_name,
                "type": conn_data.type,
                "host": conn_data.host or db_conn.host,
                "por_url": bool(conn_data.url),
            },
            user_id=user_id,
        )

        alvo = conn_data.database_name or db_conn.database_name or conn_data.name

        return _build_connection_output(
            connection_id=db_conn.id,
            message=f"✅ Conexão com {alvo} estabelecida e salva.",
            connected=True,
        )

    except Exception as e:
        # Se algo falhou, garantimos que a engine não fique órfã no cache
        _cleanup_engine(user_id)

        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Tentativa de conexão falhou",
            message="Falha ao conectar. Verifique as credenciais e o acesso à rede.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=db_conn.id if db_conn else None,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.put("/connect-toggle/", response_model=DbConnectionOutput)
async def connect_or_disconnect(
    conn_id: int = Body(..., embed=True),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Alterna entre conectar e desconectar uma conexão salva sem travar o asyncio.
    """
    try:
        active_conn = get_active_connection_by_connid(db, conn_id)

        # 1. LÓGICA DE DESCONEXÃO
        if active_conn and active_conn.status:
            _cleanup_engine(user_id)

            # CORRIGIDO: Acesso via ponto (objeto) em vez de colchetes
            disconnect_active_connection(db, active_conn.connection_id)

            conn_data = _validate_connection_owner(
                get_db_connection_by_id(db, active_conn.connection_id),
                active_conn.connection_id,
            )

            create_connection_log(
                db,
                connection_id=conn_data.id,
                action="Desconexão manual",
                status="disconnected",
                details={"database": conn_data.database_name, "type": conn_data.type},
                user_id=user_id,
            )

            return _build_connection_output(
                connection_id=conn_data.id,
                message="🔌 Desconectado com sucesso.",
                connected=False,
            )

        # 2. LÓGICA DE CONEXÃO (ATIVAÇÃO)
        conn_data = _validate_connection_owner(
            get_db_connection_by_id(db, conn_id), conn_id
        )

        desactivate_all_connections(db, user_id)

        # MELHORIA: Rodar em threadpool para não travar o loop se a rede demorar
        from fastapi.concurrency import run_in_threadpool

        engine = await run_in_threadpool(get_session_by_connection, conn_data)

        if not engine:
            raise ValueError("Engine não foi criada corretamente.")

        EngineManager.set(engine, user_id)
        set_active_connection(db, user_id, conn_data.id)

        create_connection_log(
            db,
            connection_id=conn_data.id,
            action="Conexão ativada",
            status="success",
            details={"database": conn_data.database_name, "type": conn_data.type},
            user_id=user_id,
        )

        return _build_connection_output(
            connection_id=conn_data.id,
            message="✅ Conectado com sucesso.",
            connected=True,
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao alternar conexão",
            message="Erro ao conectar ou desconectar a conexão.",
            status_code=500,
            connection_id=conn_id,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/connections/", response_model=ConnectionPaginationOutput)
async def list_connections_paginated(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    """
    Lista conexões salvas com paginação.
    """
    try:
        connections = await get_db_connections_pagination_cached(
            db, user_id, page, limit
        )
        active_conn = get_active_connection_by_userid(db, user_id)
        active_conn_id = cast(int, active_conn.connection_id) if active_conn else None

        return ConnectionPaginationOutput(
            page=connections["page"],
            limit=connections["limit"],
            total=connections["total"],
            results=[
                SavedConnectionBase.model_validate(
                    {
                        **item,
                        # Depende da conexão ativa do momento, por isso é
                        # calculado aqui e não dentro do valor em cache.
                        "status": map_status(
                            item["status"], item["id"], active_conn_id
                        ),
                    }
                )
                for item in connections["results"]
            ],
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao listar conexões",
            message=f"Erro interno ao listar conexões. {str(e)}{traceback.print_exc()}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"error": str(e)},
        )


@router.delete("/delete_connection/{conn_id}", response_model=DbConnectionOutput)
async def delete_connection_save(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    Deleta uma conexão salva. Só o dono (ou um super admin) pode apagar —
    quem recebeu a conexão por partilha, mesmo com nível 'gestão', não pode.
    """
    user_id = actor.id

    alvo = get_connection_or_404(db, conn_id)
    acesso = assert_connection_access(db, alvo, actor, ConnectionAccessLevel.read)

    if not acesso.can_delete:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Só o dono da conexão (ou um super admin) a pode apagar.",
        )

    try:
        conn = delete_connection(db, conn_id)
        if conn:
            # A FK connection_logs.connection_id é ON DELETE CASCADE: a conexão
            # (e os seus logs) já não existe, por isso NÃO se pode referenciar
            # `conn_id` aqui — dava ForeignKeyViolation e a auditoria da própria
            # eliminação nunca era gravada. Guarda-se o id nos `details`.
            create_connection_log(
                db,
                connection_id=None,
                action=f"Conexão deletada: {conn.name}",
                status="success",
                details={
                    "deleted_connection_id": conn_id,
                    "database": conn.database_name,
                    "type": conn.type,
                },
                user_id=user_id,
            )

        _cleanup_engine(user_id)

        return _build_connection_output(
            connection_id=conn_id,
            message="Conexão deletada com sucesso.",
            connected=False,
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action=f"Erro ao deletar conexão {conn_id}",
            message="Erro ao deletar a conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=conn_id,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/get_credencial_db/{conn_id}", response_model=ConnectionPassUserOut)
async def get_credenciais(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    Obtém credenciais de uma conexão específica.

    Exige acesso de leitura: dono, super admin, ou alguém com partilha ativa.
    """
    user_id = actor.id

    alvo = get_connection_or_404(db, conn_id)
    assert_connection_access(db, alvo, actor, ConnectionAccessLevel.read)

    try:
        credenciais = await get_db_connection_credentials_cached(db, conn_id)

        if not credenciais:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conexão com ID {conn_id} não encontrada.",
            )

        return ConnectionPassUserOut(
            password=credenciais["password"],
            username=credenciais["username"],
            service=credenciais["service"],
            sslmode=credenciais["sslmode"],
            trustServerCertificate=credenciais["trustServerCertificate"],
            url=credenciais.get("url") or "",
        )

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao obter credenciais",
            message="Erro ao obter credenciais da conexão.",
            status_code=status.HTTP_404_NOT_FOUND,
            connection_id=conn_id,
            details={"error": str(e)},
        )


@router.get("/db_full/{conn_id}")
async def get_db_full(
    conn_id: int,
    refresh: bool = Query(False, description="Ignora a cache Redis e recarrega"),
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    Conexão + estruturas (tabelas/coleções, campos e FKs) para o diagrama ER
    (página `mll`). Cacheado em **Redis** por (utilizador, conexão) para
    desempenho — a introspeção é cara e o schema muda raramente.

    Suporta **NoSQL**: para MongoDB (onde a introspeção SQL não se aplica) usa
    as estruturas já guardadas na BD; sem relações (o Mongo não tem FKs).
    """
    from sqlalchemy.orm import selectinload
    from app.config.redis import read_cache, write_cache
    from app.ultils.db_full_cache import DB_FULL_TTL, db_full_cache_key
    from app.services.crypto_utils import secret_decrypt
    from app.services.database_inspector import get_strutures_names
    from app.models.dbstructure_models import DBStructure
    from app.schemas.dbstructure_schema import DBStructureOut

    user_id = actor.id
    alvo = get_connection_or_404(db, conn_id)
    assert_connection_access(db, alvo, actor, ConnectionAccessLevel.read)

    cache_key = db_full_cache_key(conn_id)
    if not refresh:
        cached = read_cache(cache_key)
        if cached:
            return cached

    if refresh:
        # `refresh` tem de voltar a INTROSPECIONAR. Saltar só a cache Redis
        # devolvia exactamente as mesmas linhas guardadas, por isso tabelas
        # novas nunca apareciam no diagrama por muito que se recarregasse.
        try:
            get_strutures_names(conn_id, user_id, db, force_sync=True)
        except Exception as e:  # noqa: BLE001
            log_message(
                f"⚠️ db_full: falha ao re-sincronizar o schema (conn {conn_id}): {e}",
                "warning",
            )

    # DESEMPENHO: lê as estruturas JÁ GUARDADAS numa só query, carregando os
    # campos com `selectinload` (rápido: 2 queries, sem N+1) — em vez de
    # sincronizar todas as tabelas a cada pedido (dezenas de segundos). Com o
    # Redis por cima, os loads seguintes são instantâneos.
    structures_ser: list = []
    try:
        structs = (
            db.query(DBStructure)
            .options(selectinload(DBStructure.fields))
            .filter(
                DBStructure.db_connection_id == conn_id,
                DBStructure.is_deleted == False,  # noqa: E712
            )
            .all()
        )
        if structs:
            structures_ser = [
                DBStructureOut.model_validate(s).model_dump(mode="json") for s in structs
            ]
        else:
            # Nada guardado → bootstrap via introspeção (uma vez). Para MongoDB
            # (introspeção SQL não se aplica) fica vazio até navegar as coleções.
            bootstrapped = get_strutures_names(conn_id, user_id, db) or []
            structures_ser = [
                s.model_dump(mode="json") if hasattr(s, "model_dump") else s
                for s in bootstrapped
            ]
    except Exception as e:  # noqa: BLE001
        log_message(f"⚠️ db_full: falha a obter estruturas (conn {conn_id}): {e}", "warning")
        structures_ser = []

    def _dec(v):
        try:
            return secret_decrypt(v) if v else v
        except Exception:  # noqa: BLE001
            return v

    result = {
        "id": alvo.id,
        "name": alvo.name,
        "type": alvo.type,
        "host": _dec(alvo.host),
        "port": alvo.port,
        "database_name": alvo.database_name,
        "structures": structures_ser,
    }

    # Schema muda pouco; as rotas de DDL invalidam esta chave, e ?refresh=true
    # força uma re-introspeção completa.
    write_cache(cache_key, result, ttl=DB_FULL_TTL)
    return result


@router.post("/testconnections/", response_model=DbConnectionOutput)
async def test_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Apenas testa uma conexão sem ativá-la.
    """
    temp_conn = None

    try:
        temp_conn = create_db_connection(db, user_id, conn_data)
        engine = get_session_by_connection(conn_data)

        if not engine:
            raise ValueError("Engine não foi criado corretamente.")

        try:
            # Idem: .dispose() não fecharia um MongoClient, deixando a
            # ligação pendurada até ao timeout do servidor.
            close_engine(engine)
        except Exception:
            pass

        create_connection_log(
            db,
            connection_id=temp_conn.id,
            action="Teste de conexão",
            status="success",
            details={
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return _build_connection_output(
            connection_id=temp_conn.id,
            message="Conexão testada com sucesso.",
            connected=True,
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Teste de conexão falhou",
            message="Erro ao testar conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=temp_conn.id if temp_conn else None,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/paginate")
async def listar_elementos_connections(
    search: str | None = Query(None, description="Texto para pesquisa"),
    filtro: str | None = Query(None, description="Filtro opcional em formato JSON"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Paginação genérica de conexões.
    """
    filters = None

    if filtro:
        try:
            filters = json.loads(filtro) or {}
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Formato inválido de filtro JSON.",
            )

    return query_connections_simple(
        db,
        user_id=user_id,
        search=search,
        page=page,
        limit=limit,
        filters=filters,
    )


# =========================================================
# Endpoint de dataset
# =========================================================
@router.post("/dataset/open")
async def open_dataset(
    file: Optional[UploadFile] = File(None, description="Arquivo para upload"),
    url: Optional[str] = Form(None, description="URL pública do dataset"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Abre um dataset (que pode conter múltiplas tabelas) por upload ou URL pública,
    converte para um único ficheiro SQLite e regista como conexão do utilizador.
    """
    conn = None
    db_path: Optional[str] = None
    filename = "dataset"

    try:
        # 1. Lê os bytes do ficheiro/url
        contents, filename, source_type = await read_dataset_source(file=file, url=url)

        # 2. Transforma num dicionário de DataFrames (suporta múltiplas abas/tabelas)
        dict_dfs = read_dataframe(contents, filename)

        # 3. Guarda tudo no SQLite e obtém o caminho e as tabelas criadas
        db_path, tabelas_criadas = save_dataframe_to_sqlite(
            dict_dfs=dict_dfs,
            user_id=user_id,
            original_filename=filename,
        )

        # 4. Registar a conexão na base de dados principal (o MustaInf)
        # Usamos o nome da primeira tabela como database_name principal para referência,
        # mas o SQLite tem todas lá dentro.
        main_table_name = tabelas_criadas[0] if tabelas_criadas else "main_table"

        conn_data = DBConnectionBase(
            name=f"Dataset: {filename}",
            type="SQLite",
            host=secret_encrypt(db_path),  # caminho absoluto do ficheiro SQLite
            port=0,
            username="",
            password="",
            database_name=main_table_name,
            status="available",
        )

        conn = create_db_connection(db, user_id, conn_data)

        # 5. Prepara os metadados de resposta para todas as tabelas
        # Calcula o total de linhas somando todas as tabelas
        total_rows_all_tables = sum(len(df) for df in dict_dfs.values())

        # Cria um resumo com as informações de cada tabela extraída
        tabelas_info = []
        for tab_name in tabelas_criadas:
            df = dict_dfs[tab_name]
            tabelas_info.append(
                {
                    "table_name": tab_name,
                    "rows": int(len(df)),
                    "columns": int(len(df.columns)),
                    "column_names": list(df.columns),
                    # Preview apenas das primeiras 3 linhas de cada tabela para não sobrecarregar o JSON
                    "preview": df.head(3)
                    .where(pd.notna(df.head(3)), None)
                    .to_dict(orient="records"),
                }
            )

        log_message(
            f"📄 Dataset '{filename}' importado pelo usuário {user_id}. "
            f"Fonte: {source_type}. Tabelas: {len(tabelas_criadas)}. Linhas Totais: {total_rows_all_tables}.",
            level="info",
        )

        create_connection_log(
            db,
            connection_id=conn.id,
            action="Dataset importado e convertido para SQLite",
            status="success",
            details={
                "source": source_type,
                "host": db_path,
                "tables_created": tabelas_criadas,
                "total_rows_processed": total_rows_all_tables,
            },
            user_id=user_id,
        )

        return {
            "success": True,
            "source": source_type,
            "filename": filename,
            "connection_id": conn.id,
            "tables": tabelas_info,  # Array com info detalhada de cada tabela importada
            "total_tables_extracted": len(tabelas_criadas),
            "message": f"Dataset carregado com sucesso. {len(tabelas_criadas)} tabela(s) criada(s).",
        }

    except HTTPException as e:
        log_message(
            f"⚠️ Falha ao abrir dataset '{filename}' para o usuário {user_id}: {e.detail}",
            level="warning",
        )

        if conn:
            create_connection_log(
                db,
                connection_id=conn.id,
                action="Falha ao importar dataset",
                status="error",
                details={
                    "error": e.detail,
                    "filename": filename,
                },
                user_id=user_id,
            )

        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass

        raise

    except Exception as e:
        log_message(
            f"❌ Erro interno ao abrir dataset '{filename}' para o usuário {user_id}: "
            f"{str(e)}\n{traceback.format_exc()}",
            level="error",
        )

        if conn:
            create_connection_log(
                db,
                connection_id=cast(int, conn.id),
                action="Erro crítico no processamento do dataset",
                status="error",
                details={
                    "error": str(e),
                    "filename": filename,
                },
                user_id=user_id,
            )

        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao processar o dataset.",
        )


# =========================================================
# 🤝 Partilha de conexões
#
# Quem criou a conexão decide quem mais lhe acede. O super admin (`admin:*`)
# passa por cima de tudo. Não é preciso nenhuma permissão RBAC global aqui:
# a autoridade vem de ser dono da conexão.
# =========================================================


@router.get("/connections/{conn_id}/access", response_model=ConnectionAccessOut)
async def get_connection_access_info(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    O que o utilizador atual pode fazer nesta conexão + lista de acessos
    concedidos (esta última só para quem pode gerir partilhas).
    """
    return list_connection_shares(db, conn_id, actor)


@router.get("/connections/{conn_id}/shareable-users", response_model=list[dict])
async def list_shareable_users(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    Colegas a quem esta conexão pode ser partilhada: membros ativos da mesma
    empresa do dono, sem contar com o próprio dono nem com quem já tem acesso.
    """
    conn = get_connection_or_404(db, conn_id)
    acesso = assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    ja_com_acesso = {s.user_id for s in acesso.shares}
    ja_com_acesso.add(conn.user_id)

    query = db.query(User).filter(User.is_active.is_(True))

    dono = conn.owner
    if dono is not None and dono.empresa_id is not None:
        query = query.filter(User.empresa_id == dono.empresa_id)
    elif not is_superadmin(actor):
        query = query.filter(User.empresa_id == actor.empresa_id)

    candidatos = query.order_by(User.nome).all()

    return [
        {
            "id": u.id,
            "nome": u.nome,
            "apelido": u.apelido,
            "email": u.email,
        }
        for u in candidatos
        if u.id not in ja_com_acesso
    ]


@router.post(
    "/connections/{conn_id}/shares",
    response_model=ConnectionShareOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_connection_share(
    conn_id: int,
    data: ConnectionShareCreate,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Dá acesso a outro utilizador (ou atualiza o nível, se já tiver)."""
    resultado = share_connection(db, conn_id, actor, data.user_id, data.access_level)
    _invalidate_connections_cache()
    return resultado


@router.patch(
    "/connections/{conn_id}/shares/{target_user_id}",
    response_model=ConnectionShareOut,
)
async def update_connection_share(
    conn_id: int,
    target_user_id: int,
    data: ConnectionShareUpdate,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Altera o nível de acesso de quem já tem partilha."""
    return share_connection(db, conn_id, actor, target_user_id, data.access_level)


@router.delete(
    "/connections/{conn_id}/shares/{target_user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_connection_share(
    conn_id: int,
    target_user_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Retira o acesso de um utilizador a esta conexão."""
    revoke_connection_share(db, conn_id, actor, target_user_id)
    _invalidate_connections_cache()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
