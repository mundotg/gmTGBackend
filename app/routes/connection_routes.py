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
    add_connection_empresa,
    assert_connection_access,
    create_connection_log,
    create_connection_role,
    create_db_connection,
    delete_connection,
    delete_connection_role,
    desactivate_all_connections,
    disconnect_active_connection,
    get_active_connection_by_connid,
    get_active_connection_by_userid,
    get_connection_or_404,
    get_db_connection_by_id,
    get_db_connections_pagination_v1,
    list_connection_available_permissions,
    list_connection_empresas,
    list_connection_roles,
    list_connection_shares,
    list_shareable_empresas,
    list_shareable_users,
    map_status,
    query_connections_simple,
    remove_connection_empresa,
    revoke_connection_share,
    set_active_connection,
    share_connection,
    update_connection_empresa,
    update_connection_role,
    upsert_db_connection,
)
from app.database import get_db
from app.models.connection_models import DBConnection
from app.models.user_model import User
from app.schemas.connetion_schema import (
    ConnectionAccessLevel,
    ConnectionAccessOut,
    ConnectionEmpresaCreate,
    ConnectionEmpresaOut,
    ConnectionEmpresaUpdate,
    ConnectionPaginationOutput,
    ConnectionPassUserOut,
    ConnectionRequest,
    ConnectionRoleCreate,
    ConnectionRoleOut,
    ConnectionRolePermissionOut,
    ConnectionRoleUpdate,
    ConnectionShareCreate,
    ConnectionShareOut,
    ConnectionShareUpdate,
    DBConnectionBase,
    DbConnectionOutput,
    SavedConnectionBase,
)
from app.services.crypto_utils import secret_encrypt, to_wire
from app.services.dataset_service import (
    download_kaggle_dataset,
    list_kaggle_dataset_files,
    read_dataframe,
    read_dataset_source,
    save_dataframe_to_sqlite,
)
from app.ultils.conect_database import close_engine
from app.ultils.connection_access import assert_connection_level, assert_user_connection_level, load_actor
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


def _ensure_connection_exists(
    conn: Optional[DBConnection], conn_id: int
) -> DBConnection:
    """
    Garante que a conexão existe. NÃO verifica quem a pode usar.

    Chamava-se `_validate_connection_owner`, o que sugeria uma verificação de
    dono que nunca existiu: `get_db_connection_by_id` procura só por id, sem
    filtrar por utilizador. Quem chama isto tem de fazer o controlo de acesso
    à parte, com `assert_user_connection_level`.
    """
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conexão com ID {conn_id} não encontrada.",
        )
    return conn


def _invalidate_connections_cache() -> None:
    """
    Descarta o cache da listagem de conexões e candidatos a partilha.
    """
    try:
        clear_cache(f"{CACHE_PREFIX}get_db_connections_pagination_cached:*")
        clear_cache(f"{CACHE_PREFIX}get_shareable_users_cached:*")
        clear_cache(f"{CACHE_PREFIX}get_shareable_empresas_cached:*")
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
        object.__setattr__(conn_data, "_user_id", user_id)
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
        # Antes de tudo: esta conexão é sequer do utilizador, ou partilhada com
        # ele? Sem esta verificação, `conn_id` era aceite em cru — qualquer
        # utilizador autenticado ativava a conexão de outro (o helper por baixo
        # procura só por id) e ficava com a engine dessa base em cache no seu
        # próprio `user_id`, que rotas como `dbInfo` e `database_inspector`
        # depois usam sem reconfirmar de quem é.
        assert_user_connection_level(
            db, get_connection_or_404(db, conn_id), user_id, ConnectionAccessLevel.read
        )

        active_conn = get_active_connection_by_connid(db, conn_id, user_id)

        # 1. LÓGICA DE DESCONEXÃO
        if active_conn and active_conn.status:
            _cleanup_engine(user_id)

            # CORRIGIDO: Acesso via ponto (objeto) em vez de colchetes
            disconnect_active_connection(db, active_conn.connection_id, user_id)

            conn_data = _ensure_connection_exists(
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
        conn_data = _ensure_connection_exists(
            get_db_connection_by_id(db, conn_id), conn_id
        )

        desactivate_all_connections(db, user_id)

        # MELHORIA: Rodar em threadpool para não travar o loop se a rede demorar
        from fastapi.concurrency import run_in_threadpool

        object.__setattr__(conn_data, "_user_id", user_id)
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
        object.__setattr__(conn_data, "_user_id", user_id)
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
    max_rows: Optional[int] = Form(None, description="Limite de linhas para amostragem"),
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
        # 1. Lê os dados do ficheiro/url de forma segura para memória
        source_data, filename, source_type = await read_dataset_source(
            file=file, url=url, max_rows=max_rows
        )

        # 2. Transforma num dicionário de DataFrames (suporta múltiplas abas/tabelas) com suporte a max_rows
        dict_dfs = read_dataframe(source_data, filename, max_rows=max_rows)

        # 3. Guarda tudo no SQLite e obtém o caminho e as tabelas criadas
        db_path, tabelas_criadas = save_dataframe_to_sqlite(
            dict_dfs=dict_dfs,
            user_id=user_id,
            original_filename=filename,
        )

        # 4. Registar a conexão na base de dados principal (o MustaInfo)
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

        create_connection_log(
            db,
            connection_id=conn.id if conn else None,
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

        create_connection_log(
            db,
            connection_id=conn.id if conn else None,
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
            detail=f"Erro interno ao processar o dataset: {str(e)}",
        )


@router.get("/dataset/kaggle/files")
async def get_kaggle_dataset_files_endpoint(
    dataset_handle: str = Query(..., description="Handle ou URL do Kaggle"),
    kaggle_username: Optional[str] = Query(None),
    kaggle_key: Optional[str] = Query(None),
    user_id: int = Depends(get_current_user_id),
):
    """
    Lista arquivos disponíveis em um dataset do Kaggle.
    """
    files = list_kaggle_dataset_files(
        dataset_handle=dataset_handle,
        username=kaggle_username,
        key=kaggle_key,
    )
    return {"success": True, "files": files}


@router.post("/dataset/kaggle")
async def open_kaggle_dataset(
    dataset_handle: str = Form(..., description="Handle (owner/dataset) ou URL do Kaggle"),
    file_path: Optional[str] = Form(None, description="Arquivo específico dentro do dataset"),
    max_rows: Optional[int] = Form(50000, description="Limite máximo de linhas para amostragem"),
    kaggle_username: Optional[str] = Form(None),
    kaggle_key: Optional[str] = Form(None),
    save_credentials: bool = Form(False),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Descarrega um dataset do Kaggle (ou arquivo específico dentro dele),
    converte para base de dados SQLite e regista como conexão do utilizador.
    """
    conn = None
    db_path: Optional[str] = None
    filename = "kaggle_dataset"

    try:
        dict_dfs, filename = download_kaggle_dataset(
            dataset_handle=dataset_handle,
            file_path=file_path,
            max_rows=max_rows,
            username=kaggle_username,
            key=kaggle_key,
            save_to_env=save_credentials,
        )

        db_path, tabelas_criadas = save_dataframe_to_sqlite(
            dict_dfs=dict_dfs,
            user_id=user_id,
            original_filename=filename,
        )

        main_table_name = tabelas_criadas[0] if tabelas_criadas else "main_table"

        conn_data = DBConnectionBase(
            name=f"Kaggle: {filename}",
            type="SQLite",
            host=secret_encrypt(db_path),
            port=0,
            username="",
            password="",
            database_name=main_table_name,
            status="available",
        )

        conn = create_db_connection(db, user_id, conn_data)

        total_rows_all_tables = sum(len(df) for df in dict_dfs.values())

        tabelas_info = []
        for tab_name in tabelas_criadas:
            df = dict_dfs[tab_name]
            tabelas_info.append(
                {
                    "table_name": tab_name,
                    "rows": int(len(df)),
                    "columns": int(len(df.columns)),
                    "column_names": list(df.columns),
                    "preview": df.head(3)
                    .where(pd.notna(df.head(3)), None)
                    .to_dict(orient="records"),
                }
            )

        log_message(
            f"📄 Dataset Kaggle '{filename}' importado pelo utilizador {user_id}. "
            f"Tabelas: {len(tabelas_criadas)}. Linhas: {total_rows_all_tables}.",
            level="info",
        )

        create_connection_log(
            db,
            connection_id=conn.id,
            action="Dataset Kaggle importado e convertido para SQLite",
            status="success",
            details={
                "source": "kaggle",
                "handle": dataset_handle,
                "file": file_path,
                "max_rows": max_rows,
                "host": db_path,
                "tables_created": tabelas_criadas,
                "total_rows_processed": total_rows_all_tables,
            },
            user_id=user_id,
        )

        return {
            "success": True,
            "source": "kaggle",
            "filename": filename,
            "connection_id": conn.id,
            "tables": tabelas_info,
            "total_tables_extracted": len(tabelas_criadas),
            "message": f"Dataset do Kaggle '{filename}' carregado com sucesso. {len(tabelas_criadas)} tabela(s) criada(s).",
        }

    except HTTPException as e:
        log_message(
            f"⚠️ Falha ao abrir dataset Kaggle '{filename}' para utilizador {user_id}: {e.detail}",
            level="warning",
        )
        create_connection_log(
            db,
            connection_id=conn.id if conn else None,
            action="Falha ao importar dataset Kaggle",
            status="error",
            details={"error": e.detail, "filename": filename},
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
            f"❌ Erro interno ao abrir dataset Kaggle '{filename}' para utilizador {user_id}: "
            f"{str(e)}\n{traceback.format_exc()}",
            level="error",
        )
        create_connection_log(
            db,
            connection_id=conn.id if conn else None,
            action="Erro crítico no processamento do dataset Kaggle",
            status="error",
            details={"error": str(e), "filename": filename},
            user_id=user_id,
        )
        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao processar o dataset do Kaggle: {str(e)}",
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


@cache_result(ttl=120, user_id="shareable_users")
async def get_shareable_users_cached(
    conn_id: int, user_id: int, *, db: Session
) -> list[dict]:
    actor = load_actor(db, user_id)
    return list_shareable_users(db, conn_id, actor)


@cache_result(ttl=120, user_id="shareable_empresas")
async def get_shareable_empresas_cached(
    conn_id: int, user_id: int, *, db: Session
) -> list[dict]:
    actor = load_actor(db, user_id)
    return list_shareable_empresas(db, conn_id, actor)


@router.get("/connections/{conn_id}/shareable-users", response_model=list[dict])
async def list_shareable_users_endpoint(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """
    Colegas a quem esta conexão pode ser partilhada: membros ativos da mesma
    empresa do dono, sem contar com o próprio dono nem com quem já tem acesso.
    """
    return await get_shareable_users_cached(conn_id, actor.id, db=db)


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
    """Dá acesso a outro utilizador (ou atualiza o nível/role/regras, se já tiver)."""
    resultado = share_connection(
        db,
        conn_id,
        actor,
        data.user_id,
        data.access_level,
        role_id=data.role_id,
        allowed_tables=data.allowed_tables,
        blocked_tables=data.blocked_tables,
        allowed_columns=data.allowed_columns,
        blocked_columns=data.blocked_columns,
        allowed_query_types=data.allowed_query_types,
        max_rows=data.max_rows,
    )
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
    """Altera o nível de acesso, role ou regras de quem já tem partilha."""
    resultado = share_connection(
        db,
        conn_id,
        actor,
        target_user_id,
        data.access_level,
        role_id=data.role_id,
        allowed_tables=data.allowed_tables,
        blocked_tables=data.blocked_tables,
        allowed_columns=data.allowed_columns,
        blocked_columns=data.blocked_columns,
        allowed_query_types=data.allowed_query_types,
        max_rows=data.max_rows,
    )
    _invalidate_connections_cache()
    return resultado


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


# =========================================================
# 🔑 Rotas de Funções (Roles) por Conexão
# =========================================================

@router.get("/permissions/available", response_model=list[ConnectionRolePermissionOut])
async def get_connection_available_permissions(
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Catálogo de permissões granulares aplicáveis a conexões de dados."""
    return list_connection_available_permissions(db)


@router.get("/connections/{conn_id}/roles", response_model=list[ConnectionRoleOut])
async def get_connection_roles(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Lista as funções RBAC disponíveis nesta conexão específica."""
    return list_connection_roles(db, conn_id, actor)


@router.post(
    "/connections/{conn_id}/roles",
    response_model=ConnectionRoleOut,
    status_code=status.HTTP_201_CREATED,
)
async def post_connection_role(
    conn_id: int,
    data: ConnectionRoleCreate,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Cria uma nova função RBAC isolada nesta conexão."""
    return create_connection_role(db, conn_id, actor, data)


@router.patch("/connections/{conn_id}/roles/{role_id}", response_model=ConnectionRoleOut)
async def patch_connection_role(
    conn_id: int,
    role_id: int,
    data: ConnectionRoleUpdate,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Atualiza metadados, permissões ou regras de uma função da conexão."""
    return update_connection_role(db, conn_id, role_id, actor, data)


@router.delete("/connections/{conn_id}/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_connection_role(
    conn_id: int,
    role_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Remove uma função da conexão."""
    delete_connection_role(db, conn_id, role_id, actor)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# =========================================================
# 🏢🔗🔌 Rotas de Associação N:N Conexão - Empresa
# =========================================================

@router.get("/connections/{conn_id}/empresas", response_model=list[ConnectionEmpresaOut])
async def get_connection_empresas(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Lista as empresas vinculadas a esta conexão."""
    return list_connection_empresas(db, conn_id, actor)


@router.get("/connections/{conn_id}/shareable-empresas")
async def get_connection_shareable_empresas(
    conn_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Lista empresas disponíveis para vincular a esta conexão."""
    return await get_shareable_empresas_cached(conn_id, actor.id, db=db)


@router.post(
    "/connections/{conn_id}/empresas",
    response_model=ConnectionEmpresaOut,
    status_code=status.HTTP_201_CREATED,
)
@router.post(
    "/connections/{conn_id}/empresas/{empresa_id}",
    response_model=ConnectionEmpresaOut,
    status_code=status.HTTP_201_CREATED,
)
async def post_connection_empresa(
    conn_id: int,
    empresa_id: Optional[int] = None,
    data: Optional[ConnectionEmpresaCreate] = None,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Vincula uma empresa a esta conexão (relação N:N), com nível e role opcionais."""
    target_empresa_id = empresa_id or (data.empresa_id if data else None)
    if not target_empresa_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="ID da empresa é obrigatório.",
        )
    lvl = data.access_level if data else None
    r_id = data.role_id if data else None
    res = add_connection_empresa(db, conn_id, target_empresa_id, actor, access_level=lvl, role_id=r_id)
    _invalidate_connections_cache()
    return res


@router.patch("/connections/{conn_id}/empresas/{empresa_id}", response_model=ConnectionEmpresaOut)
async def patch_connection_empresa(
    conn_id: int,
    empresa_id: int,
    data: ConnectionEmpresaUpdate,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Atualiza o nível de acesso ou role de uma empresa vinculada à conexão."""
    res = update_connection_empresa(db, conn_id, empresa_id, actor, data)
    _invalidate_connections_cache()
    return res


@router.delete("/connections/{conn_id}/empresas/{empresa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection_empresa(
    conn_id: int,
    empresa_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    """Desvincula uma empresa desta conexão (relação N:N)."""
    remove_connection_empresa(db, conn_id, empresa_id, actor)
    _invalidate_connections_cache()
    return Response(status_code=status.HTTP_204_NO_CONTENT)



# =========================================================
# 🚇 MustaLink Data Connector Agent (Túnel Reverso TCP/WS)
# =========================================================
from fastapi import WebSocket, WebSocketDisconnect  # noqa: E402
from fastapi.responses import FileResponse, RedirectResponse  # noqa: E402
from app.config.dotenv import get_env  # noqa: E402
from app.services.agent_tunnel_manager import (  # noqa: E402
    DEFAULT_AGENT_ID,
    DEFAULT_AGENT_VERSION,
    DEFAULT_TARGET_DB,
    DEFAULT_TOKEN_EXPIRES_DAYS,
    AgentTunnelSession,
    agent_tunnel_registry,
    authenticate_agent_websocket,
    authenticate_agent_websocket_with_reason,
    create_agent_tunnel_token,
    get_available_releases,
    get_minio_releases_base_url,
    get_release_minio_url,
    get_release_zip_path,
    normalize_agent_id,
)

ws_router = APIRouter(prefix="/conn", tags=["connections"])


@router.post("/agent-tunnel/token")
async def generate_agent_tunnel_token(
    agent_id: str = Body("default", embed=True),
    expires_days: int = Body(DEFAULT_TOKEN_EXPIRES_DAYS, ge=1, le=365, embed=True),
    user_id: int = Depends(get_current_user_id),
):
    """
    Gera um token JWT dedicado para autenticar o `musta-link-agent` na infraestrutura local.
    """
    norm_id = normalize_agent_id(agent_id)
    token = create_agent_tunnel_token(
        user_id=user_id,
        agent_id=norm_id,
        expires_days=expires_days,
    )
    return {
        "agent_id": norm_id,
        "virtual_host": f"agent://{norm_id}",
        "expires_days": expires_days,
        "token": token,
    }


@router.post("/agent-tunnel/pairing-code")
async def generate_agent_pairing_code(
    agent_id: str = Body(DEFAULT_AGENT_ID, embed=True),
    target_db: str = Body(DEFAULT_TARGET_DB, embed=True),
    expires_days: int = Body(DEFAULT_TOKEN_EXPIRES_DAYS, ge=1, le=365, embed=True),
    user_id: int = Depends(get_current_user_id),
):
    """
    Emite um código temporário de emparelhamento (`XXXX-XXXX`, válido por 10 minutos)
    para associar um `musta-link-agent` recém-descarregado sem expor o JWT ao utilizador.
    """
    entry = agent_tunnel_registry.create_pairing_code(
        user_id=user_id,
        agent_id=agent_id,
        target_db=target_db,
        expires_days=expires_days,
    )
    return {
        "code": entry.code,
        "agent_id": entry.agent_id,
        "target_db": entry.target_db,
        "virtual_host": f"agent://{entry.agent_id}",
        "expires_at": entry.expires_at.isoformat(),
    }


@router.post("/agent-tunnel/pair")
async def pair_agent_with_code(
    code: str = Body(..., embed=True),
    agent_id: Optional[str] = Body(None, embed=True),
    target_db: Optional[str] = Body(None, embed=True),
):
    """
    Troca um código temporário de emparelhamento (`XXXX-XXXX`) por uma credencial JWT
    dedicada do agente. O código é de uso único.
    """
    result = agent_tunnel_registry.consume_pairing_code(
        raw_code=code,
        override_agent_id=agent_id,
        override_target_db=target_db,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Código de emparelhamento inválido, expirado ou já utilizado.",
        )

    log_message(
        f"🔗 [MustaLink] Agente '{result['agent_id']}' emparelhado com sucesso (user={result['user_id']}).",
        level="info",
    )
    return result


@router.get("/agent-tunnel/releases")
async def list_agent_releases(
    user_id: int = Depends(get_current_user_id),
):
    """
    Lista os pacotes nativos compilados do MustaLink Agent disponíveis para download
    (Windows, Linux, macOS · amd64 / arm64) com os respetivos checksums SHA-256 e URLs do MinIO.
    """
    releases = get_available_releases()
    return {
        "version": DEFAULT_AGENT_VERSION,
        "minio_base_url": get_minio_releases_base_url(),
        "releases": releases,
    }


@router.get("/agent-tunnel/download")
async def download_agent_package(
    os: str = Query(..., description="Sistema operativo: windows, linux ou darwin"),
    arch: str = Query("amd64", description="Arquitetura: amd64 ou arm64"),
    redirect_minio: bool = Query(False, description="Se true, redireciona diretamente para a URL do MinIO configurada no .env"),
    user_id: int = Depends(get_current_user_id),
):
    """
    Entrega o pacote `.zip` compilado do MustaLink Agent a partir do MinIO ou do disco local.
    """
    if redirect_minio:
        minio_url = get_release_minio_url(os, arch)
        if not minio_url:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pacote para {os}/{arch} não suportado.",
            )
        return RedirectResponse(url=minio_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    zip_path = get_release_zip_path(os, arch)
    if not zip_path:
        minio_url = get_release_minio_url(os, arch)
        if minio_url:
            return RedirectResponse(url=minio_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pacote para {os}/{arch} não encontrado ou não suportado.",
        )

    return FileResponse(
        path=str(zip_path),
        filename=zip_path.name,
        media_type="application/zip",
    )


@router.get("/agent-tunnel/status")
async def get_agent_tunnels_status(
    user_id: int = Depends(get_current_user_id),
):
    """
    Lista os agentes MustaLink atualmente conectados para o utilizador autenticado.
    """
    agents = agent_tunnel_registry.list_for_user(user_id)
    is_dev = (get_env("ENV", "development") or "development").lower() in ("dev", "development")
    default_relay = "ws://127.0.0.1:8000/conn/agent-tunnel" if is_dev else "wss://api.mustainfo.cloud/conn/agent-tunnel"
    relay_url = get_env("MUSTA_RELAY_WS_URL", default_relay) or default_relay
    return {
        "connected": len(agents) > 0,
        "count": len(agents),
        "agents": agents,
        "environment": "development" if is_dev else "production",
        "relay_url": relay_url,
    }


@router.delete("/agent-tunnel/{agent_id}")
async def revoke_agent_tunnel(
    agent_id: str,
    user_id: int = Depends(get_current_user_id),
):
    """
    Revoga as credenciais de um agente MustaLink e encerra imediatamente qualquer
    túnel ativo associado a `(user_id, agent_id)`.
    """
    norm_id = normalize_agent_id(agent_id)
    was_active = await agent_tunnel_registry.revoke_agent(user_id, norm_id)
    is_dev = (get_env("ENV", "development") or "development").lower() in ("dev", "development")
    log_message(
        f"🛑 [MustaLink] Agente '{norm_id}' revogado pelo utilizador #{user_id} (sessão_ativa_encerrada={was_active}). "
        f"[Ambiente: {'DESENVOLVIMENTO' if is_dev else 'PRODUÇÃO'}] "
        f"{'💡 Dica: Para re-emparelhar, gere um novo código temporário.' if is_dev else ''}",
        level="info",
    )
    return {
        "agent_id": norm_id,
        "revoked": True,
        "disconnected_active_session": was_active,
        "environment": "development" if is_dev else "production",
        "message": f"Agente '{norm_id}' revogado com sucesso. Para reconectar, gere um novo código de emparelhamento.",
    }


@ws_router.websocket("/agent-tunnel")
async def agent_reverse_tunnel_ws(websocket: WebSocket):
    """
    Endpoint WebSocket do túnel reverso multiplexado (`musta-link-agent`).
    Exige cabeçalho `Authorization: Bearer <token>` e aloca um listener TCP
    em loopback (`127.0.0.1`) isolado por sessão.
    """
    auth_info, fail_reason = authenticate_agent_websocket_with_reason(websocket)
    is_dev = (get_env("ENV", "development") or "development").lower() in ("dev", "development")

    if not auth_info:
        log_message(
            f"⛔ [MustaLink] Tentativa de conexão de agente recusada: {fail_reason}. "
            f"[Ambiente: {'DESENVOLVIMENTO' if is_dev else 'PRODUÇÃO'}]",
            level="warning",
        )
        try:
            # Envia o motivo exato de recusa no frame de fecho do WebSocket (RFC 6455)
            await websocket.close(
                code=status.WS_1008_POLICY_VIOLATION,
                reason=fail_reason[:120],
            )
        except Exception:
            pass
        return

    user_id, agent_id, jti = auth_info
    target_db = (websocket.headers.get("x-musta-target-db") or "127.0.0.1:5432").strip()[:128]
    platform = (websocket.headers.get("x-musta-agent-platform") or "").strip()[:32]
    version = (websocket.headers.get("x-musta-agent-version") or "1.0.0").strip()[:16]

    await websocket.accept()

    session = AgentTunnelSession(
        user_id=user_id,
        agent_id=agent_id,
        target_db=target_db,
        websocket=websocket,
        token_jti=jti,
        platform=platform,
        version=version,
    )

    try:
        local_port = await session.start_local_listener()
        await agent_tunnel_registry.register(session)
        log_message(
            f"✅ [MustaLink] Agente '{agent_id}' (user={user_id}, {platform}) conectado. "
            f"Relay local em 127.0.0.1:{local_port} -> {target_db} (use host 'agent://{agent_id}').",
            level="info",
        )
        await session.receive_loop()
    except WebSocketDisconnect:
        log_message(
            f"🔌 [MustaLink] Agente '{agent_id}' (user={user_id}) desconectado.",
            level="info",
        )
    except Exception as exc:
        log_message(
            f"⚠️ [MustaLink] Erro no túnel do agente '{agent_id}' (user={user_id}): {exc}",
            level="warning",
        )
    finally:
        await agent_tunnel_registry.unregister(session)
