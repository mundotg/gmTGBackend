"""
🧠 LineageService — Motor de Rastreamento e Grafo de Linhagem de Dados (Data Lineage & Flow)

Constrói e analisa o Directed Acyclic Graph (DAG) do fluxo de dados entre bancos heterogêneos
(PostgreSQL, MySQL, MongoDB, SQLite, etc.), mapeando proveniência (upstream), transformações
e consumidores (downstream).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models.connection_models import DBConnection
from app.models.lineage_models import DataLineageEvent
from app.models.queryhistory_models import QueryHistory
from app.schemas.lineage_schemas import (
    LineageEdge,
    LineageGraphResponse,
    LineageNode,
    LineageSummary,
    TableImpactAnalysisResponse,
)
from app.ultils.logger import log_message


class LineageService:
    """Serviço de governança e linhagem visual de dados."""

    @staticmethod
    def record_event_sync(
        db: Session,
        *,
        user_id: Optional[int],
        source_conn_id: Optional[int],
        source_table: str,
        target_conn_id: Optional[int],
        target_table: str,
        source_query: Optional[str] = None,
        columns_mapping: Optional[List[Dict[str, Any]]] = None,
        transformation_type: str = "direct_transfer",
        transformation_rule: Optional[str] = None,
        rows_transferred: int = 0,
        duration_ms: int = 0,
        status: str = "success",
        error_message: Optional[str] = None,
        project_id: Optional[int] = None,
    ) -> Optional[DataLineageEvent]:
        """Registra um evento de linhagem de forma síncrona com tratamento fail-safe."""
        try:
            # Obtém metadados das conexões se disponíveis
            src_name, src_type = None, None
            dst_name, dst_type = None, None

            if source_conn_id:
                src_conn = db.query(DBConnection).filter(DBConnection.id == source_conn_id).first()
                if src_conn:
                    src_name = src_conn.name_connection
                    src_type = str(src_conn.type or "").lower()

            if target_conn_id:
                dst_conn = db.query(DBConnection).filter(DBConnection.id == target_conn_id).first()
                if dst_conn:
                    dst_name = dst_conn.name_connection
                    dst_type = str(dst_conn.type or "").lower()

            event = DataLineageEvent(
                source_connection_id=source_conn_id,
                source_connection_name=src_name or f"Conn #{source_conn_id}",
                source_db_type=src_type or "sql",
                source_table=source_table,
                source_query=source_query,
                target_connection_id=target_conn_id,
                target_connection_name=dst_name or f"Conn #{target_conn_id}",
                target_db_type=dst_type or "sql",
                target_table=target_table,
                columns_mapping=columns_mapping,
                transformation_type=transformation_type,
                transformation_rule=transformation_rule or (
                    "Query de Filtro / Extração" if source_query else "Mapeamento Direto"
                ),
                rows_transferred=rows_transferred,
                duration_ms=duration_ms,
                status=status,
                error_message=error_message,
                user_id=user_id,
                project_id=project_id,
                created_at=datetime.now(timezone.utc),
            )
            db.add(event)
            db.commit()
            db.refresh(event)
            log_message(
                f"[Lineage] Evento registrado: {source_table} -> {target_table} ({rows_transferred} linhas)",
                "info",
            )
            return event
        except Exception as e:
            db.rollback()
            log_message(f"[Lineage] Falha não bloqueante ao registrar evento: {e}", "warning")
            return None

    @staticmethod
    async def record_transfer_event_async(
        db: AsyncSession,
        *,
        user_id: Optional[int],
        source_conn: Optional[Any],
        target_conn: Optional[Any],
        source_table: str,
        target_table: str,
        source_query: Optional[str] = None,
        columns_mapping: Optional[List[Any]] = None,
        transformation_type: str = "direct_transfer",
        transformation_rule: Optional[str] = None,
        rows_transferred: int = 0,
        duration_ms: int = 0,
        status: str = "success",
        error_message: Optional[str] = None,
        project_id: Optional[int] = None,
    ) -> Optional[DataLineageEvent]:
        """Registra um evento de transferência assíncrono diretamente de db_transfer.py."""
        try:
            src_id = getattr(source_conn, "id", None)
            src_name = getattr(source_conn, "name_connection", None) or f"Conn #{src_id}"
            src_type = str(getattr(source_conn, "type", "") or "").lower()

            dst_id = getattr(target_conn, "id", None)
            dst_name = getattr(target_conn, "name_connection", None) or f"Conn #{dst_id}"
            dst_type = str(getattr(target_conn, "type", "") or "").lower()

            # Normaliza columns_mapping para JSON serializável
            norm_cols = []
            if columns_mapping:
                for col in columns_mapping:
                    if hasattr(col, "coluna_origem"):
                        norm_cols.append({
                            "source": col.coluna_origem,
                            "target": getattr(col, "coluna_destino", col.coluna_origem),
                            "type": getattr(col, "tipo_dado_origem", None),
                        })
                    elif isinstance(col, dict):
                        norm_cols.append(col)
                    else:
                        norm_cols.append({"source": str(col), "target": str(col)})

            rule = transformation_rule
            if not rule:
                if source_query:
                    rule = f"Query Customizada: {source_query[:100]}..."
                else:
                    rule = f"Transferência Direta ({len(norm_cols)} colunas)"

            event = DataLineageEvent(
                source_connection_id=src_id,
                source_connection_name=src_name,
                source_db_type=src_type,
                source_table=source_table,
                source_query=source_query,
                target_connection_id=dst_id,
                target_connection_name=dst_name,
                target_db_type=dst_type,
                target_table=target_table,
                columns_mapping=norm_cols,
                transformation_type="query_filter" if source_query else transformation_type,
                transformation_rule=rule,
                rows_transferred=rows_transferred,
                duration_ms=duration_ms,
                status=status,
                error_message=error_message,
                user_id=user_id,
                project_id=project_id,
                created_at=datetime.now(timezone.utc),
            )
            db.add(event)
            await db.commit()
            return event
        except Exception as e:
            log_message(f"[Lineage] Aviso ao gravar linhagem assíncrona: {e}", "warning")
            return None

    @classmethod
    def get_lineage_graph(
        cls,
        db: Session,
        *,
        user_id: Optional[int] = None,
        connection_id: Optional[int] = None,
        table_name: Optional[str] = None,
        search: Optional[str] = None,
    ) -> LineageGraphResponse:
        """
        Recupera os eventos de linhagem e sintetiza o grafo completo (Nodes, Edges e Métricas)
        usando NetworkX para ordenação topológica por camadas (levels).
        """
        query = db.query(DataLineageEvent)

        # Filtros opcionais
        if user_id is not None:
            query = query.filter(
                or_(
                    DataLineageEvent.user_id == user_id,
                    DataLineageEvent.user_id.is_(None),
                )
            )

        if connection_id:
            query = query.filter(
                or_(
                    DataLineageEvent.source_connection_id == connection_id,
                    DataLineageEvent.target_connection_id == connection_id,
                )
            )

        if table_name:
            query = query.filter(
                or_(
                    DataLineageEvent.source_table == table_name,
                    DataLineageEvent.target_table == table_name,
                )
            )

        if search:
            termo = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    DataLineageEvent.source_table.ilike(termo),
                    DataLineageEvent.target_table.ilike(termo),
                    DataLineageEvent.source_connection_name.ilike(termo),
                    DataLineageEvent.target_connection_name.ilike(termo),
                    DataLineageEvent.transformation_rule.ilike(termo),
                )
            )

        events: List[DataLineageEvent] = query.order_by(desc(DataLineageEvent.created_at)).limit(100).all()

        # Se não houver eventos reais no banco ainda, gera demonstração inteligente com as conexões reais
        if not events:
            cls._seed_demo_events_if_empty(db, user_id)
            events = db.query(DataLineageEvent).order_by(desc(DataLineageEvent.created_at)).limit(100).all()

        return cls._build_graph_from_events(events)

    @classmethod
    def _seed_demo_events_if_empty(cls, db: Session, user_id: Optional[int]) -> None:
        """Gera fluxos representativos para o usuário visualizar o valor imediato da linhagem."""
        try:
            conns = db.query(DBConnection).limit(5).all()
            conn_map = {c.id: c for c in conns}

            c_pg = next((c for c in conns if "postg" in str(c.type or "").lower()), None)
            c_mongo = next((c for c in conns if "mongo" in str(c.type or "").lower()), None)
            c_mysql = next((c for c in conns if "mysql" in str(c.type or "").lower()), None)
            c_sqlite = next((c for c in conns if "sqlite" in str(c.type or "").lower()), None)

            # Fallback se não encontrar os 4 tipos
            first_c = conns[0] if conns else None
            second_c = conns[1] if len(conns) > 1 else first_c

            demo_events = [
                DataLineageEvent(
                    source_connection_id=c_pg.id if c_pg else getattr(first_c, "id", 1),
                    source_connection_name=c_pg.name_connection if c_pg else "PostgreSQL (Produção)",
                    source_db_type=c_pg.type if c_pg else "postgresql",
                    source_table="pedidos",
                    source_query="SELECT id, cliente_id, valor_total, status, data_criacao FROM pedidos WHERE status = 'pago'",
                    target_connection_id=c_mongo.id if c_mongo else getattr(second_c, "id", 20),
                    target_connection_name=c_mongo.name_connection if c_mongo else "MongoDB (DataLake Analytics)",
                    target_db_type=c_mongo.type if c_mongo else "mongodb",
                    target_table="analytics_pedidos",
                    columns_mapping=[
                        {"source": "id", "target": "pedido_id", "type": "INTEGER"},
                        {"source": "cliente_id", "target": "cliente_id", "type": "INTEGER"},
                        {"source": "valor_total", "target": "receita_liquida", "type": "NUMERIC"},
                        {"source": "data_criacao", "target": "data_processamento", "type": "TIMESTAMP"},
                    ],
                    transformation_type="query_filter",
                    transformation_rule="Filtro: status = 'pago' AND data_criacao >= '2026-01-01'",
                    rows_transferred=45230,
                    duration_ms=3120,
                    status="success",
                    user_id=user_id,
                    created_at=datetime.now(timezone.utc),
                ),
                DataLineageEvent(
                    source_connection_id=c_mysql.id if c_mysql else getattr(first_c, "id", 2),
                    source_connection_name=c_mysql.name_connection if c_mysql else "MySQL (E-Commerce)",
                    source_db_type=c_mysql.type if c_mysql else "mysql",
                    source_table="clientes",
                    source_query="SELECT id, nome, email, cidade, estado FROM clientes WHERE ativo = 1",
                    target_connection_id=c_pg.id if c_pg else getattr(first_c, "id", 1),
                    target_connection_name=c_pg.name_connection if c_pg else "PostgreSQL (Data Warehouse)",
                    target_db_type=c_pg.type if c_pg else "postgresql",
                    target_table="dim_clientes",
                    columns_mapping=[
                        {"source": "id", "target": "cliente_id", "type": "INT"},
                        {"source": "nome", "target": "nome_cliente", "type": "VARCHAR"},
                        {"source": "email", "target": "email_mascarado", "type": "VARCHAR"},
                    ],
                    transformation_type="anonymization",
                    transformation_rule="Anonimização LGPD & Limpeza de Cadastro",
                    rows_transferred=18900,
                    duration_ms=1850,
                    status="success",
                    user_id=user_id,
                    created_at=datetime.now(timezone.utc),
                ),
                DataLineageEvent(
                    source_connection_id=c_pg.id if c_pg else getattr(first_c, "id", 1),
                    source_connection_name=c_pg.name_connection if c_pg else "PostgreSQL (Produção)",
                    source_db_type=c_pg.type if c_pg else "postgresql",
                    source_table="pagamentos",
                    source_query="SELECT id, pedido_id, metodo, valor, data_pagamento FROM pagamentos",
                    target_connection_id=c_sqlite.id if c_sqlite else getattr(second_c, "id", 3),
                    target_connection_name=c_sqlite.name_connection if c_sqlite else "SQLite (Cache Local)",
                    target_db_type=c_sqlite.type if c_sqlite else "sqlite",
                    target_table="cache_financeiro_mensal",
                    columns_mapping=[
                        {"source": "metodo", "target": "metodo", "type": "TEXT"},
                        {"source": "valor", "target": "total_consolidado", "type": "REAL"},
                    ],
                    transformation_type="aggregation",
                    transformation_rule="Agregação Mensal e Fechamento Contábil",
                    rows_transferred=12400,
                    duration_ms=950,
                    status="success",
                    user_id=user_id,
                    created_at=datetime.now(timezone.utc),
                ),
            ]
            db.bulk_save_objects(demo_events)
            db.commit()
            log_message("🌱 Linhagem de demonstração semeada com sucesso.", "info")
        except Exception as e:
            db.rollback()
            log_message(f"Falha ao semear linhagem demo: {e}", "warning")

    @classmethod
    def _build_graph_from_events(cls, events: List[DataLineageEvent]) -> LineageGraphResponse:
        """Converte uma lista de eventos em nós e arestas com níveis topológicos calculados por NetworkX."""
        g = nx.DiGraph()

        table_nodes: Dict[str, LineageNode] = {}
        transform_nodes: Dict[str, LineageNode] = {}
        edges: List[LineageEdge] = []

        total_volume = 0
        total_duration = 0
        databases_set: Set[str] = set()

        for ev in events:
            total_volume += (ev.rows_transferred or 0)
            total_duration += (ev.duration_ms or 0)

            # 1. Nó de Tabela Origem
            src_node_id = f"table_{ev.source_connection_id or 0}_{ev.source_table}"
            if src_node_id not in table_nodes:
                db_name = ev.source_connection_name or "Banco Origem"
                databases_set.add(db_name)
                table_nodes[src_node_id] = LineageNode(
                    id=src_node_id,
                    type="table",
                    label=ev.source_table,
                    database_name=db_name,
                    db_type=ev.source_db_type or "sql",
                    table_name=ev.source_table,
                    connection_id=ev.source_connection_id,
                    columns_count=len(ev.columns_mapping or []),
                    columns=[
                        c.get("source", "")
                        for c in (ev.columns_mapping or [])
                        if isinstance(c, dict)
                    ][:6],
                    level=0,
                    metadata={"total_flows_origin": 0},
                )
                g.add_node(src_node_id, role="source")

            table_nodes[src_node_id].metadata["total_flows_origin"] = (
                table_nodes[src_node_id].metadata.get("total_flows_origin", 0) + 1
            )

            # 2. Nó de Transformação Intermediário
            tx_node_id = f"tx_{ev.id}"
            tx_label = ev.transformation_rule or (
                "Query de Filtro" if ev.source_query else "Transferência Direta"
            )
            if len(tx_label) > 40:
                tx_label = tx_label[:37] + "..."

            transform_nodes[tx_node_id] = LineageNode(
                id=tx_node_id,
                type="transformation",
                label=tx_label,
                database_name="MustaInf Pipeline Engine",
                db_type="pipeline",
                table_name=None,
                connection_id=None,
                level=1,
                metadata={
                    "event_id": ev.id,
                    "transformation_type": ev.transformation_type,
                    "query": ev.source_query,
                    "rule": ev.transformation_rule,
                    "rows_transferred": ev.rows_transferred,
                    "duration_ms": ev.duration_ms,
                    "status": ev.status,
                    "created_at": ev.created_at.isoformat() if ev.created_at else None,
                },
            )
            g.add_node(tx_node_id, role="transformation")

            # 3. Nó de Tabela Destino
            dst_node_id = f"table_{ev.target_connection_id or 0}_{ev.target_table}"
            if dst_node_id not in table_nodes:
                db_name = ev.target_connection_name or "Banco Destino"
                databases_set.add(db_name)
                table_nodes[dst_node_id] = LineageNode(
                    id=dst_node_id,
                    type="table",
                    label=ev.target_table,
                    database_name=db_name,
                    db_type=ev.target_db_type or "sql",
                    table_name=ev.target_table,
                    connection_id=ev.target_connection_id,
                    columns_count=len(ev.columns_mapping or []),
                    columns=[
                        c.get("target", "")
                        for c in (ev.columns_mapping or [])
                        if isinstance(c, dict)
                    ][:6],
                    level=2,
                    metadata={"total_flows_dest": 0},
                )
                g.add_node(dst_node_id, role="target")

            table_nodes[dst_node_id].metadata["total_flows_dest"] = (
                table_nodes[dst_node_id].metadata.get("total_flows_dest", 0) + 1
            )

            # 4. Arestas: Origem -> Transformação -> Destino
            edge_extract_id = f"edge_{src_node_id}_to_{tx_node_id}"
            edges.append(
                LineageEdge(
                    id=edge_extract_id,
                    source=src_node_id,
                    target=tx_node_id,
                    label="Extração",
                    rows=ev.rows_transferred,
                    duration_ms=ev.duration_ms,
                    query=ev.source_query,
                    transformation_type=ev.transformation_type,
                    status=ev.status,
                    timestamp=ev.created_at.isoformat() if ev.created_at else None,
                )
            )
            g.add_edge(src_node_id, tx_node_id)

            edge_load_id = f"edge_{tx_node_id}_to_{dst_node_id}"
            edges.append(
                LineageEdge(
                    id=edge_load_id,
                    source=tx_node_id,
                    target=dst_node_id,
                    label=f"Carga ({ev.rows_transferred:,} lins)",
                    rows=ev.rows_transferred,
                    duration_ms=ev.duration_ms,
                    query=ev.source_query,
                    transformation_type=ev.transformation_type,
                    status=ev.status,
                    timestamp=ev.created_at.isoformat() if ev.created_at else None,
                )
            )
            g.add_edge(tx_node_id, dst_node_id)

        # Ajuste inteligente de níveis (Topological Generations com fallback)
        try:
            if nx.is_directed_acyclic_graph(g):
                topo_layers = list(nx.topological_generations(g))
                for layer_idx, layer_nodes in enumerate(topo_layers):
                    for nid in layer_nodes:
                        if nid in table_nodes:
                            table_nodes[nid].level = layer_idx
                        elif nid in transform_nodes:
                            transform_nodes[nid].level = layer_idx
        except Exception as e:
            log_message(f"Aviso topológico em grafo de linhagem: {e}", "info")

        all_nodes = list(table_nodes.values()) + list(transform_nodes.values())
        avg_dur = (total_duration / len(events)) if events else 0.0

        summary = LineageSummary(
            total_sources=len(table_nodes),
            total_targets=len([n for n in table_nodes.values() if n.level > 0]),
            total_transformations=len(transform_nodes),
            total_volume_records=total_volume,
            databases_involved=sorted(list(databases_set)),
            active_flows_count=len(events),
            average_duration_ms=round(avg_dur, 1),
        )

        return LineageGraphResponse(nodes=all_nodes, edges=edges, summary=summary)

    @classmethod
    def get_table_impact(
        cls,
        db: Session,
        table_name: str,
        connection_id: Optional[int] = None,
    ) -> TableImpactAnalysisResponse:
        """
        Analisa a árvore de dependência de uma tabela específica:
        - Upstream: De onde os dados vieram (quais tabelas alimentam esta).
        - Downstream: Quem consome esses dados (se esta tabela for alterada, quem é impactado).
        """
        query = db.query(DataLineageEvent)
        if connection_id:
            query = query.filter(
                or_(
                    DataLineageEvent.source_connection_id == connection_id,
                    DataLineageEvent.target_connection_id == connection_id,
                )
            )

        events = query.all()

        upstream = []
        downstream = []
        db_name = None

        for ev in events:
            if ev.target_table.lower() == table_name.lower():
                # Esta tabela é destino, logo a origem é UPSTREAM
                db_name = ev.target_connection_name
                upstream.append({
                    "source_table": ev.source_table,
                    "source_database": ev.source_connection_name,
                    "db_type": ev.source_db_type,
                    "query": ev.source_query,
                    "transformation": ev.transformation_rule,
                    "last_rows": ev.rows_transferred,
                    "last_updated": ev.created_at.isoformat() if ev.created_at else None,
                })

            if ev.source_table.lower() == table_name.lower():
                # Esta tabela é origem, logo o destino é DOWNSTREAM
                if not db_name:
                    db_name = ev.source_connection_name
                downstream.append({
                    "target_table": ev.target_table,
                    "target_database": ev.target_connection_name,
                    "db_type": ev.target_db_type,
                    "transformation": ev.transformation_rule,
                    "last_rows": ev.rows_transferred,
                    "last_updated": ev.created_at.isoformat() if ev.created_at else None,
                })

        count_dep = len(downstream)
        if count_dep == 0:
            risk = "LOW"
            explanation = "Esta tabela não possui nenhum fluxo downstream cadastrado. Pode ser alterada sem risco imediato para outros bancos."
        elif count_dep <= 2:
            risk = "MEDIUM"
            explanation = f"Alterações de esquema ou exclusão de dados nesta tabela afetarão {count_dep} tabela(s) downstream."
        elif count_dep <= 5:
            risk = "HIGH"
            explanation = f"Tabela crítica! É consumida por {count_dep} tabelas downstream em bancos analíticos ou de relatório."
        else:
            risk = "CRITICAL"
            explanation = f"Tabela de impacto severo! Mais de {count_dep} tabelas e pipelines corporativos dependem diretamente desta entidade."

        return TableImpactAnalysisResponse(
            table_name=table_name,
            connection_id=connection_id,
            database_name=db_name or "Banco de Dados",
            upstream_sources=upstream,
            downstream_dependents=downstream,
            total_dependent_tables=count_dep,
            risk_assessment=risk,
            risk_explanation=explanation,
        )
