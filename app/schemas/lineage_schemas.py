"""
📐 Schemas Pydantic para Linhagem de Dados (Data Lineage & Flow)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class LineageColumnMapping(BaseModel):
    source_column: str
    target_column: str
    data_type: Optional[str] = None


class LineageEventCreate(BaseModel):
    source_connection_id: Optional[int] = None
    source_connection_name: Optional[str] = None
    source_db_type: Optional[str] = None
    source_table: str
    source_query: Optional[str] = None

    target_connection_id: Optional[int] = None
    target_connection_name: Optional[str] = None
    target_db_type: Optional[str] = None
    target_table: str

    columns_mapping: Optional[List[Dict[str, Any]]] = None
    transformation_type: Optional[str] = "direct_transfer"
    transformation_rule: Optional[str] = None
    rows_transferred: Optional[int] = 0
    duration_ms: Optional[int] = 0
    status: Optional[str] = "success"
    error_message: Optional[str] = None
    project_id: Optional[int] = None


class LineageEventResponse(BaseModel):
    id: int
    source_connection_id: Optional[int] = None
    source_connection_name: Optional[str] = None
    source_db_type: Optional[str] = None
    source_table: str
    source_query: Optional[str] = None

    target_connection_id: Optional[int] = None
    target_connection_name: Optional[str] = None
    target_db_type: Optional[str] = None
    target_table: str

    columns_mapping: Optional[List[Dict[str, Any]]] = None
    transformation_type: str = "direct_transfer"
    transformation_rule: Optional[str] = None
    rows_transferred: int = 0
    duration_ms: int = 0
    status: str = "success"
    error_message: Optional[str] = None

    user_id: Optional[int] = None
    project_id: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


class LineageNode(BaseModel):
    id: str
    type: str = Field(..., description="table | transformation | database")
    label: str
    database_name: Optional[str] = None
    db_type: Optional[str] = None  # postgresql, mysql, mongodb, sqlite, etc.
    table_name: Optional[str] = None
    columns_count: Optional[int] = 0
    columns: Optional[List[str]] = Field(default_factory=list)
    connection_id: Optional[int] = None
    level: int = Field(0, description="Nível topológico no fluxo: 0=Origem, 1=Transformação, 2=Destino")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    id: str
    source: str
    target: str
    label: Optional[str] = None
    rows: Optional[int] = 0
    duration_ms: Optional[int] = 0
    query: Optional[str] = None
    transformation_type: Optional[str] = "direct_transfer"
    status: Optional[str] = "success"
    timestamp: Optional[str] = None


class LineageSummary(BaseModel):
    total_sources: int = 0
    total_targets: int = 0
    total_transformations: int = 0
    total_volume_records: int = 0
    databases_involved: List[str] = Field(default_factory=list)
    active_flows_count: int = 0
    average_duration_ms: float = 0.0


class LineageGraphResponse(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    summary: LineageSummary


class TableImpactAnalysisResponse(BaseModel):
    table_name: str
    connection_id: Optional[int] = None
    database_name: Optional[str] = None
    upstream_sources: List[Dict[str, Any]] = Field(default_factory=list)
    downstream_dependents: List[Dict[str, Any]] = Field(default_factory=list)
    total_dependent_tables: int = 0
    risk_assessment: str = Field("LOW", description="LOW | MEDIUM | HIGH | CRITICAL")
    risk_explanation: str
