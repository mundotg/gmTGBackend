# """
# Schemas Pydantic para Relatório de Tarefas
# app/schemas/task_schema.py
# """

# from pydantic import BaseModel, Field, validator
# from typing import Optional, List, Dict, Literal
# from datetime import datetime


# # =============================
# # 📊 SCHEMAS DE ESTATÍSTICAS
# # =============================

# class PriorityCountsSchema(BaseModel):
#     """Contadores de prioridades."""
#     critica: int = Field(default=0, ge=0, description="Número de tarefas críticas")
#     alta: int = Field(default=0, ge=0, description="Número de tarefas de alta prioridade")
#     media: int = Field(default=0, ge=0, description="Número de tarefas de média prioridade")
#     baixa: int = Field(default=0, ge=0, description="Número de tarefas de baixa prioridade")


# class TaskStatsSchema(BaseModel):
#     """
#     Estatísticas consolidadas de tarefas.
    
#     Interface correspondente ao TaskStats do TypeScript.
#     """
#     total: int = Field(..., ge=0, description="Total de tarefas")
#     completed: int = Field(..., ge=0, description="Tarefas concluídas")
#     in_progress: int = Field(..., ge=0, description="Tarefas em andamento")
#     pending: int = Field(..., ge=0, description="Tarefas pendentes")
#     inReview: int = Field(default=0, ge=0, description="Tarefas em revisão", alias="inReview")
#     blocked: int = Field(default=0, ge=0, description="Tarefas bloqueadas")
#     cancelled: int = Field(default=0, ge=0, description="Tarefas canceladas")
#     progress_percent: float = Field(..., ge=0, le=100, description="Percentual de progresso")
#     total_estimated_hours: float = Field(..., ge=0, description="Total de horas estimadas")
#     priorityCounts: PriorityCountsSchema = Field(..., description="Contadores por prioridade")
#     project_id: Optional[str] = Field(None, description="ID do projeto")
#     sprint_id: Optional[str] = Field(None, description="ID da sprint")
#     updated_at: Optional[str] = Field(None, description="Data da última atualização")

#     class Config:
#         populate_by_name = True  # Permite usar alias
#         json_schema_extra = {
#             "example": {
#                 "total": 42,
#                 "completed": 18,
#                 "in_progress": 12,
#                 "pending": 8,
#                 "inReview": 2,
#                 "blocked": 1,
#                 "cancelled": 1,
#                 "progress_percent": 42.86,
#                 "total_estimated_hours": 520,
#                 "priorityCounts": {
#                     "critica": 5,
#                     "alta": 12,
#                     "media": 18,
#                     "baixa": 7
#                 }
#             }
#         }


# # =============================
# # 👤 SCHEMAS DE USUÁRIO
# # =============================

# class UsuarioSchema(BaseModel):
#     """Schema de usuário simplificado."""
#     id: Optional[str] = None
#     nome: str = Field(..., min_length=1, description="Nome do usuário")
#     email: Optional[str] = Field(None, description="Email do usuário")
#     avatar: Optional[str] = Field(None, description="URL do avatar")

#     class Config:
#         json_schema_extra = {
#             "example": {
#                 "id": "user-001",
#                 "nome": "João Silva",
#                 "email": "joao.silva@empresa.com"
#             }
#         }


# # =============================
# # 🏃 SCHEMAS DE SPRINT
# # =============================

# class SprintSchema(BaseModel):
#     """Schema de sprint."""
#     id: Optional[str] = None
#     name: str = Field(..., min_length=1, description="Nome da sprint")
#     start_date: Optional[str] = Field(None, description="Data de início")
#     end_date: str = Field(..., description="Data de término")
#     goal: Optional[str] = Field(None, description="Objetivo da sprint")
#     is_active: bool = Field(default=False, description="Se a sprint está ativa")
#     project_id: str = Field(..., description="ID do projeto")
#     cancelled: bool = Field(default=False, description="Se a sprint foi cancelada")
#     motivo_cancelamento: Optional[str] = Field(None, description="Motivo do cancelamento")

#     class Config:
#         json_schema_extra = {
#             "example": {
#                 "id": "sprint-003",
#                 "name": "Sprint 3 - Q4 2025",
#                 "start_date": "2025-10-01",
#                 "end_date": "2025-10-31",
#                 "goal": "Implementar módulos de CRM",
#                 "is_active": True,
#                 "project_id": "proj-001"
#             }
#         }


# # =============================
# # 🎯 SCHEMAS DE PROJETO
# # =============================

# class ProjectSchema(BaseModel):
#     """Schema de projeto."""
#     id: Optional[str] = None
#     name: str = Field(..., min_length=1, description="Nome do projeto")
#     description: Optional[str] = Field(None, description="Descrição do projeto")
#     owner_id: str = Field(..., description="ID do proprietário")
#     owner: Optional[UsuarioSchema] = Field(None, description="Dados do proprietário")
#     team: Optional[List[str]] = Field(None, description="IDs dos membros da equipe")
#     type_project: Optional[str] = Field(None, description="Tipo do projeto")
#     team_members: Optional[List[UsuarioSchema]] = Field(None, description="Membros da equipe")
#     sprints: Optional[List[SprintSchema]] = Field(None, description="Sprints do projeto")
#     created_at: Optional[str] = Field(None, description="Data de criação")
#     due_date: Optional[str] = Field(None, description="Prazo final")

#     class Config:
#         json_schema_extra = {
#             "example": {
#                 "id": "proj-001",
#                 "name": "Transformação Digital 2025",
#                 "description": "Modernização da infraestrutura tecnológica",
#                 "owner_id": "user-001",
#                 "owner": {
#                     "nome": "João Silva",
#                     "email": "joao.silva@empresa.com"
#                 },
#                 "team_members": [
#                     {"nome": "João Silva"},
#                     {"nome": "Maria Santos"}
#                 ],
#                 "created_at": "2025-01-15",
#                 "due_date": "2025-12-31"
#             }
#         }


# # =============================
# # ✅ SCHEMAS DE TAREFA
# # =============================

# class TaskScheduleSchema(BaseModel):
#     """Schema de agendamento de tarefa."""
#     repeat: str = Field(default="nenhum", description="Tipo de repetição")
#     until: Optional[str] = Field(None, description="Data limite da repetição")


# class TaskSchema(BaseModel):
#     """Schema de tarefa."""
#     id: Optional[str] = None
#     title: str = Field(..., min_length=1, description="Título da tarefa")
#     description: Optional[str] = Field(None, description="Descrição da tarefa")
#     priority: Optional[Literal["critica", "alta", "media", "baixa"]] = Field(
#         None, description="Prioridade da tarefa"
#     )
#     start_date: Optional[str] = Field(None, description="Data de início")
#     end_date: str = Field(..., description="Data de término")
#     estimated_hours: Optional[float] = Field(None, ge=0, description="Horas estimadas")
#     tags: Optional[List[str]] = Field(None, description="Tags da tarefa")
#     status: Optional[Literal[
#         "pendente", "em_andamento", "concluida", "cancelada", "bloqueada", "em_revisao"
#     ]] = Field(None, description="Status da tarefa")
#     completed_at: Optional[str] = Field(None, description="Data de conclusão")
#     is_validated: Optional[bool] = Field(None, description="Se a tarefa foi validada")
#     comentario_is_validated: Optional[str] = Field(None, description="Comentário da validação")
#     schedule: Optional[TaskScheduleSchema] = Field(None, description="Agendamento")
    
#     # Relacionamentos
#     delegated_to_i