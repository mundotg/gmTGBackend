from typing import List
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.cruds.task_cruds import (
    get_projects, get_project, create_project, update_project, delete_project
)
from app.schemas.task_schema import ProjectSchema

def list_projects_service(db: Session) -> List[ProjectSchema]:
    return get_projects(db)

def retrieve_project_service(db: Session, project_id: str) -> ProjectSchema:
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Projeto não encontrado")
    return project

def create_project_service(db: Session, project: ProjectSchema, user_id: int) -> ProjectSchema:
    
    return create_project(db, project)

def update_project_service(db: Session, project_id: str, project: ProjectSchema) -> ProjectSchema:
    updated = update_project(db, project_id, project)
    if not updated:
        raise HTTPException(status_code=404, detail="Projeto não encontrado")
    return updated

def delete_project_service(db: Session, project_id: str) -> dict:
    success = delete_project(db, project_id)
    if not success:
        raise HTTPException(status_code=404, detail="Projeto não encontrado")
    return {"detail": "Projeto deletado"}
