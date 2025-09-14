from fastapi import APIRouter, Depends
from typing import List
from sqlalchemy.orm import Session
from app.schemas.task_schema import ProjectSchema, TaskSchema
from app.database import get_db
from app.ultils.get_id_by_token import get_current_user_id
from app.services import project_service, task_service

router = APIRouter()

# ----------------------------- PROJETOS -----------------------------
@router.get("/projects/", response_model=List[ProjectSchema])
def list_projects(db: Session = Depends(get_db)):
    print("Listando projetos...")
    return project_service.list_projects_service(db)

@router.get("/projects/{project_id}", response_model=ProjectSchema)
def retrieve_project(project_id: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return project_service.retrieve_project_service(db, project_id)

@router.post("/projects/", response_model=ProjectSchema)
def create_new_project(project: ProjectSchema, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return project_service.create_project_service(db, project, user_id)

@router.put("/projects/{project_id}", response_model=ProjectSchema)
def update_existing_project(project_id: str, project: ProjectSchema, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return project_service.update_project_service(db, project_id, project)

@router.delete("/projects/{project_id}")
def delete_existing_project(project_id: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return project_service.delete_project_service(db, project_id)

# ----------------------------- TAREFAS -----------------------------
@router.get("/projects/{project_id}/tasks/", response_model=List[TaskSchema])
def list_project_tasks(project_id: str, db: Session = Depends(get_db)):
    return task_service.list_tasks_service(db, project_id)

@router.post("/projects/{project_id}/tasks/", response_model=TaskSchema)
def add_new_task(project_id: str, task: TaskSchema, db: Session = Depends(get_db)):
    return task_service.add_task_service(db, project_id, task)

@router.put("/projects/{project_id}/tasks/{task_id}", response_model=TaskSchema)
def update_existing_task(project_id: str, task_id: str, task: TaskSchema, db: Session = Depends(get_db)):
    return task_service.update_task_service(db, project_id, task_id, task)

@router.delete("/projects/{project_id}/tasks/{task_id}")
def delete_existing_task(project_id: str, task_id: str, db: Session = Depends(get_db)):
    return task_service.delete_task_service(db, project_id, task_id)
