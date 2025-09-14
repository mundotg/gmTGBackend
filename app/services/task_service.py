from typing import List
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.cruds.task_cruds import get_tasks, add_task, update_task, delete_task
from app.schemas.task_schema import TaskSchema

def list_tasks_service(db: Session, project_id: str) -> List[TaskSchema]:
    tasks = get_tasks(db, project_id)
    if tasks is None:
        raise HTTPException(status_code=404, detail="Projeto não encontrado")
    return tasks

def add_task_service(db: Session, project_id: str, task: TaskSchema) -> TaskSchema:
    new_task = add_task(db, project_id, task)
    if not new_task:
        raise HTTPException(status_code=404, detail="Projeto não encontrado")
    return new_task

def update_task_service(db: Session, project_id: str, task_id: str, task: TaskSchema) -> TaskSchema:
    updated_task = update_task(db, project_id, task_id, task)
    if not updated_task:
        raise HTTPException(status_code=404, detail="Tarefa ou projeto não encontrado")
    return updated_task

def delete_task_service(db: Session, project_id: str, task_id: str) -> dict:
    success = delete_task(db, project_id, task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Tarefa ou projeto não encontrado")
    return {"detail": "Tarefa deletada"}
