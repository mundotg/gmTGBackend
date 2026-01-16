import traceback
from fastapi import APIRouter, Depends, HTTPException
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.config.cache_manager import cache_result
from app.schemas.project_schemas import ProjectResponseSchema, ProjectSchema
from app.database import get_db
from app.ultils.get_id_by_token import get_current_user_id
from app.services import project_service
from app.ultils.logger import log_message

router = APIRouter()

# ----------------------------- #
# Endpoints de Projetos         #
# ----------------------------- #

def handle_service_error(context: str, error: Exception, status_code: int = 500):
    """Função utilitária para logar erros e retornar HTTPException."""
    error_trace = traceback.format_exc()
    log_message(f"❌ Erro em {context}: {error}\n{error_trace}", level="error")

    if isinstance(error, HTTPException):
        raise error  # já tratado
    elif isinstance(error, SQLAlchemyError):
        raise HTTPException(status_code=500, detail="Erro de banco de dados")
    else:
        raise HTTPException(status_code=status_code, detail=f"Erro interno em {context}")

@cache_result(ttl=300, user_id="user_{user_id}")
def list_projects_cached(db: Session, user_id: int = Depends(get_current_user_id)):
    """Lista projetos com cache."""
    return project_service.list_projects_service(db)

@cache_result(ttl=600, user_id="user_{user_id}")
def retrieve_project_cached(db: Session, project_id: str, user_id: int = Depends(get_current_user_id)):
    """Obtém projeto específico com cache."""
    return project_service.retrieve_project_service(db, project_id)



@router.get("/projects/", response_model=List[ProjectResponseSchema])
def list_projects(db: Session = Depends(get_db), user_id: str = Depends(get_current_user_id)):
    """Lista todos os projetos do usuário."""
    try:
        return list_projects_cached(db, user_id)
    except Exception as e:
        handle_service_error("listar projetos", e)

@router.get("/projects/{project_id}", response_model=ProjectResponseSchema)
def retrieve_project(project_id: str, db: Session = Depends(get_db), user_id: str = Depends(get_current_user_id)):
    """Obtém projeto por ID."""
    try:
        return retrieve_project_cached(db, project_id, user_id)
    except Exception as e:
        handle_service_error(f"buscar projeto {project_id}", e)

@router.post("/projects/", response_model=ProjectResponseSchema)
def create_new_project(project: ProjectSchema, db: Session = Depends(get_db), 
                       user: str = Depends(get_current_user_id)):
    """Cria um novo projeto e limpa o cache."""
    try:
        result = project_service.create_project_service(db, project, user)
        # _clear_projects_cache(user_id)
        log_message(f"✅ Projeto criado: {project.name}", level="info")
        return result
    except Exception as e:
        handle_service_error("criar projeto", e)

@router.put("/projects/{project_id}", response_model=ProjectResponseSchema)
def update_existing_project(project_id: str, project: ProjectSchema, db: Session = Depends(get_db), 
                            user_id: int = Depends(get_current_user_id)):
    """Atualiza projeto existente."""
    try:
        result = project_service.update_project_service(db, project_id, project)
        # _clear_all_project_cache(user_id, project_id)
        log_message(f"✅ Projeto atualizado: {project_id}", level="info")
        return result
    except Exception as e:
        handle_service_error(f"atualizar projeto {project_id}", e)

@router.delete("/projects/{project_id}")
def delete_existing_project(project_id: str, db: Session = Depends(get_db), 
                            user: str = Depends(get_current_user_id)):
    """Deleta projeto e limpa cache relacionado."""
    try:
        result = project_service.delete_project_service(db, project_id)
        # _clear_all_project_cache(user_id, project_id)
        log_message(f"✅ Projeto deletado: {project_id}", level="info")
        return result
    except Exception as e:
        handle_service_error(f"deletar projeto {project_id}", e)

