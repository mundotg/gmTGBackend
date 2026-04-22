from typing import List, Optional, Any, Dict, Type, TypeVar
from sqlalchemy import func, select, or_
from sqlalchemy.orm import selectinload, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.models.geral_model import Settings
from app.schemas.geral_schema import SettingsUpdate, UpdateAppearancePayload, UpdateLanguagePayload
from app.ultils.logger import log_message

# TypeVar para permitir que o get_paginated_query saiba qual modelo está a retornar
T = TypeVar("T")

# ==========================================
# GESTÃO DE CONFIGURAÇÕES (SETTINGS)
# ==========================================

def get_settings(db: Session, user_id: int) -> Optional[Settings]:
    """
    Busca as configurações do utilizador.
    
    Args:
        db (Session): Sessão da base de dados.
        user_id (int): ID do utilizador.
        
    Returns:
        Optional[Settings]: Objeto Settings se encontrado, senão None.
    """
    try:
        # Usar `select` moderno do SQLAlchemy 2.0 é mais rápido e claro
        stmt = select(Settings).where(Settings.user_id == user_id)
        return db.execute(stmt).scalar_one_or_none()
    except SQLAlchemyError as e:
        log_message(f"Erro ao buscar settings para o user {user_id}: {str(e)}", "error")
        return None
def update_appearance_settings_crud(db: Session, user_id: int, payload: UpdateAppearancePayload) -> Optional[Settings]:
    """
    Atualiza as preferências de aparência do utilizador (tema e estado da sidebar).
    Cria um novo registo de Settings caso o utilizador ainda não tenha um.
    
    Args:
        db (Session): Sessão da base de dados.
        user_id (int): ID do utilizador.
        payload (UpdateAppearancePayload): Payload contendo as novas preferências de aparência.
        
    Returns:
        Optional[Settings]: O objeto Settings atualizado (ou recém-criado) ou None em caso de erro.
    """
    try:
        db_settings = get_settings(db, user_id)
        
        # Se não existir, cria um novo objeto Settings associado ao utilizador
        if not db_settings:
            log_message(f"ℹ️ Criando novas configurações de aparência para o user {user_id}", "info")
            db_settings = Settings(user_id=user_id)
            db.add(db_settings)

        # Atualiza apenas os campos relacionados à aparência
        if payload.theme is not None:
            db_settings.theme = payload.theme
        if payload.sidebar_collapsed is not None:
            db_settings.sidebar_collapsed = payload.sidebar_collapsed

        # Grava as alterações na base de dados
        db.commit()
        db.refresh(db_settings)
        return db_settings
    except SQLAlchemyError as e:
        db.rollback() # É crucial fazer rollback em caso de erro na transação
        log_message(f"Erro ao atualizar/criar aparência para o user {user_id}: {str(e)}", "error")
        return None


def update_language_settings_crud(db: Session, user_id: int, payload: UpdateLanguagePayload) -> Optional[Settings]:
    """
    Atualiza apenas a preferência de idioma do utilizador.
    Cria um novo registo de Settings caso o utilizador ainda não tenha um.
    
    Args:
        db (Session): Sessão da base de dados.
        user_id (int): ID do utilizador.
        payload (UpdateLanguagePayload): Payload contendo o novo idioma.
        
    Returns:
        Optional[Settings]: O objeto Settings atualizado (ou recém-criado) ou None em caso de erro.
    """
    try:
        db_settings = get_settings(db, user_id)
        
        # Se não existir, cria um novo objeto Settings associado ao utilizador
        if not db_settings:
            log_message(f"ℹ️ Criando novas configurações de idioma para o user {user_id}", "info")
            db_settings = Settings(user_id=user_id)
            db.add(db_settings)

        # Atualiza o idioma no objeto Settings
        db_settings.language = payload.language

        # Grava as alterações na base de dados
        db.commit()
        db.refresh(db_settings)
        return db_settings
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao atualizar/criar idioma para o user {user_id}: {str(e)}", "error")
        return None
    
    
def update_settings(db: Session, user_id: int, settings_data: SettingsUpdate) -> Optional[Settings]:
    """
    Atualiza de forma parcial (PATCH) as configurações do utilizador.
    Cria um novo registo de Settings caso o utilizador ainda não tenha um.
    
    Args:
        db (Session): Sessão da base de dados.
        user_id (int): ID do utilizador dono das configurações.
        settings_data (SettingsUpdate): Schema Pydantic com os dados a atualizar.
        
    Returns:
        Optional[Settings]: O objeto atualizado (ou recém-criado) ou None em caso de erro.
    """
    try:
        # 1. Busca o registo existente
        db_settings = get_settings(db, user_id)
        is_new_record = False
        
        # 2. Se não existir, instanciamos um novo
        if not db_settings:
            log_message(f"ℹ️ Criando novas configurações gerais para o user {user_id}", "info")
            db_settings = Settings(user_id=user_id)
            db.add(db_settings)
            is_new_record = True

        # 3. Extrai apenas os valores enviados (ignora os que vieram como None no PATCH)
        update_data = settings_data.model_dump(exclude_unset=True)
        
        # Se não houver nada para atualizar e o registo já existia, não faz nada
        if not update_data and not is_new_record:
            return db_settings

        # 4. Atualiza os atributos
        for key, value in update_data.items():
            # Verificação extra de segurança para garantir que a coluna existe no modelo
            if hasattr(db_settings, key):
                setattr(db_settings, key, value)
            
        # 5. Grava na base de dados
        db.commit()
        db.refresh(db_settings)
        
        return db_settings

    except IntegrityError as e:
        db.rollback()
        log_message(f"Erro de integridade ao atualizar/criar settings do user {user_id}: {str(e)}", "error")
        return None
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro na BD ao atualizar/criar settings do user {user_id}: {str(e)}", "error")
        return None


# ==========================================
# MOTOR DE PAGINAÇÃO UNIVERSAL
# ==========================================

def get_paginated_query(
    db: Session,
    model: Type[T],
    search: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    limit: int = 10,
    relationships: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Retorna resultados paginados de qualquer modelo SQLAlchemy, 
    com suporte a eager loading, pesquisa textual e filtros exatos.
    """
    # Validações de segurança para paginação
    page = max(1, page)
    limit = max(1, min(100, limit)) # Cap máximo de 100 para evitar sobrecarga (DDoS)

    try:
        query = select(model)

        # 🔗 Carregar relações (Eager Loading para evitar N+1 Queries)
        if relationships:
            for relation in relationships:
                if hasattr(model, relation):
                    query = query.options(selectinload(getattr(model, relation)))
                else:
                    log_message(f"Relação '{relation}' não encontrada no modelo {model.__name__}", "warning")

        # ⚙️ Filtros dinâmicos (WHERE exato)
        # Aplicado antes do search porque filtros exatos usam índices de DB e reduzem o set de dados mais rápido
        if filters:
            for key, value in filters.items():
                if hasattr(model, key) and value is not None:
                    query = query.where(getattr(model, key) == value)

        # 🔍 Busca textual (LIKE)
        if search:
            # Obtém apenas as colunas de texto reais do modelo
            searchable_columns = [
                col for col in model.__table__.columns
                if hasattr(col.type, "python_type") and col.type.python_type == str
            ]
            
            if searchable_columns:
                search_term = f"%{search}%"
                or_conditions = [col.ilike(search_term) for col in searchable_columns]
                query = query.where(or_(*or_conditions))

        # 🧮 Contagem Total (Otimizada)
        # Usamos uma subquery apenas com o ID para ser super rápida na contagem, em vez de contar todos os dados carregados
        count_stmt = select(func.count()).select_from(query.with_only_columns(model.id).subquery())
        total = db.execute(count_stmt).scalar() or 0

        # 📄 Execução da query com Offset e Limit
        offset = (page - 1) * limit
        items = db.execute(query.offset(offset).limit(limit)).scalars().all()

        return {
            "items": list(items),
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit if total > 0 else 0,
        }

    except SQLAlchemyError as e:
        log_message(f"Erro ao executar consulta paginada em {model.__name__}: {str(e)}", "error")
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}