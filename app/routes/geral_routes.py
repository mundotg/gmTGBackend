from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.cruds.dbstatistics_crud import create_statistics, get_statistics_by_connection
from app.cruds.queryhistory_crud import create_query_history
from app.database import get_db

from app.cruds.geral_crud import create_settings,get_settings
  
from app.schemas.dbstatistics_schema import DBStatisticsCreate, DBStatisticsOut
from app.schemas.geral_schema import Settings, SettingsCreate
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryOut
from app.ultils.get_id_by_token import get_current_user_id


router = APIRouter(prefix="/geral", tags=["Geral"])


# === QueryHistory ===
@router.post("/queries/", response_model=QueryHistoryOut)
def create_query(query_data: QueryHistoryCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return create_query_history(db, user_id, query_data)


# === DBStatistics ===
@router.post("/statistics/", response_model=DBStatisticsOut)
def create_statistic(stat_data: DBStatisticsCreate, db: Session = Depends(get_db)):
    return create_statistics(db, stat_data)

@router.get("/statistics/{connection_id}", response_model=DBStatisticsOut)
def get_statistic(connection_id: int, db: Session = Depends(get_db)):
    stats = get_statistics_by_connection(db, connection_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Estatísticas não encontradas")
    return stats

# === Settings ===
@router.post("/settings/", response_model=Settings)
def create_user_settings(settings_data: SettingsCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return create_settings(db, user_id, settings_data)

@router.get("/settings/", response_model=Settings)
def get_user_settings(db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    settings = get_settings(db, user_id)
    if not settings:
        raise HTTPException(status_code=404, detail="Configurações não encontradas")
    return settings
