### ✅ `schemas/geral.py`
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal, Optional
from datetime import datetime


# === Settings Schemas ===
class SettingsBase(BaseModel):
    theme: Optional[str] = Field(default="light", pattern="^(light|dark)$")
    language: Optional[str] = Field(default="pt", min_length=2, max_length=5)
    sidebar_collapsed: Optional[bool] = False
    preferred_db_type: Optional[str] = None

    class Config:
        json_schema_extra  = {
            "example": {
                "theme": "light",
                "language": "pt",
                "sidebar_collapsed": False,
                "preferred_db_type": "PostgreSQL"
            }
        }

class SettingsCreate(SettingsBase):
    pass

class Settings(SettingsBase):
    id: int
    user_id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True) 
    
type OptionTipoModel =Literal["user", "project", "task", "sprint", "type_project", "Role", "project_team_association", "AuditLog", "TaskStats", "DBConnection"]
