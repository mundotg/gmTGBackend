from app.models.connection_models import ActiveConnection, ConnectionLog, DBConnection
from app.models.dbstatistics_models import DBStatistics
from app.models.geral_model import Settings
from app.models.queryhistory_models import QueryHistory
from app.models.user_model import User,RefreshToken,Role,Permission,Empresa,Cargo,roles_permissions
from app.models.dbstructure_models import DBField, DBStructure,DBEnumField
from app.models.task_models import  Task, Sprint,TaskStats,TypeProjecto,Project,project_team_association,AuditLog
from app.models.ai_models import ChatSession, Message, UsageLog, Feedback,chat_sessions

