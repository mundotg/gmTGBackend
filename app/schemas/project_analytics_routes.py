import traceback
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import case, func

from app.database import get_db
from app.models.task_models import AuditLog, Project, Task, TaskStats
from app.routes.connection_routes import get_current_user_id
from app.schemas.project_analytics_schema import ProjectAnalyticsResponse
from app.ultils.logger import log_message

router = APIRouter(prefix="/analytics/projects", tags=["ProjectAnalytics"])


# =========================
# HELPERS
# =========================
def get_time_ago(dt: datetime) -> str:
    if not dt:
        return ""

    # 🔥 evita erro de timezone (naive vs aware)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    diff = now - dt

    if diff.days > 0:
        return f"há {diff.days}d"

    hours = diff.seconds // 3600
    if hours > 0:
        return f"há {hours}h"

    minutes = diff.seconds // 60
    return f"há {minutes}m"


# =========================
# ROUTE
# =========================
@router.get("/", response_model=ProjectAnalyticsResponse)
def get_project_analytics(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        now = datetime.now(timezone.utc)

        # =========================
        # OVERVIEW
        # =========================
        overview_data = db.query(
            func.count(Project.id).label("total_projects"),

            func.sum(case((Project.is_active == True, 1), else_=0)).label("active_projects"),
            func.sum(case((Project.is_active == False, 1), else_=0)).label("completed_projects"),

            func.sum(case(
                (
                    (Project.due_date < now) & (Project.is_active == True),
                    1
                ),
                else_=0
            )).label("overdue_projects"),

            func.count(func.distinct(Project.owner_id)).label("team_members"),
        ).one()

        # =========================
        # TASK STATS
        # =========================
        task_data = db.query(
            func.count(Task.id).label("total_tasks"),
            func.sum(case((Task.status == "completed", 1), else_=0)).label("completed_tasks"),
        ).one()

        overview = {
            "activeProjects": int(overview_data.active_projects or 0),
            "completedTasks": int(task_data.completed_tasks or 0),
            "teamMembers": int(overview_data.team_members or 0),
            "overdueProjects": int(overview_data.overdue_projects or 0),
            "totalProjects": int(overview_data.total_projects or 0),
            "completedProjects": int(overview_data.completed_projects or 0),
            "totalTasks": int(task_data.total_tasks or 0),
        }

        # =========================
        # RECENT ACTIVITY
        # =========================
        logs = (
            db.query(AuditLog)
            .order_by(AuditLog.timestamp.desc())
            .limit(5)
            .all()
        )

        recent_activity = [
            {
                "id": l.id,
                "user": str(l.user_id) if l.user_id else "Sistema",
                "action": l.action or "",
                "project": l.entity or "",
                "time": get_time_ago(l.timestamp),
            }
            for l in logs
        ]

        # =========================
        # PROJECT PROGRESS
        # =========================
        stats = (
            db.query(TaskStats)
            .join(Project, TaskStats.project_id == Project.id)
            .order_by(TaskStats.last_updated.desc())
            .limit(5)
            .all()
        )

        project_progress = [
            {
                "name": s.project.name if s.project else "Projeto",
                "progress": int(s.progress_percent or 0),
                "tasks": int(s.total or 0),
                "completed": int(s.completed or 0),
            }
            for s in stats
        ]

        # =========================
        # RESPONSE
        # =========================
        return {
            "overview": overview,
            "recentActivity": recent_activity,
            "projectProgress": project_progress,
            "weeklyActivity": [],
            "teamPerformance": [],
            "taskStatus": [],
            "projectTypes": [],
        }

    except Exception as e:
        log_message(
            f"[PROJECT_ANALYTICS] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )

        raise HTTPException(
            status_code=500,
            detail="Erro ao carregar analytics de projetos",
        )