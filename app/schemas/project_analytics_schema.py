from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class OverviewSchema(BaseModel):
    activeProjects: int
    completedTasks: int
    teamMembers: int
    overdueProjects: int
    totalProjects: int
    completedProjects: int
    totalTasks: int


class ActivitySchema(BaseModel):
    id: int
    user: Optional[str]
    action: str
    project: Optional[str]
    time: str


class ProjectProgressSchema(BaseModel):
    name: str
    progress: int
    tasks: int
    completed: int


class ProjectAnalyticsResponse(BaseModel):
    overview: OverviewSchema
    recentActivity: List[ActivitySchema]
    projectProgress: List[ProjectProgressSchema]

    # opcional (já preparado pro futuro)
    weeklyActivity: list = []
    teamPerformance: list = []
    taskStatus: list = []
    projectTypes: list = []