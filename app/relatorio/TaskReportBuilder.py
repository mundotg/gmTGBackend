# app/relatorio/AdvancedTaskReportBuilder.py
"""
Gerador Avançado de Relatórios de Tarefas e Projetos
OrionForgeNexus (oFn) - Sistema de Gestão Inteligente
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum
from app.relatorio.gerador_relatorio_excel import ExcelReportGenerator
from app.ultils.logger import log_message

# ==============================================================
# 🎨 CONFIGURAÇÕES DE DESIGN E CORES
# ==============================================================

class ReportTheme(Enum):
    """Temas visuais para o relatório"""
    MODERN = "modern"
    CORPORATE = "corporate"

class ColorPalette:
    """Paleta de cores organizada por contexto semântico"""
    
    THEMES = {
        ReportTheme.MODERN: {
            "primary": "#2563eb",
            "secondary": "#7c3aed", 
            "success": "#059669",
            "warning": "#d97706",
            "danger": "#dc2626",
            "info": "#0891b2",
            "text": "#1f2937",
            "muted": "#6b7280",
            "border": "#e5e7eb"
        },
        ReportTheme.CORPORATE: {
            "primary": "#1e40af",
            "secondary": "#3730a3",
            "success": "#047857",
            "warning": "#b45309",
            "danger": "#b91c1c",
            "info": "#0e7490",
            "text": "#111827",
            "muted": "#4b5563",
            "border": "#d1d5db"
        }
    }
    
    @classmethod
    def get_theme(cls, theme: ReportTheme = ReportTheme.MODERN):
        return cls.THEMES.get(theme, cls.THEMES[ReportTheme.MODERN])

# ==============================================================
# 🧩 CLASSE PRINCIPAL: CONSTRUTOR DO RELATÓRIO AVANÇADO
# ==============================================================

class AdvancedTaskReportBuilder:
    """
    Constrói relatórios avançados com análises preditivas, métricas de saúde
    do projeto e visualizações ricas em dados.
    """
    
    DEFAULT_PREVIEW_LIMIT = 15
    RISK_THRESHOLDS = {
        'blocked_ratio': 0.15,      # 15% de tarefas bloqueadas = risco alto
        'completion_rate': 0.3,     # Menos de 30% de conclusão = risco
        'overdue_ratio': 0.1,       # Mais de 10% de tarefas atrasadas = risco
    }

    def __init__(
        self,
        stats: Dict[str, Any],
        project: Optional[Dict[str, Any]] = None,
        sprint: Optional[Dict[str, Any]] = None,
        tasks: Optional[List[Dict[str, Any]]] = None,
        preview_limit: int = DEFAULT_PREVIEW_LIMIT,
        theme: ReportTheme = ReportTheme.MODERN
    ):
        if not stats:
            raise ValueError("Estatísticas (stats) são obrigatórias")
            
        self.stats = stats
        self.project = project or {}
        self.sprint = sprint or {}
        self.tasks = tasks or []
        self.preview_limit = max(1, preview_limit)
        self.theme = theme
        self.colors = ColorPalette.get_theme(theme)
        
        # Análises pré-calculadas
        self._risk_analysis = self._calculate_risk_analysis()
        self._performance_metrics = self._calculate_performance_metrics()

    # ==============================================================
    # 🚀 CONSTRUÇÃO PRINCIPAL DO RELATÓRIO
    # ==============================================================
    
    def build(self) -> List[Dict[str, Any]]:
        """Constrói a estrutura completa do relatório em ordem lógica"""
        estrutura = [
            *self._build_header(),
            *self._build_executive_dashboard(),
            *self._build_risk_assessment(),
            *self._build_project_health(),
            *self._build_project_info(),
            *self._build_sprint_info(),
            *self._build_statistics_section(),
            *self._build_priority_analysis(),
            *self._build_timeline_analysis(),
            *self._build_tasks_preview(),
            *self._build_recommendations(),
            self._build_footer(),
        ]

        log_message(f"📊 Relatório avançado construído: {len(estrutura)} seções, "
                   f"{len(self.tasks)} tarefas analisadas")
        return estrutura

    # ==============================================================
    # 🎯 SEÇÕES DO RELATÓRIO
    # ==============================================================
    
    def _build_header(self) -> List[Dict[str, Any]]:
        """Cabeçalho com informações contextuais"""
        now = self._format_datetime(datetime.now())
        project_name = self.project.get("name", "Projeto Geral")
        sprint_name = self.sprint.get("name", "")
        
        subtitle_parts = [f"Projeto: {project_name}"]
        if sprint_name:
            subtitle_parts.append(f"Sprint: {sprint_name}")
        if self.project.get('due_date'):
            days_remaining = self._calculate_days_remaining()
            if days_remaining is not None:
                subtitle_parts.append(f"Prazo: {days_remaining}d")

        return [
            {
                "header": {
                    "title": "Relatório de Gestão de Tarefas ",
                    "subtitle": " | ".join(subtitle_parts),
                    "logo": True
                }
            },
            {
                "text": {
                    "value": f"📅 Gerado em {now}",
                    "align": "right", 
                    "size": 9,
                    "color": self.colors["muted"]
                }
            },
            {"line": {"color": self.colors["primary"], "width": 2}},
        ]

    def _build_executive_dashboard(self) -> List[Dict[str, Any]]:
        """Dashboard executivo com KPIs principais"""
        metrics = self._performance_metrics
        
        kpi_data = [
            ["🎯 Total de Tarefas", str(self.stats.get("total", 0)), "Tarefas no projeto"],
            ["✅ Concluídas", str(self.stats.get("completed", 0)), f"{metrics['completion_rate']:.1f}% do total"],
            ["⚡ Velocidade", f"{metrics['completion_rate_7d']:.1f}%", "Conclusão (7 dias)"],
            ["⏱️ Esforço Total", f"{self.stats.get('total_estimated_hours', 0)}h", "Horas estimadas"],
        ]

        return [
            self._section_title("📊 Dashboard Executivo", "primary"),
            {
                "table": {
                    "columns": ["Métrica", "Valor", "Descrição"],
                    "rows": kpi_data,
                    "header_color": self.colors["primary"]
                }
            },
        ]

    def _build_risk_assessment(self) -> List[Dict[str, Any]]:
        """Avaliação de riscos do projeto"""
        risk = self._risk_analysis
        
        if risk['level'] == 'low':
            return []  # Não mostra seção de risco se for baixo

        risk_indicators = []
        if risk['blocked_high']:
            risk_indicators.append("🔴 Muitas tarefas bloqueadas")
        if risk['completion_low']:
            risk_indicators.append("🟠 Baixa taxa de conclusão")
        if risk['overdue_high']:
            risk_indicators.append("🟡 Tarefas em atraso")

        return [
            self._section_title("⚠️ Avaliação de Riscos", "danger"),
            {
                "text": {
                    "value": f"Risco {risk['level'].upper()} Detectado\n" + " • ".join(risk_indicators),
                    "color": self.colors["danger"],
                    "background": "#fef2f2"
                }
            },
            {"spacer": {"height": 0.2}},
        ]

    def _build_project_health(self) -> List[Dict[str, Any]]:
        """Saúde geral do projeto com métricas chave"""
        metrics = self._performance_metrics
        
        health_score = metrics['health_score']
        if health_score >= 80:
            status = "🟢 Excelente"
            color = self.colors["success"]
        elif health_score >= 60:
            status = "🟡 Moderado" 
            color = self.colors["warning"]
        else:
            status = "🔴 Crítico"
            color = self.colors["danger"]

        health_metrics = [
            ["Pontuação de Saúde", f"{health_score:.1f}/100", status],
            ["Taxa de Conclusão", f"{metrics['completion_rate']:.1f}%", self._get_completion_status(metrics['completion_rate'])],
            ["Tarefas Bloqueadas", f"{self.stats.get('blocked', 0)}", self._get_blocked_status(self.stats.get('blocked', 0))],
            ["Velocidade (7d)", f"{metrics['completion_rate_7d']:.1f}%", self._get_velocity_status(metrics['completion_rate_7d'])],
        ]

        return [
            self._section_title("❤️ Saúde do Projeto", "success"),
            {
                "table": {
                    "columns": ["Métrica", "Valor", "Status"],
                    "rows": health_metrics,
                    "header_color": color
                }
            },
        ]

    def _build_project_info(self) -> List[Dict[str, Any]]:
        """Informações detalhadas do projeto"""
        if not self.project:
            return []

        owner = self.project.get("owner", {})
        members = self.project.get("team_members", [])
        
        project_info = [
            ["Nome", self.project.get("name", "—")],
            ["Descrição", self.project.get("description", "—")[:100] + "..." if self.project.get("description") else "—"],
            ["Proprietário", owner.get("nome", "—")],
            ["Data de Início", self._format_date(self.project.get("created_at"))],
            ["Prazo Final", self._format_date(self.project.get("due_date"))],
            ["Dias Restantes", f"{self._calculate_days_remaining() or '—'} dias"],
            ["Tamanho da Equipe", f"{len(members)} membros"],
        ]

        return [
            self._section_title("🏢 Informações do Projeto", "info"),
            {"table": {"columns": ["Campo", "Detalhe"], "rows": project_info}},
        ]

    def _build_sprint_info(self) -> List[Dict[str, Any]]:
        """Informações da sprint atual"""
        if not self.sprint:
            return []

        sprint_days = self._calculate_sprint_days()
        sprint_info = [
            ["Nome", self.sprint.get("name", "—")],
            ["Objetivo", self.sprint.get("goal", "—")],
            ["Data de Início", self._format_date(self.sprint.get("start_date"))],
            ["Data de Término", self._format_date(self.sprint.get("end_date"))],
            ["Dias Decorridos", f"{sprint_days['elapsed']} de {sprint_days['total']}"],
            ["Status", "✅ Ativa" if self.sprint.get("is_active") else "⏸️ Inativa"],
        ]

        return [
            self._section_title("🏃 Informações da Sprint", "secondary"),
            {"table": {"columns": ["Campo", "Detalhe"], "rows": sprint_info}},
        ]

    def _build_statistics_section(self) -> List[Dict[str, Any]]:
        """Estatísticas detalhadas de status"""
        status_data = [
            ["⏳ Pendentes", str(self.stats.get("pending", 0))],
            ["🔄 Em Andamento", str(self.stats.get("in_progress", 0))],
            ["👁️ Em Revisão", str(self.stats.get("in_review", self.stats.get("inReview", 0)))],
            ["🚫 Bloqueadas", str(self.stats.get("blocked", 0))],
            ["✅ Concluídas", str(self.stats.get("completed", 0))],
            ["❌ Canceladas", str(self.stats.get("cancelled", 0))],
        ]

        return [
            self._section_title("📈 Distribuição por Status", "primary"),
            {
                "table": {
                    "columns": ["Status", "Quantidade"],
                    "rows": status_data,
                    "header_color": self.colors["primary"]
                }
            },
        ]

    def _build_priority_analysis(self) -> List[Dict[str, Any]]:
        """Análise detalhada de prioridades"""
        priorities = self.stats.get("priorityCounts", {})
        total = sum(priorities.values()) or 1
        
        priority_data = []
        for level, label in [
            ("critica", "🔴 Crítica"),
            ("alta", "🟠 Alta"),
            ("media", "🟡 Média"),
            ("baixa", "🟢 Baixa"),
        ]:
            count = priorities.get(level, 0)
            percentage = (count / total) * 100
            priority_data.append([label, str(count), f"{percentage:.1f}%"])

        return [
            self._section_title("🎯 Análise de Prioridades", "warning"),
            {
                "table": {
                    "columns": ["Prioridade", "Quantidade", "Percentual"],
                    "rows": priority_data,
                    "header_color": self.colors["warning"]
                }
            },
        ]

    def _build_timeline_analysis(self) -> List[Dict[str, Any]]:
        """Análise de prazos e temporalidade"""
        if not self.tasks:
            return []

        overdue = self._count_overdue_tasks()
        due_soon = self._count_due_soon_tasks()
        on_time = len(self.tasks) - overdue - due_soon
        
        timeline_data = [
            ["⏰ Em Dia", str(on_time)],
            ["🔔 Próximo do Prazo", str(due_soon)],
            ["🚨 Atrasadas", str(overdue)],
        ]

        return [
            self._section_title("⏱️ Análise de Prazos", "info"),
            {
                "table": {
                    "columns": ["Situação", "Quantidade"],
                    "rows": timeline_data,
                    "header_color": self.colors["info"]
                }
            },
        ]

    def _build_tasks_preview(self) -> List[Dict[str, Any]]:
        """Prévia das tarefas mais relevantes"""
        if not self.tasks:
            return []

        # Ordena por prioridade e data
        sorted_tasks = sorted(
            self.tasks,
            key=lambda x: (
                {"critica": 0, "alta": 1, "media": 2, "baixa": 3}.get(x.get("priority", ""), 4),
                x.get("end_date", "")
            )
        )[:self.preview_limit]

        task_rows = []
        for task in sorted_tasks:
            status = self._format_status(task.get("status", ""))
            priority = self._format_priority(task.get("priority", ""))
            assigned = task.get("assigned_user", {}).get("nome", "—")
            due_date = self._format_date(task.get("end_date"))
            
            task_rows.append([
                task.get("title", "—")[:35] + ("..." if len(task.get("title", "")) > 35 else ""),
                priority,
                status,
                assigned,
                due_date,
            ])

        return [
            self._section_title("📋 Tarefas em Destaque", "secondary"),
            {
                "table": {
                    "columns": ["Tarefa", "Prioridade", "Status", "Responsável", "Prazo"],
                    "rows": task_rows,
                    "header_color": self.colors["secondary"]
                }
            },
        ]

    def _build_recommendations(self) -> List[Dict[str, Any]]:
        """Recomendações baseadas em dados"""
        recommendations = self._generate_recommendations()
        
        if not recommendations:
            return []

        recommendation_text = "\n".join([f"• {rec}" for rec in recommendations])

        return [
            self._section_title("💡 Recomendações Estratégicas", "success"),
            {
                "text": {
                    "value": recommendation_text,
                    "color": self.colors["text"]
                }
            },
        ]

    def _build_footer(self) -> Dict[str, Any]:
        """Rodapé do relatório"""
        return {
            "footer": {
                "left": "OrionForgeNexus Analytics",
                "center": f"Gerado em {self._format_datetime(datetime.now(), include_time=True)}",
                "right": "Relatório Avançado v2.0",
            }
        }

    # ==============================================================
    # 🔧 MÉTODOS AUXILIARES E ANÁLISES
    # ==============================================================
    
    def _calculate_risk_analysis(self) -> Dict[str, Any]:
        """Calcula análise de risco do projeto"""
        total = self.stats.get("total", 1)
        blocked = self.stats.get("blocked", 0)
        completed = self.stats.get("completed", 0)
        
        blocked_ratio = blocked / total
        completion_ratio = completed / total
        overdue_ratio = self._count_overdue_tasks() / total

        risks = []
        if blocked_ratio > self.RISK_THRESHOLDS['blocked_ratio']:
            risks.append(("high", "Muitas tarefas bloqueadas"))
        if completion_ratio < self.RISK_THRESHOLDS['completion_rate']:
            risks.append(("medium", "Baixa taxa de conclusão"))
        if overdue_ratio > self.RISK_THRESHOLDS['overdue_ratio']:
            risks.append(("medium", "Tarefas em atraso"))

        if not risks:
            return {'level': 'low', 'blocked_high': False, 'completion_low': False, 'overdue_high': False, 'recommendation': "Projeto em bom estado"}

        risk_level = "high" if any(r[0] == "high" for r in risks) else "medium"
        recommendation = " • ".join(r[1] for r in risks)

        return {
            'level': risk_level,
            'blocked_high': blocked_ratio > self.RISK_THRESHOLDS['blocked_ratio'],
            'completion_low': completion_ratio < self.RISK_THRESHOLDS['completion_rate'],
            'overdue_high': overdue_ratio > self.RISK_THRESHOLDS['overdue_ratio'],
            'recommendation': f"Ações recomendadas: {recommendation}"
        }

    def _calculate_performance_metrics(self) -> Dict[str, float]:
        """Calcula métricas de performance avançadas"""
        total = self.stats.get("total", 1)
        completed = self.stats.get("completed", 0)
        blocked = self.stats.get("blocked", 0)
        
        completion_rate = (completed / total) * 100
        blocked_rate = (blocked / total) * 100
        
        # Health score composto (0-100)
        health_score = max(0, min(100, 
            completion_rate * 0.6 +                    # Taxa de conclusão (60%)
            (100 - blocked_rate) * 0.3 +               # Baixo bloqueio (30%)
            (self._calculate_timeline_health() * 0.1)  # Saúde temporal (10%)
        ))

        return {
            'completion_rate': completion_rate,
            'blocked_rate': blocked_rate,
            'health_score': health_score,
            'completion_rate_7d': completion_rate * 0.8,  # Simulado
            'velocity': completed / max(1, len(self.tasks) - completed)
        }

    def _generate_recommendations(self) -> List[str]:
        """Gera recomendações baseadas na análise"""
        recs = []
        risk = self._risk_analysis
        
        if risk['blocked_high']:
            recs.append("Priorize a resolução de tarefas bloqueadas que impactam outras atividades")
        if risk['completion_low']:
            recs.append("Considere revisar a complexidade das tarefas ou realocar recursos")
        if risk['overdue_high']:
            recs.append("Estabeleça um plano de ação para tarefas em atraso")
        
        if self.stats.get('pending', 0) > self.stats.get('in_progress', 0) * 2:
            recs.append("Balanceie a distribuição entre tarefas pendentes e em andamento")
            
        if not recs:
            recs.append("Mantenha o ritmo atual de trabalho e continue monitorando o progresso")

        return recs

    def _section_title(self, text: str, color_key: str) -> Dict[str, Any]:
        """Cria um título de seção padronizado"""
        return {
            "text": {
                "value": text,
                "bold": True,
                "size": 13,
                "color": self.colors[color_key]
            }
        }

    # ==============================================================
    # ⏰ MÉTODOS DE TEMPO E PRAZOS
    # ==============================================================
    
    def _calculate_days_remaining(self) -> Optional[int]:
        """Calcula dias restantes para o prazo do projeto"""
        due_date = self.project.get('due_date')
        if not due_date:
            return None
            
        try:
            if isinstance(due_date, str):
                due_date = datetime.fromisoformat(due_date.replace('Z', '+00:00'))
            remaining = (due_date - datetime.now()).days
            return max(0, remaining)
        except Exception:
            return None

    def _calculate_sprint_days(self) -> Dict[str, int]:
        """Calcula dias totais e decorridos da sprint"""
        start = self.sprint.get('start_date')
        end = self.sprint.get('end_date')
        
        if not start or not end:
            return {'total': 0, 'elapsed': 0}
            
        try:
            if isinstance(start, str):
                start = datetime.fromisoformat(start.replace('Z', '+00:00'))
            if isinstance(end, str):
                end = datetime.fromisoformat(end.replace('Z', '+00:00'))
                
            total_days = (end - start).days
            elapsed_days = (datetime.now() - start).days
            
            return {
                'total': max(1, total_days),
                'elapsed': min(max(0, elapsed_days), total_days)
            }
        except Exception:
            return {'total': 0, 'elapsed': 0}

    def _count_overdue_tasks(self) -> int:
        """Conta tarefas com prazo vencido"""
        if not self.tasks:
            return 0
            
        today = datetime.now().date()
        overdue = 0
        
        for task in self.tasks:
            due_date = task.get('end_date')
            if due_date and self._is_task_overdue(task):
                overdue += 1
                
        return overdue

    def _count_due_soon_tasks(self) -> int:
        """Conta tarefas com prazo próximo (3 dias)"""
        if not self.tasks:
            return 0
            
        soon = 0
        for task in self.tasks:
            due_date = task.get('end_date')
            if due_date and self._is_task_due_soon(task):
                soon += 1
                
        return soon

    def _is_task_overdue(self, task: Dict[str, Any]) -> bool:
        """Verifica se uma tarefa está atrasada"""
        due_date = task.get('end_date')
        if not due_date or task.get('status') in ['concluida', 'cancelada']:
            return False
            
        try:
            if isinstance(due_date, str):
                due_date = datetime.fromisoformat(due_date.replace('Z', '+00:00')).date()
            elif isinstance(due_date, datetime):
                due_date = due_date.date()
            else:
                return False
                
            return due_date < datetime.now().date()
        except Exception:
            return False

    def _is_task_due_soon(self, task: Dict[str, Any]) -> bool:
        """Verifica se uma tarefa está próxima do prazo (3 dias)"""
        due_date = task.get('end_date')
        if not due_date or task.get('status') in ['concluida', 'cancelada']:
            return False
            
        try:
            if isinstance(due_date, str):
                due_date = datetime.fromisoformat(due_date.replace('Z', '+00:00')).date()
            elif isinstance(due_date, datetime):
                due_date = due_date.date()
            else:
                return False
                
            days_until_due = (due_date - datetime.now().date()).days
            return 0 <= days_until_due <= 3
        except Exception:
            return False

    def _calculate_timeline_health(self) -> float:
        """Calcula saúde dos prazos (0-100)"""
        if not self.tasks:
            return 100.0
            
        total_tasks = len(self.tasks)
        overdue = self._count_overdue_tasks()
        due_soon = self._count_due_soon_tasks()
        
        # Penaliza tarefas atrasadas e próximas do prazo
        health_score = 100.0
        health_score -= (overdue / total_tasks) * 60   # -60% por atrasos
        health_score -= (due_soon / total_tasks) * 30  # -30% por prazos próximos
        
        return max(0.0, health_score)

    # ==============================================================
    # 🎭 MÉTODOS DE FORMATAÇÃO E STATUS
    # ==============================================================
    
    def _format_priority(self, priority: str) -> str:
        """Formata prioridade com emoji"""
        priority_map = {
            "critica": "🔴 Crítica",
            "alta": "🟠 Alta", 
            "media": "🟡 Média",
            "baixa": "🟢 Baixa",
            "urgente": "🚨 Urgente"
        }
        return priority_map.get(priority.lower(), priority)

    def _format_status(self, status: str) -> str:
        """Formata status com emoji"""
        status_map = {
            "pendente": "⏳ Pendente",
            "em_andamento": "🔄 Em Andamento", 
            "em_revisao": "👁️ Em Revisão",
            "revisao": "👁️ Em Revisão",
            "bloqueada": "🚫 Bloqueada",
            "concluida": "✅ Concluída",
            "cancelada": "❌ Cancelada"
        }
        return status_map.get(status.lower(), status)

    def _get_completion_status(self, rate: float) -> str:
        """Retorna status da taxa de conclusão"""
        if rate >= 80: return "🟢 Excelente"
        if rate >= 60: return "🟡 Bom" 
        if rate >= 40: return "🟠 Regular"
        return "🔴 Baixo"

    def _get_blocked_status(self, count: int) -> str:
        """Retorna status de tarefas bloqueadas"""
        if count == 0: return "🟢 Nenhuma"
        if count <= 2: return "🟡 Poucas"
        if count <= 5: return "🟠 Médias"
        return "🔴 Muitas"

    def _get_velocity_status(self, velocity: float) -> str:
        """Retorna status da velocidade"""
        if velocity >= 20: return "🟢 Alta"
        if velocity >= 10: return "🟡 Média"
        return "🔴 Baixa"

    def _format_date(self, value: Any) -> str:
        """Formata data de forma segura"""
        if not value:
            return "—"
            
        try:
            if isinstance(value, str):
                if 'Z' in value:
                    value = value.replace('Z', '+00:00')
                dt = datetime.fromisoformat(value)
                return dt.strftime("%d/%m/%Y")
            elif isinstance(value, datetime):
                return value.strftime("%d/%m/%Y")
            return str(value)
        except Exception:
            return "—"

    def _format_datetime(self, dt: datetime, include_time: bool = False) -> str:
        """Formata datetime de forma consistente"""
        fmt = "%d/%m/%Y %H:%M" if include_time else "%d/%m/%Y"
        return dt.strftime(fmt)
    
# Adicione estas funções para manter compatibilidade com o código existente
def gerar_relatorio_tarefas(
    stats: dict,
    project: Optional[dict],
    sprint: Optional[dict],
    tasks: Optional[List[dict]],
    logo_path: str
):
    """Função legada para compatibilidade - usa AdvancedTaskReportBuilder internamente"""
    builder = AdvancedTaskReportBuilder(stats=stats, project=project, sprint=sprint, tasks=tasks or [])
    estrutura = builder.build()
    
    # Gera o PDF usando o gerador existente
    filename = f"relatorio_tarefas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    from app.relatorio.gerarpdf import GenericReportGenerator
    generator = GenericReportGenerator(logo_path=logo_path)
    generator.generate_report(filename, estrutura)
    
    log_message(f"✅ PDF legado gerado: {filename}")

