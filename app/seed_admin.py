from datetime import datetime
from sqlalchemy.orm import Session
from app.auth import hash_password
from app.ultils.logger import log_message

from app.models.user_model import Empresa, Cargo, User
from app.models.task_models import Usuario as UserTask, Role, TypeProjecto, AuditLog


def seed_data(db: Session):
    """
    Popula a base de dados com:
    - Empresa padrão
    - Cargos base
    - Usuário administrador
    - Roles padrão
    - Tipos de projeto
    - Usuário admin no módulo de tarefas
    - Registro inicial no AuditLog
    """

    log_message("🚀 Iniciando seed de dados...", "info")

    # -----------------------------
    # 🏢 Empresa padrão
    # -----------------------------
    empresa_nome = "OkayulaTech Lda"
    empresa = db.query(Empresa).filter_by(nome=empresa_nome).first()

    if not empresa:
        empresa = Empresa(
            nome=empresa_nome,
            tamanho="51-200",
            nif="700000000",
            endereco="Rua das pedras, Golf 2 Projecto, Luanda",
            criado_em=datetime.utcnow(),
        )
        db.add(empresa)
        db.flush()
        log_message(f"🏢 Empresa criada: {empresa.nome}", "success")
    else:
        log_message(f"🏢 Empresa já existe: {empresa.nome}", "info")

    # -----------------------------
    # 💼 Cargos base
    # -----------------------------
    cargos_base = [
        {"nome": "Admin", "descricao": "Administrador do sistema", "nivel": "sênior"},
        {
            "nome": "Gerente",
            "descricao": "Responsável pela equipe e decisões estratégicas",
            "nivel": "pleno",
        },
        {
            "nome": "Desenvolvedor",
            "descricao": "Cria e mantém sistemas e aplicações",
            "nivel": "pleno",
        },
        {
            "nome": "Analista",
            "descricao": "Analisa e projeta soluções para problemas técnicos",
            "nivel": "júnior",
        },
        {
            "nome": "Designer UX/UI",
            "descricao": "Projeta experiências e interfaces de usuário",
            "nivel": "pleno",
        },
        {
            "nome": "Engenheiro de Dados",
            "descricao": "Gerencia pipelines e estruturas de dados complexos",
            "nivel": "sênior",
        },
        {
            "nome": "DevOps Engineer",
            "descricao": "Automatiza e mantém infraestrutura de desenvolvimento",
            "nivel": "sênior",
        },
        {
            "nome": "Product Owner",
            "descricao": "Define visão e prioridades de produtos",
            "nivel": "pleno",
        },
        {
            "nome": "Scrum Master",
            "descricao": "Facilita processos ágeis e comunicação da equipe",
            "nivel": "pleno",
        },
        {
            "nome": "QA Tester",
            "descricao": "Executa testes e garante a qualidade do software",
            "nivel": "júnior",
        },
        {
            "nome": "Analista de Suporte",
            "descricao": "Atende usuários e resolve problemas técnicos",
            "nivel": "júnior",
        },
        {
            "nome": "Gestor Financeiro",
            "descricao": "Gerencia orçamento e finanças da empresa",
            "nivel": "sênior",
        },
        {
            "nome": "Recursos Humanos",
            "descricao": "Gerencia recrutamento, benefícios e clima organizacional",
            "nivel": "pleno",
        },
        {
            "nome": "Marketing Digital",
            "descricao": "Planeja e executa campanhas de marketing online",
            "nivel": "pleno",
        },
        {
            "nome": "Consultor Técnico",
            "descricao": "Presta consultoria e orientação em soluções tecnológicas",
            "nivel": "sênior",
        },
    ]

    cargos_criados = []
    for cargo_data in cargos_base:
        cargo = db.query(Cargo).filter_by(nome=cargo_data["nome"]).first()
        if not cargo:
            cargo = Cargo(**cargo_data)
            db.add(cargo)
            cargos_criados.append(cargo_data["nome"])
            db.flush()

    if cargos_criados:
        log_message(f"💼 Cargos criados: {', '.join(cargos_criados)}", "success")
    else:
        log_message("💼 Nenhum cargo novo necessário — já existiam", "info")

    # -----------------------------
    # 👤 Usuário administrador (sistema principal)
    # -----------------------------
    admin_email = "admin@okayulaTech.com"
    admin_user = db.query(User).filter_by(email=admin_email).first()

    if not admin_user:
        cargo_admin = db.query(Cargo).filter_by(nome="Admin").first()
        hashed_pw = hash_password("Admin@123")

        admin_user = User(
            nome="Administrador",
            apelido="Geral",
            email=admin_email,
            telefone="+244900000001",
            empresa_id=empresa.id,
            cargo_id=cargo_admin.id,
            hashed_password=hashed_pw,
            concorda_termos=True,
            criado_em=datetime.utcnow(),
        )

        db.add(admin_user)
        db.commit()
        db.refresh(admin_user)

        log_message(f"✅ Usuário administrador criado: {admin_user.email}", "success")
    else:
        log_message("👤 Usuário administrador já existe.", "info")

    # -----------------------------
    # 🎭 Roles padrão
    # -----------------------------
    roles_base = [
        {"name": "admin", "description": "Acesso total ao sistema"},
        {"name": "manager", "description": "Gerencia projetos e equipes"},
        {"name": "user", "description": "Acesso básico ao sistema"},
        {
            "name": "developer",
            "description": "Desenvolve e mantém funcionalidades técnicas",
        },
        {
            "name": "qa_tester",
            "description": "Executa testes e garante a qualidade do software",
        },
        {
            "name": "product_owner",
            "description": "Define e prioriza funcionalidades do produto",
        },
        {
            "name": "scrum_master",
            "description": "Facilita processos ágeis e remove impedimentos",
        },
        {
            "name": "data_analyst",
            "description": "Analisa e interpreta dados para suporte à decisão",
        },
        {
            "name": "devops",
            "description": "Gerencia infraestrutura, deploys e automação",
        },
        {
            "name": "security_officer",
            "description": "Garante a segurança de dados e acessos",
        },
        {
            "name": "support_agent",
            "description": "Atende solicitações e problemas de usuários",
        },
        {
            "name": "hr_manager",
            "description": "Gerencia o recrutamento e o bem-estar da equipe",
        },
        {
            "name": "finance_manager",
            "description": "Controla orçamentos, despesas e relatórios financeiros",
        },
    ]

    roles_criadas = []
    for role_data in roles_base:
        role = db.query(Role).filter_by(name=role_data["name"]).first()
        if not role:
            role = Role(**role_data)
            db.add(role)
            roles_criadas.append(role_data["name"])
            db.flush()

    if roles_criadas:
        log_message(f"🎭 Roles criadas: {', '.join(roles_criadas)}", "success")
    else:
        log_message("🎭 Nenhuma role nova necessária — já existiam", "info")

    # -----------------------------
    # 📦 Tipos de Projeto
    # -----------------------------
    tipos_projetos = [
        {"name": "Interno", "description": "Projetos internos da empresa"},
        {"name": "Externo", "description": "Projetos para clientes"},
        {"name": "Pesquisa", "description": "Projetos experimentais ou de estudo"},
    ]

    tipos_criados = []
    for tipo_data in tipos_projetos:
        tipo = db.query(TypeProjecto).filter_by(name=tipo_data["name"]).first()
        if not tipo:
            tipo = TypeProjecto(**tipo_data)
            db.add(tipo)
            tipos_criados.append(tipo_data["name"])
            db.flush()

    if tipos_criados:
        log_message(
            f"📦 Tipos de projeto criados: {', '.join(tipos_criados)}", "success"
        )
    else:
        log_message("📦 Nenhum tipo de projeto novo necessário — já existiam", "info")

    # -----------------------------
    # 👨‍💼 Usuário administrador (módulo de tarefas)
    # -----------------------------
    task_admin_email = "admin.tasks@okayulaTech.com"
    task_admin = db.query(UserTask).filter_by(email=task_admin_email).first()

    if not task_admin:
        role_admin = db.query(Role).filter_by(name="admin").first()
        task_admin = UserTask(
            nome="Administrador de Tarefas",
            user_id = admin_user.id,
            email=task_admin_email,
            senha=hash_password("AdminTask@123"),
            role_id=role_admin.id,
            is_active=True,
            email_verified=True,
            created_at=datetime.utcnow(),
        )
        db.add(task_admin)
        db.commit()
        db.refresh(task_admin)
        log_message(
            f"✅ Usuário administrador de tarefas criado: {task_admin.email}", "success"
        )
    else:
        log_message("👨‍💼 Usuário administrador de tarefas já existe.", "info")

    # -----------------------------
    # 🧾 Registro no AuditLog
    # -----------------------------
    audit_entry = AuditLog(
        user_id=str(task_admin.id),
        action="Seed inicial executado",
        entity="Sistema",
        entity_id="seed-init",
        timestamp=datetime.utcnow(),
    )
    db.add(audit_entry)
    db.commit()

    log_message("🧾 Log de auditoria criado para o seed inicial", "info")
    log_message("🌱 Seed concluído com sucesso!", "success")
