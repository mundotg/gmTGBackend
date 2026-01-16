from datetime import datetime
from sqlalchemy.orm import Session

from app.auth import hash_password
from app.ultils.logger import log_message
from app.models.user_model import Empresa, Cargo, Role, User
from app.models.task_models import TypeProjecto, AuditLog


def get_or_create(db: Session, model, defaults=None, **filters):
    """
    Helper genérico para criar registros se não existirem
    """
    instance = db.query(model).filter_by(**filters).first()
    if instance:
        return instance, False

    params = {**filters, **(defaults or {})}
    instance = model(**params)
    db.add(instance)
    db.flush()
    return instance, True


def seed_data(db: Session):
    """
    Seed inicial do sistema:
    - Empresa
    - Cargos
    - Usuário Admin
    - Roles
    - Tipos de Projeto
    - AuditLog
    """

    log_message("🚀 Iniciando seed de dados...", "info")

    try:
        # -----------------------------
        # 🏢 Empresa padrão
        # -----------------------------
        empresa, created = get_or_create(
            db,
            Empresa,
            nome="OkayulaTech Lda",
            defaults=dict(
                tamanho="51-200",
                nif="700000000",
                endereco="Rua das pedras, Golf 2 Projecto, Luanda",
            ),
        )

        log_message(
            f"🏢 Empresa {'criada' if created else 'já existente'}: {empresa.nome}",
            "success" if created else "info",
        )

        # -----------------------------
        # 💼 Cargos base
        # -----------------------------
        cargos_base = [
            ("Admin", "Administrador do sistema", "sênior"),
            ("Gerente", "Responsável pela equipe e decisões estratégicas", "pleno"),
            ("Desenvolvedor", "Cria e mantém sistemas e aplicações", "pleno"),
            ("Analista", "Analisa e projeta soluções técnicas", "júnior"),
            ("Designer UX/UI", "Projeta experiências e interfaces", "pleno"),
            ("Engenheiro de Dados", "Gerencia pipelines de dados", "sênior"),
            ("DevOps Engineer", "Automação e infraestrutura", "sênior"),
            ("Product Owner", "Define visão do produto", "pleno"),
            ("Scrum Master", "Facilita processos ágeis", "pleno"),
            ("QA Tester", "Garante qualidade do software", "júnior"),
            ("Analista de Suporte", "Suporte técnico", "júnior"),
            ("Gestor Financeiro", "Gestão financeira", "sênior"),
            ("Recursos Humanos", "Gestão de pessoas", "pleno"),
            ("Marketing Digital", "Marketing online", "pleno"),
            ("Consultor Técnico", "Consultoria tecnológica", "sênior"),
        ]

        cargos_criados = []
        for nome, descricao, nivel in cargos_base:
            _, created = get_or_create(
                db,
                Cargo,
                nome=nome,
                defaults=dict(descricao=descricao, nivel=nivel),
            )
            if created:
                cargos_criados.append(nome)

        log_message(
            f"💼 Cargos criados: {', '.join(cargos_criados)}"
            if cargos_criados
            else "💼 Nenhum cargo novo necessário",
            "success" if cargos_criados else "info",
        )

        # -----------------------------
        # 👤 Usuário Administrador
        # -----------------------------
        cargo_admin = db.query(Cargo).filter_by(nome="Admin").first()

        admin_user, created = get_or_create(
            db,
            User,
            email="admin@okayulaTech.com",
            defaults=dict(
                nome="Administrador",
                apelido="Geral",
                telefone="+244900000001",
                empresa_id=empresa.id,
                cargo_id=cargo_admin.id,
                hashed_password=hash_password("Admin@123"),
                concorda_termos=True,
            ),
        )

        log_message(
            f"👤 Usuário admin {'criado' if created else 'já existente'}",
            "success" if created else "info",
        )

        # -----------------------------
        # 🎭 Roles padrão
        # -----------------------------
        roles_base = [
            ("admin", "Acesso total ao sistema"),
            ("manager", "Gerencia projetos e equipes"),
            ("user", "Acesso básico"),
            ("developer", "Desenvolvimento técnico"),
            ("qa_tester", "Qualidade de software"),
            ("product_owner", "Gestão de produto"),
            ("scrum_master", "Agilidade"),
            ("data_analyst", "Análise de dados"),
            ("devops", "Infraestrutura e deploy"),
            ("security_officer", "Segurança"),
            ("support_agent", "Suporte"),
            ("hr_manager", "Recursos Humanos"),
            ("finance_manager", "Financeiro"),
        ]

        roles_criadas = []
        for name, description in roles_base:
            _, created = get_or_create(
                db,
                Role,
                name=name,
                defaults=dict(description=description),
            )
            if created:
                roles_criadas.append(name)

        log_message(
            f"🎭 Roles criadas: {', '.join(roles_criadas)}"
            if roles_criadas
            else "🎭 Nenhuma role nova necessária",
            "success" if roles_criadas else "info",
        )
        

        log_message("🔐 Criando permissões do sistema...", "info")

        permissions_base = {
            # 🔌 DB Connections
            "db_connection:create": "Criar conexões de banco de dados",
            "db_connection:read": "Visualizar próprias conexões",
            "db_connection:update": "Editar próprias conexões",
            "db_connection:delete": "Remover próprias conexões",
            "db_connection:read_company": "Visualizar conexões da empresa",
            "db_connection:read_all": "Visualizar todas as conexões",

            # 👤 Usuários
            "user:read": "Visualizar usuários",
            "user:manage": "Gerenciar usuários",

            # 📊 Queries
            "query:execute": "Executar queries",
            "query:history": "Ver histórico de queries",

            # 📁 Projetos
            "project:create": "Criar projetos",
            "project:update": "Editar projetos",
            "project:delete": "Remover projetos",
            "project:view": "Visualizar projetos",
        }

        permissions_map = {}

        for name, description in permissions_base.items():
            permission = db.query(Permission).filter_by(name=name).first()
            if not permission:
                permission = Permission(name=name, description=description)
                db.add(permission)
                db.flush()
                log_message(f"➕ Permissão criada: {name}", "success")

            permissions_map[name] = permission

        # ============================
        # 🎭 Vínculo ROLE → PERMISSIONS
        # ============================

        role_permissions = {
            "admin": list(permissions_map.keys()),

            "manager": [
                "db_connection:read_company",
                "query:execute",
                "query:history",
                "project:create",
                "project:update",
                "project:view",
                "user:read",
            ],

            "developer": [
                "db_connection:create",
                "db_connection:read",
                "query:execute",
                "query:history",
                "project:view",
            ],

            "user": [
                "db_connection:read",
                "query:execute",
                "project:view",
            ],

            "devops": [
                "db_connection:create",
                "db_connection:update",
                "db_connection:read_company",
                "query:execute",
            ],

            "security_officer": [
                "db_connection:read_all",
                "query:history",
                "user:read",
            ],

            "qa_tester": [
                "project:view",
                "query:execute",
            ],

            "product_owner": [
                "project:create",
                "project:update",
                "project:view",
            ],

            "support_agent": [
                "db_connection:read_company",
                "query:history",
            ],

            "finance_manager": [
                "project:view",
            ],

            "hr_manager": [
                "user:read",
            ],
        }

        for role_name, perms in role_permissions.items():
            role = db.query(Role).filter_by(name=role_name).first()
            if not role:
                continue

            for perm_name in perms:
                permission = permissions_map.get(perm_name)
                if permission and permission not in role.permissions:
                    role.permissions.append(permission)

            log_message(
                f"🔗 Permissões associadas à role '{role_name}'",
                "success"
            )

        db.commit()
        log_message("✅ Seed de permissões finalizado!", "success")


        # -----------------------------
        # 📦 Tipos de Projeto
        # -----------------------------
        tipos_projeto = [
            ("Interno", "Projetos internos da empresa"),
            ("Externo", "Projetos para clientes"),
            ("Pesquisa", "Projetos experimentais"),
        ]

        tipos_criados = []
        for name, description in tipos_projeto:
            _, created = get_or_create(
                db,
                TypeProjecto,
                name=name,
                defaults=dict(description=description),
            )
            if created:
                tipos_criados.append(name)

        log_message(
            f"📦 Tipos de projeto criados: {', '.join(tipos_criados)}"
            if tipos_criados
            else "📦 Nenhum tipo de projeto novo necessário",
            "success" if tipos_criados else "info",
        )

        # -----------------------------
        # 🧾 Audit Log
        # -----------------------------
        db.add(
            AuditLog(
                user_id=str(admin_user.id),
                action="Seed inicial executado",
                entity="Sistema",
                entity_id="seed-init",
                timestamp=datetime.utcnow(),
            )
        )

        db.commit()
        log_message("🧾 Log de auditoria criado", "info")
        log_message("🌱 Seed concluído com sucesso!", "success")

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro durante seed: {str(e)}", "error")
        raise
