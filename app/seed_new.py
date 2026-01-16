"""
Seed inicial do sistema
- Empresa
- Cargos
- Roles
- Permissões (RBAC)
- Usuário Admin
- Tipos de Projeto
- Audit Log
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Type

from sqlalchemy.orm import Session

from app.auth import hash_password
from app.models.task_models import AuditLog, TypeProjecto
from app.models.user_model import Empresa, Cargo, Role, Permission, User
from app.ultils.logger import log_message


# ==========================================================
# 🔧 CONFIGURAÇÕES BASE
# ==========================================================

DEFAULT_EMPRESA = {
    "nome": "OkayulaTech Lda",
    "tamanho": "51-200",
    "nif": "700000000",
    "endereco": "Rua das pedras, Golf 2 Projecto, Luanda",
}

CARGOS_DATA = [
    ("Admin", "Administrador do sistema", "sênior"),
    ("Gerente", "Responsável pela equipe", "pleno"),
    ("Desenvolvedor", "Cria e mantém sistemas", "pleno"),
    ("Analista", "Analisa soluções técnicas", "júnior"),
    ("Designer UX/UI", "Projeta interfaces", "pleno"),
    ("DevOps Engineer", "Automação e infraestrutura", "sênior"),
    ("QA Tester", "Garante qualidade", "júnior"),
]


ROLES_PERMISSIONS = {
    "admin": {
        "description": "Acesso total ao sistema",
        "permissions": [
            # AUTH & USERS
            "auth:login", "auth:logout", "auth:refresh",
            "user:create", "user:read", "user:update", "user:delete",
            "user:invite", "user:deactivate", "user:manage",

            # ROLES & PERMISSIONS
            "role:create", "role:read", "role:update", "role:delete", "role:manage",
            "permission:create", "permission:read", "permission:update",
            "permission:delete", "permission:manage",

            # COMPANY
            "company:read", "company:update", "company:settings",
            "company:billing", "company:members",
            "company:invite", "company:remove_member",

            # DB CONNECTIONS
            "db_connection:create", "db_connection:read_own",
            "db_connection:read_company", "db_connection:read_all",
            "db_connection:update", "db_connection:delete", "db_connection:test",

            # QUERY / SQL
            "query:execute", "query:read_history",
            "query:delete_history", "query:export",

            # TABLES
            "table:read", "table:describe", "table:stats", "table:export",

            # PROJECTS
            "project:create", "project:read", "project:view",
            "project:update", "project:delete", "project:manage",
            "project:assign_user", "project:remove_user",

            # TEAM
            "team:read", "team:update", "team:manage",

            # INTEGRATIONS
            "integration:read", "integration:create",
            "integration:update", "integration:delete",
            "integration:webhook",

            # SETTINGS
            "settings:user", "settings:company",
            "settings:projects", "settings:team",
            "settings:integrations", "settings:system",

            # SYSTEM
            "audit:read", "audit:export",
            "logs:read", "logs:export",
            "backup:read", "backup:configure",
            "backup:execute", "backup:restore",        
            #analytics
            "analytics:db:view","audit:read", "analytics:project:view", "analytics:db:export","analytics:project:export","admin:*",
            
            #logs
            "logs:view"
        ],
    },

    "manager": {
        "description": "Gestão de equipe e projetos",
        "permissions": [
            "user:read", "user:invite",
            "team:read",

            "company:read",

            "db_connection:read_company",
            "query:execute", "query:read_history",

            "project:create", "project:read",
            "project:update", "project:assign_user",

            "settings:projects", "settings:team",
        ],
    },

    "developer": {
        "description": "Desenvolvedor técnico",
        "permissions": [
            "db_connection:create",
            "db_connection:read_own",

            "query:execute", "query:read_history",

            "table:read", "table:describe",

            "project:view",

            "settings:user",
        ],
    },

    "user": {
        "description": "Usuário básico",
        "permissions": [
            "db_connection:read_own",
            "query:execute",

            "project:view",

            "settings:user",
        ],
    },
}

PROJECT_TYPES = [
    ("Interno", "Projetos internos"),
    ("Externo", "Projetos para clientes"),
    ("Pesquisa", "Projetos experimentais"),
]


# ==========================================================
# 🧠 HELPER GENÉRICO
# ==========================================================

def get_or_create(
    db: Session,
    model: Type[Any],
    defaults: Optional[Dict[str, Any]] = None,
    **filters: Any,
) -> Tuple[Any, bool]:
    instance = db.query(model).filter_by(**filters).first()
    if instance:
        return instance, False

    params = {**filters, **(defaults or {})}
    instance = model(**params)
    db.add(instance)
    db.flush()
    return instance, True


# ==========================================================
# 🏢 EMPRESA
# ==========================================================

def seed_empresa(db: Session) -> Empresa:
    empresa, created = get_or_create(
        db,
        Empresa,
        nome=DEFAULT_EMPRESA["nome"],
        defaults=DEFAULT_EMPRESA,
    )
    log_message(
        f"🏢 Empresa {'criada' if created else 'já existente'}: {empresa.nome}",
        "success" if created else "info",
    )
    return empresa


# ==========================================================
# 💼 CARGOS
# ==========================================================

def seed_cargos(db: Session) -> None:
    for nome, descricao, nivel in CARGOS_DATA:
        get_or_create(
            db,
            Cargo,
            nome=nome,
            defaults={"descricao": descricao, "nivel": nivel},
        )
    log_message("💼 Cargos sincronizados", "info")


# ==========================================================
# 🔐 RBAC (Roles + Permissões)
# ==========================================================

def seed_rbac(db: Session) -> None:
    # Criar permissões únicas
    permission_map: Dict[str, Permission] = {}

    all_permissions = {
        perm
        for role in ROLES_PERMISSIONS.values()
        for perm in role["permissions"]
    }

    for perm_name in all_permissions:
        perm, _ = get_or_create(
            db,
            Permission,
            name=perm_name,
            defaults={"description": f"Permissão {perm_name}"},
        )
        permission_map[perm_name] = perm

    # Criar roles e associar permissões
    for role_name, data in ROLES_PERMISSIONS.items():
        role, _ = get_or_create(
            db,
            Role,
            name=role_name,
            defaults={"description": data["description"]},
        )

        # Sincronização forte (modelo Google/AWS)
        role.permissions = [
            permission_map[p] for p in data["permissions"]
        ]

    log_message("🎭 RBAC sincronizado (roles + permissões)", "success")


# ==========================================================
# 👤 USUÁRIO ADMIN
# ==========================================================

def seed_admin_user(db: Session, empresa: Empresa) -> User:
    cargo_admin = db.query(Cargo).filter_by(nome="Admin").first()
    role_admin = db.query(Role).filter_by(name="admin").first()

    admin, created = get_or_create(
        db,
        User,
        email="admin@okayulatech.com",
        defaults={
            "nome": "Administrador",
            "apelido": "Geral",
            "telefone": "+244900000001",
            "empresa_id": empresa.id,
            "cargo_id": cargo_admin.id if cargo_admin else None,
            "role_id": role_admin.id if role_admin else None,
            "hashed_password": hash_password("Admin@123"),
            "concorda_termos": True,
            "is_active": True,
        },
    )

    log_message(
        f"👤 Usuário admin {'criado' if created else 'já existente'}",
        "success" if created else "info",
    )
    return admin


# ==========================================================
# 🌱 SEED PRINCIPAL
# ==========================================================

def seed_data(db: Session) -> None:
    log_message("🚀 Iniciando seed do sistema...", "info")

    try:
        empresa = seed_empresa(db)
        seed_cargos(db)
        seed_rbac(db)
        admin = seed_admin_user(db, empresa)

        # Tipos de projeto
        for name, desc in PROJECT_TYPES:
            get_or_create(
                db,
                TypeProjecto,
                name=name,
                defaults={"description": desc},
            )

        # Audit Log
        db.add(
            AuditLog(
                user_id=str(admin.id),
                action="Seed inicial do sistema",
                entity="Sistema",
                entity_id="seed-init",
                timestamp=datetime.now(timezone.utc),
            )
        )

        db.commit()
        log_message("🌱 Seed executado com sucesso!", "success")

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro crítico no seed: {str(e)}", "error")
        raise
