"""
🏢 Permissões que um CARGO DA EMPRESA pode conceder.

Um cargo (função com `empresa_id`) só pode dar as quatro ações da empresa:
editar a empresa, adicionar membros, remover membros e mudar o cargo dos
membros. Tudo o resto — gerir a equipa e os cargos, projetos, registos e
auditoria, configurações do sistema, gestão global de utilizadores e de
permissões, acesso a dados (consultas, tabelas, escrita, transferências,
backups) — vem só do TIPO DE UTILIZADOR (função global), que é gerido pela
administração do sistema.

Lista explícita (e não por prefixo) de propósito: uma permissão nova não entra
nos cargos sem alguém decidir que é da empresa.

Sem imports: usada pelo modelo (User.permissions), pelo CRUD e pela migração
de limpeza sem criar importações circulares.
"""

COMPANY_ROLE_PERMISSIONS: frozenset[str] = frozenset({
    "company:update",         # Editar a empresa
    "company:invite",         # Adicionar membros
    "company:remove_member",  # Remover membros
    "company:assign_cargo",   # Mudar o cargo dos membros
})


def is_company_permission(name: str) -> bool:
    return name in COMPANY_ROLE_PERMISSIONS
