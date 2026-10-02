"""
🔌 Operações que uma REGRA de conexão pode conceder.

Uma regra (ConnectionRole) é definida pelo dono da conexão e atribuída a cada
membro com acesso. Só estas operações contam para o acesso — outras
permissões ligadas à regra (ex.: das regras padrão antigas) são ignoradas.

Sem imports: usado pela política de acesso (connection_access), pelo CRUD e
pelas rotas sem criar importações circulares.
"""

# Nome da permissão → rótulo mostrado no ecrã de regras.
CONNECTION_CAPABILITIES: dict[str, str] = {
    "query:execute": "Consultar dados",
    "data:write": "Inserir e editar dados",
    "data:delete": "Apagar dados",
    "schema:manage": "Alterar a estrutura (tabelas e colunas)",
    "query:export": "Exportar e gerar relatórios",
    "data:transfer": "Transferir dados entre conexões",
    "backup:execute": "Fazer backup",
    "backup:restore": "Restaurar backup",
}

CAP_QUERY = "query:execute"
CAP_WRITE = "data:write"
CAP_DELETE = "data:delete"
CAP_SCHEMA = "schema:manage"
CAP_EXPORT = "query:export"
CAP_TRANSFER = "data:transfer"
CAP_BACKUP = "backup:execute"
CAP_RESTORE = "backup:restore"

# Operações que alteram a base: com alguma delas, o nível passa a "write".
WRITE_CAPABILITIES: frozenset[str] = frozenset(
    {CAP_WRITE, CAP_DELETE, CAP_SCHEMA, CAP_RESTORE}
)


def is_connection_capability(name: str) -> bool:
    return name in CONNECTION_CAPABILITIES
