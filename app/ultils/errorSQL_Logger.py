import traceback
from app.ultils.logger import log_message


def _lidar_com_erro_sql(e: Exception) -> str:
    """
    Trata erros SQL comuns e retorna mensagens padronizadas para o usuário.
    """
    log_message(
        f"Erro SQL: {e} ({type(e).__name__})\n{traceback.format_exc()}",
        level="error"
    )

    error_message = str(e)

    if "ForeignKeyViolation" in error_message or "violates foreign key" in error_message:
        msg = (
            "❌ Falha ao salvar: violação de chave estrangeira. "
            "Verifique se os dados informados existem na tabela de referência."
        )
    elif "UniqueViolation" in error_message or "duplicate key value" in error_message:
        msg = (
            "❌ Falha ao salvar: violação de unicidade. "
            "Um registro com o mesmo valor já existe no banco de dados."
        )
    elif "NotNullViolation" in error_message or "null value in column" in error_message:
        msg = (
            "❌ Falha ao salvar: valor nulo em coluna obrigatória. "
            "Preencha todos os campos obrigatórios antes de salvar."
        )
    elif "CheckViolation" in error_message or "violates check constraint" in error_message:
        msg = (
            "❌ Falha ao salvar: restrição CHECK violada. "
            "Os valores informados não atendem às regras definidas no banco."
        )
    elif "DataError" in error_message or "value too long" in error_message:
        msg = (
            "❌ Falha ao salvar: dado inválido ou muito longo para o campo. "
            "Reduza o tamanho do valor ou verifique o formato esperado."
        )
     # Erros de integridade (INSERT/UPDATE/DELETE)
    elif "ForeignKeyViolation" in error_message:
        msg = "Falha devido à violação de chave estrangeira. Verifique as referências entre tabelas."
    elif "UniqueViolation" in error_message:
        msg = "Falha devido a uma violação de unicidade. O valor informado já existe no banco de dados."

    # Erros típicos em SELECT
    elif "UndefinedTable" in error_message:
        msg = "A tabela especificada não existe no banco de dados."
    elif "UndefinedColumn" in error_message:
        msg = "Uma das colunas informadas não existe na tabela."
    elif "DatatypeMismatch" in error_message or "InvalidTextRepresentation" in error_message:
        msg = "Tipo de dado inválido na consulta. Verifique os valores passados no filtro."
    elif "SyntaxError" in error_message:
        msg = "Erro de sintaxe na consulta SQL. Verifique a estrutura do comando."
    elif "OperationalError" in error_message:
        msg = "Erro operacional ao executar a consulta. Verifique a conexão e a consulta."
    elif "IntegrityError" in error_message:
        msg = (
            "❌ Falha ao salvar: integridade referencial violada. "
            "Verifique chaves primárias, estrangeiras e restrições de unicidade."
        )
    elif "SyntaxError" in error_message or "syntax error at" in error_message:
        msg = (
            "❌ Erro de sintaxe na query SQL. "
            "Verifique se a consulta está correta."
        )
    else:
        msg = f"⚠️ Ocorreu um erro inesperado ao tentar salvar os dados: {error_message}"

    return msg



