# reset_db.py
import sys
import traceback
from app import database
from app.config.dotenv import get_env
from app.models import (
    user_model,
    geral_model,
    connection_models,
    task_models,
    dbstatistics_models,
    queryhistory_models,
    dbstructure_models,
)


def recreate_db():
    """Apaga e recria todas as tabelas do banco de dados (somente em dev)."""
    print("  Iniciando processo de reset do banco de dados...\n")

    all_bases = [
        user_model.Base,
        geral_model.Base,
        connection_models.Base,
        task_models.Base,
        dbstatistics_models.Base,
        queryhistory_models.Base,
        dbstructure_models.Base,
    ]

    try:
        # 1️⃣ Apaga todas as tabelas
        for base in all_bases:
            base.metadata.drop_all(bind=database.sync_engine)
        print("  Todas as tabelas foram removidas com sucesso.")

        # 2️⃣ Recria todas as tabelas
        for base in all_bases:
            base.metadata.create_all(bind=database.sync_engine)
        print("  Banco de dados recriado com sucesso!\n")

        # print("📦 Engine usada:", database.DATABASE_URL)
        print(" Pronto para inserção de dados iniciais.")
        return True
    except Exception as e:
        print(f"  Erro ao recriar o banco de dados: {e}{traceback.format_exc()}")
        sys.exit(1)
        return False


if __name__ == "__main__":
    env = get_env("ENV", "dev")

    if env.lower() == "dev":
        print("  Ambiente de desenvolvimento detectado (ENV=dev)")
        recreate_db()
    else:
        print("  Operação bloqueada! Ambiente atual:", env)
        print("  O reset só pode ser executado no ambiente de desenvolvimento.")
        sys.exit(0)
