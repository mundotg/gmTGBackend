import os
import pickle
from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError
from app.config.dotenv import get_env  # ou ajuste conforme seu projeto
from app.database import sync_engine as engine
from app.models import (
    user_model,
    geral_model,
    connection_models,
    task_models,
    dbstatistics_models,
    queryhistory_models,
    dbstructure_models,
)
# from app.ultils.logger import log_message

FLAG_FILE = get_env("FLAG_FILE", "app/config/initialized.pkl")


def already_initialized():
    return os.path.exists(FLAG_FILE)


def mark_initialized():
    with open((FLAG_FILE), "wb") as f:
        pickle.dump({"initialized": True}, f)



def reset_database():
    # print("🔁 Resetando banco de dados...")
    # with engine.connect() as conn:
    #     # Elimina o schema inteiro (remove tudo)
    #     conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
    #     # Recria o schema vazio
    #     conn.execute(text("CREATE SCHEMA public"))
    #     # (Opcional) remove a tabela de controle de migrações, se quiser garantir
    #     conn.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))
    #     # (Opcional) recria permissões padrão
    #     conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))
    #     conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    #     conn.commit()
            # conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    try:
        for base in [
            user_model.Base,
            geral_model.Base,
            connection_models.Base,
            task_models.Base,
            dbstatistics_models.Base,
            queryhistory_models.Base,
            dbstructure_models.Base,
        ]:
            base.metadata.drop_all(bind=engine)
        print("🗑️ Todas as tabelas removidas com sucesso.")
    except Exception as e:
        print(f"⚠️ Erro ao apagar tabelas: {e}")

    # Passo 2: Criar novamente
    try:
        for base in [
            user_model.Base,
            geral_model.Base,
            connection_models.Base,
            task_models.Base,
            dbstatistics_models.Base,
            queryhistory_models.Base,
            dbstructure_models.Base,
        ]:
            base.metadata.create_all(bind=engine)
        print("✅ Banco de dados recriado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao recriar tabelas: {e}")