# reset_db.py
from app import database
from app.config.dotenv import get_env
from app.models import user_model

def recreate_db():
    print("⚠️ Apagando e recriando o banco de dados...")
    user_model.Base.metadata.drop_all(bind=database.engine)
    user_model.Base.metadata.create_all(bind=database.engine)
    print("✅ Banco recriado com sucesso.")

if __name__ == "__main__":
    if get_env("ENV") == "dev":
        recreate_db()
    else:
        print("❌ Operação bloqueada fora do ambiente de desenvolvimento.")
