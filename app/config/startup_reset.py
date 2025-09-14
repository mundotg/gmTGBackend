import os
import pickle
from sqlalchemy import text
from app.config.dotenv import get_env  # ou ajuste conforme seu projeto
from app.database import sync_engine as engine
from app.models import user_model, geral_model, connection_models

FLAG_FILE = get_env('FLAG_FILE', 'app/config/initialized.pkl')
def already_initialized():
    return os.path.exists(FLAG_FILE)

def mark_initialized():
    with open((FLAG_FILE), "wb") as f:
        pickle.dump({"initialized": True}, f)

def reset_database():
    print("🔁 Resetando banco de dados...")
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))

    user_model.Base.metadata.create_all(bind=engine)
    geral_model.Base.metadata.create_all(bind=engine)
    # connection_models.Base.metadata.drop_all(bind=engine)
    connection_models.Base.metadata.create_all(bind=engine)
