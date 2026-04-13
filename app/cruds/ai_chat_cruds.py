# services/chat_service.py

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# 🔥 Adicionados Feedback e UsageLog aos imports
from app.models.ai_models import ChatSession, Message, Feedback, UsageLog
from app.ultils.logger import log_message


# ============================================================
# 🧠 CRIAR SESSÃO
# ============================================================
def create_session(db: Session, user_id: int, title: str | None = None):
    try:
        session = ChatSession(user_id=user_id, title=title or "Nova conversa")

        db.add(session)
        db.commit()
        db.refresh(session)

        log_message(
            f"Sessão de chat {session.id} criada para o utilizador {user_id}",
            level="info",
        )
        return session

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[create_session] Erro no DB (user: {user_id}): {str(e)}", level="error"
        )
        raise Exception("Erro ao criar sessão")


# ============================================================
# 📋 LISTAR SESSÕES
# ============================================================
def get_sessions(db: Session, user_id: int):
    try:
        return (
            db.query(ChatSession)
            .filter(ChatSession.user_id == user_id)
            .order_by(ChatSession.created_at.desc())
            .all()
        )
    except SQLAlchemyError as e:
        log_message(
            f"[get_sessions] Erro no DB (user: {user_id}): {str(e)}", level="error"
        )
        return []


# ============================================================
# 🔍 BUSCAR SESSÃO POR ID
# ============================================================
def get_session_by_id(db: Session, session_id: int):
    try:
        return db.get(ChatSession, session_id)
    except SQLAlchemyError as e:
        log_message(
            f"[get_session_by_id] Erro no DB (session: {session_id}): {str(e)}",
            level="error",
        )
        return None


# ============================================================
# 💬 BUSCAR MENSAGENS
# ============================================================
def get_messages(db: Session, session_id: int):
    try:
        return (
            db.query(Message)
            .filter(Message.session_id == session_id)
            .order_by(Message.created_at.asc())
            .all()
        )
    except SQLAlchemyError as e:
        log_message(
            f"[get_messages] Erro no DB (session: {session_id}): {str(e)}",
            level="error",
        )
        return []


# ============================================================
# ✉️ CRIAR MENSAGEM
# ============================================================
def create_message(
    db: Session,
    session_id: int,
    role: str,
    content: str,
    tokens: int | None = None,
    model_used: str | None = None,
):
    try:
        if not content:
            raise ValueError("O conteúdo da mensagem não pode estar vazio")

        msg = Message(
            session_id=session_id,
            role=role,
            content=content.strip(),
            tokens=tokens,
            model_used=model_used,
        )

        db.add(msg)
        db.commit()
        db.refresh(msg)

        return msg

    except ValueError as e:
        log_message(f"[create_message] Erro de validação: {str(e)}", level="warning")
        raise

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[create_message] Erro no DB (session: {session_id}): {str(e)}",
            level="error",
        )
        raise Exception("Erro ao salvar mensagem")


# ============================================================
# ❌ DELETAR SESSÃO
# ============================================================
def delete_session(db: Session, session_id: int):
    try:
        session = db.get(ChatSession, session_id)

        if not session:
            return False

        db.delete(session)
        db.commit()

        log_message(f"Sessão de chat {session_id} eliminada.", level="info")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[delete_session] Erro no DB (session: {session_id}): {str(e)}",
            level="error",
        )
        raise Exception("Erro ao deletar sessão")


# ============================================================
# ⭐ ADICIONAR REAÇÃO / FEEDBACK (NOVO)
# ============================================================
def add_message_feedback(
    db: Session, message_id: int, rating: int, comment: str | None = None
):
    """
    Adiciona ou atualiza o feedback (1 a 5 estrelas/reação) de uma mensagem específica.
    """
    try:
        # Verifica se já existe feedback para fazer update (evita duplicação)
        feedback = db.query(Feedback).filter(Feedback.message_id == message_id).first()

        if feedback:
            feedback.rating = rating
            feedback.comment = comment
            log_msg = f"Feedback atualizado na mensagem {message_id}"
        else:
            feedback = Feedback(message_id=message_id, rating=rating, comment=comment)
            db.add(feedback)
            log_msg = f"Feedback adicionado na mensagem {message_id}"

        db.commit()
        db.refresh(feedback)

        log_message(log_msg, level="info")
        return feedback

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[add_message_feedback] Erro no DB (msg: {message_id}): {str(e)}",
            level="error",
        )
        raise Exception("Erro ao processar feedback")


# ============================================================
# 📊 REGISTAR CONSUMO DA IA / LOG (NOVO)
# ============================================================
def log_ai_usage(
    db: Session, user_id: int, model: str, tokens_input: int, tokens_output: int
):
    """
    Guarda o registo de consumo de tokens para faturação e métricas de uso.
    """
    try:
        usage = UsageLog(
            user_id=user_id,
            model=model,
            tokens_input=tokens_input,
            tokens_output=tokens_output,
        )

        db.add(usage)
        db.commit()

        # Como isso acontece muito em background, evitamos poluir o log de terminal com "info"
        # Mantemos apenas em DB, a menos que haja erro.
        return usage

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[log_ai_usage] Erro no DB ao registar uso (user: {user_id}): {str(e)}",
            level="error",
        )
        # Não damos 'raise' aqui para que uma falha de log não quebre a resposta principal do chat
        return None
