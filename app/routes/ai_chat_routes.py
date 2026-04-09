# routes/chat_routes.py

import traceback

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.database import get_db

# 🔹 Certifica-te de que o nome do ficheiro está correto (ai_chat_cruds ou chat_service)
from app.cruds.ai_chat_cruds import (
    create_session,
    create_message,
    get_messages,
    get_session_by_id,
    get_sessions,
    delete_session,
    add_message_feedback,  # 🔥 Novo
    log_ai_usage,  # 🔥 Novo
)
from app.schemas.ai_chat_schemas import (
    ChatSessionCreate,
    ChatSessionResponse,
    ChatSessionDetailResponse,  # 🔥 Novo
    MessageCreate,
    MessageResponse,
    FeedbackCreate,  # 🔥 Novo
)
from app.config.ai_config import GeminiService
from app.ultils.logger import log_message


# 🔥 instância singleton
gemini_client = GeminiService()


# 👉 depois substitui por auth real
def get_current_user_id():
    return 1


router = APIRouter(prefix="/chat", tags=["Chat"])


# ============================================================
# 🧠 CRIAR SESSÃO
# ============================================================
@router.post("/session", response_model=ChatSessionResponse)
async def create_chat_session(
    data: ChatSessionCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        if data.title and len(data.title) > 255:
            raise HTTPException(400, "Título muito longo")

        return create_session(db, user_id, data.title)

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"[create_session] DB error: {str(e)}", level="error")
        raise HTTPException(500, "Erro ao criar sessão")


# ============================================================
# 📋 LISTAR SESSÕES
# ============================================================
@router.get("/sessions", response_model=list[ChatSessionResponse])
async def list_sessions(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return get_sessions(db, user_id)

    except Exception as e:
        log_message(
            f"[list_sessions] error: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro ao buscar sessões")


# ============================================================
# 🔍 BUSCAR SESSÃO COM MENSAGENS (NOVO)
# ============================================================
@router.get("/session/{session_id}", response_model=ChatSessionDetailResponse)
def get_session_details(
    session_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Retorna a sessão de chat e todo o histórico de mensagens associado a ela de uma vez.
    Ideal para quando o utilizador clica numa conversa no histórico.
    """
    session = get_session_by_id(db, session_id)

    if not session or session.user_id != user_id:
        log_message(
            f"Acesso negado à sessão {session_id} pelo utilizador {user_id}", "warning"
        )
        raise HTTPException(404, "Sessão não encontrada")

    return session


# ============================================================
# 💬 LISTAR MENSAGENS (Apenas as mensagens)
# ============================================================
@router.get("/messages/{session_id}", response_model=list[MessageResponse])
async def list_messages(
    session_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        session = get_session_by_id(db, session_id)

        if not session or session.user_id != user_id:
            log_message(
                f"[list_messages] acesso inválido | session={session_id} user={user_id}",
                level="warning",
            )
            raise HTTPException(404, "Sessão não encontrada")

        return get_messages(db, session_id)

    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"[list_messages] error: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro ao buscar mensagens")


# ============================================================
# ✉️ ENVIAR MENSAGEM + IA
# ============================================================
@router.post("/send/{session_id}")
async def send_message(
    session_id: int,
    data: MessageCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        content = (data.content or "").strip()
        role = data.__dict__.get(
            "role", "user"
        )  # Podes enviar o role opcionalmente, mas default é "user"
        if not content:
            raise HTTPException(400, "Mensagem vazia")

        if len(content) > 13000:
            raise HTTPException(400, "Mensagem muito longa")

        session = get_session_by_id(db, session_id)
        if not session or session.user_id != user_id:
            raise HTTPException(404, "Sessão inválida")
        # 💾 salva mensagem do user
        user_msg = create_message(db, session_id, "user", content)

        # 🧠 histórico
        history = get_messages(db, session_id)
        mensagens = [{"role": m.role, "content": m.content} for m in history[-10:]]

        # 🤖 IA
        modelo_utilizado = (
            gemini_client.models_priority[0]
            # Ou o que o teu GeminiService estiver a usar
        )
        try:
            # Podes adaptar o teu GeminiService para retornar os tokens também, mas por enquanto:
            resposta = gemini_client.gerar_com_contexto(mensagens)

            tokens_in = (
                len(content) // 4
            )  # Estimativa muito básica se não tiveres os tokens reais
            tokens_out = len(resposta) // 4

            # 🔥 Registar consumo
            log_ai_usage(db, user_id, modelo_utilizado, tokens_in, tokens_out)

        except Exception as ai_error:
            log_message(
                f"[AI] erro: {str(ai_error)}{traceback.format_exc()}", level="error"
            )
            resposta = "⚠️ Erro ao gerar resposta. Tenta novamente."

        # 💾 salva resposta da IA com o modelo que foi usado
        ai_msg = create_message(
            db, session_id, "assistant", resposta, model_used=modelo_utilizado
        )

        return {
            "message_id": ai_msg.id,  # 🔥 Útil para o frontend saber o ID caso queira dar feedback
            "message": user_msg.content,
            "response": ai_msg.content,
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[send_message] DB error: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro ao salvar mensagem")
    except Exception as e:
        db.rollback()
        log_message(
            f"[send_message] error: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro interno")


# ============================================================
# ⭐ ADICIONAR FEEDBACK / REAÇÃO (NOVO)
# ============================================================
@router.post("/message/{message_id}/feedback")
async def rate_ai_message(
    message_id: int,
    data: FeedbackCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Permite ao utilizador dar uma nota (1 a 5) ou comentário na resposta da IA.
    """
    try:
        # Podes adicionar validação aqui para garantir que a mensagem pertence ao user_id
        # chamando db.query(Message)... mas o crud trata a inserção segura.

        feedback = add_message_feedback(db, message_id, data.rating, data.comment)
        return {"ok": True, "feedback_id": feedback.id}

    except Exception as e:
        log_message(
            f"[rate_ai_message] erro: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro ao salvar o teu feedback.")


# ============================================================
# ❌ DELETAR SESSÃO
# ============================================================
@router.delete("/session/{session_id}")
async def remove_session(
    session_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        session = get_session_by_id(db, session_id)

        if not session or session.user_id != user_id:
            raise HTTPException(404, "Sessão não encontrada")

        delete_session(db, session_id)

        return {"ok": True}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[delete_session] DB error: {str(e)}{traceback.format_exc()}",
            level="error",
        )
        raise HTTPException(500, "Erro ao deletar sessão")
    except Exception as e:
        db.rollback()
        log_message(
            f"[delete_session] error: {str(e)}{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro interno")


# ============================================================
# ⚡ STREAMING DE RESPOSTA (NOVO)
# ============================================================


@router.post("/send-stream/{session_id}")
async def send_message_stream(
    session_id: int,
    data: MessageCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        content = (data.content or "").strip()

        if not content:
            raise HTTPException(400, "Mensagem vazia")

        if len(content) > 9000:
            raise HTTPException(400, "Mensagem muito longa")

        # 🔍 validar sessão
        session = get_session_by_id(db, session_id)
        if not session or session.user_id != user_id:
            raise HTTPException(404, "Sessão inválida")

        # 💾 salva mensagem do user
        create_message(db, session_id, "user", content)

        # 🧠 histórico
        history = get_messages(db, session_id)
        mensagens = [{"role": m.role, "content": m.content} for m in history[-10:]]

        def stream_generator():
            full_response = ""
            tokens_out = 0

            try:
                # 🔥 STREAM vindo do GeminiService
                for chunk in gemini_client.gerar_stream_com_contexto(mensagens):
                    full_response += chunk
                    tokens_out += len(chunk.split())  # Estimativa
                    yield chunk  # 👈 envia chunk para frontend

            except Exception as e:
                log_message(
                    f"[STREAM] erro: {str(e)}\n{traceback.format_exc()}", level="error"
                )
                yield "\n⚠️ Erro durante a geração da resposta."

            finally:
                # 🛡️ O BLOCO FINALLY: Garante que salva mesmo se o cliente desconectar!
                if full_response.strip():
                    try:
                        # 📊 log simples
                        tokens_in = len(content.split())
                        log_ai_usage(
                            db,
                            user_id,
                            "gemini-2.5-flash-stream",  # Guardar o nome real ajuda nas métricas
                            tokens_in,
                            tokens_out,
                        )

                        # 💾 guarda resposta (completa ou parcial) no fim
                        create_message(
                            db,
                            session_id,
                            "assistant",
                            full_response,
                            model_used="gemini-2.5-flash-stream",
                        )
                    except Exception as db_error:
                        log_message(
                            f"[STREAM DB SAVE] erro: {str(db_error)}", level="error"
                        )

        return StreamingResponse(stream_generator(), media_type="text/plain")

    except HTTPException:
        raise

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"[send_stream] DB error: {str(e)}\n{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro ao salvar mensagem")

    except Exception as e:
        db.rollback()
        log_message(
            f"[send_stream] error: {str(e)}\n{traceback.format_exc()}", level="error"
        )
        raise HTTPException(500, "Erro interno")
