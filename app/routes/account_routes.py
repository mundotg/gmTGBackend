"""
👤 A conta do próprio utilizador — perfil, palavra-passe, sessões e dados.

O frontend desta aba já estava escrito (`ComponentUsuarioTab/EditModal.tsx` e
`app/services/settingsApi.ts`) e apontava para sete rotas que nunca existiram:
guardar o perfil, mudar a palavra-passe, notificações, exportar, apagar a conta,
avatar e 2FA. Todas davam 404, portanto nenhum botão desta aba fazia nada.

Este módulo implementa-as com os mesmos caminhos e os mesmos nomes de campo que
o frontend já envia — o contrato é o que lá está, não um novo.

Fica de fora, de propósito, o `PUT /user/security/2fa`. Guardar um booleano
chamado "dois fatores ativo" sem existir segundo fator nenhum diz ao utilizador
que a conta está protegida quando não está; é a única mentira desta aba que
custa mais do que a falta da funcionalidade. O controlo saiu da interface.

Tudo aqui opera sobre a conta de quem está autenticado. Não há `user_id` em
nenhuma rota: quem quiser mexer noutra conta usa o RBAC em `/users`.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Response, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import hash_password, verify_password
from app.database import get_db
from app.models.connection_models import DBConnection
from app.models.geral_model import Settings
from app.models.queryhistory_models import QueryHistory
from app.models.task_models import Project, Task
from app.models.user_model import RefreshToken, User
from app.ultils.logger import log_message
from app.ultils.permissions import get_current_user

router = APIRouter(prefix="/user", tags=["Minha Conta"])

#: Onde ficam os avatares. Servido por `/static`, que o `main` já monta.
_DIR_AVATARES = Path(__file__).resolve().parent.parent / "static" / "avatares"

#: Mesmo mínimo do registo (`UserCreate.senha`), para não haver duas políticas.
MIN_SENHA = 8

EXTENSOES_AVATAR = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
MAX_AVATAR_BYTES = 2 * 1024 * 1024


# ══════════════════════════ modelos ══════════════════════════
class PerfilIn(BaseModel):
    nome: str = Field(min_length=1, max_length=255)
    apelido: Optional[str] = Field(default=None, max_length=100)
    telefone: Optional[str] = Field(default=None, max_length=30)
    avatar_url: Optional[str] = None
    # O frontend envia estes dois, mas cargo e empresa são organizacionais:
    # deixar alguém mudar a sua própria empresa era mudar de tenant sozinho.
    # Aceitam-se para não rejeitar o pedido, e são ignorados.
    cargo: Optional[str] = None
    empresa: Optional[str] = None


class PerfilOut(BaseModel):
    id: int
    nome: str
    apelido: Optional[str] = None
    email: str
    telefone: Optional[str] = None
    avatar_url: Optional[str] = None
    criado_em: Optional[datetime] = None


class MudarSenhaIn(BaseModel):
    senhaAtual: str = Field(min_length=1)
    novaSenha: str = Field(min_length=MIN_SENHA)


class NotificacoesIn(BaseModel):
    email: bool = True
    push: bool = True
    # Enviados pelo frontend; não há canal de SMS nem digest semanal no sistema,
    # por isso não se inventa coluna para eles.
    sms: Optional[bool] = None
    weeklyDigest: Optional[bool] = None


class NotificacoesOut(BaseModel):
    email: bool
    push: bool
    suportados: List[str]
    ignorados: List[str]


class SessaoOut(BaseModel):
    """
    Uma sessão, tal como o sistema a regista.

    Não há nome de navegador nem localização: o login guarda de propósito um
    **prefixo** de IP (`127.0.0`, não o endereço completo) e um **hash** do
    user-agent, para poder detetar que a sessão mudou de sítio sem armazenar o
    rasto de quem a usa. É menos legível do que "Chrome/Windows", e é o que
    existe — inventar o resto seria a mesma ficção que esta aba já mostrava.
    """

    id: int
    ip: Optional[str] = None
    #: Primeiros caracteres do hash do user-agent. Serve para distinguir
    #: sessões entre si, não para identificar o aparelho.
    dispositivo: Optional[str] = None
    criada_em: Optional[datetime] = None
    expira_em: Optional[datetime] = None
    ativa: bool
    atual: bool


# ══════════════════════════ perfil ══════════════════════════
@router.get("/profile", response_model=PerfilOut, summary="Dados do meu perfil")
def obter_perfil(ator: User = Depends(get_current_user)):
    return PerfilOut(
        id=ator.id,
        nome=ator.nome,
        apelido=ator.apelido,
        email=ator.email,
        telefone=ator.telefone,
        avatar_url=ator.avatar_url,
        criado_em=ator.created_at,
    )


@router.put("/profile", response_model=PerfilOut, summary="Atualizar o meu perfil")
def atualizar_perfil(
    dados: PerfilIn,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    O e-mail não se altera aqui: é a identidade de login e mudá-lo sem
    reverificação deixaria a conta acessível a partir de um endereço não provado.
    """
    ator.nome = dados.nome.strip()
    if dados.apelido is not None:
        ator.apelido = dados.apelido.strip() or None
    if dados.telefone is not None:
        ator.telefone = dados.telefone.strip() or None
    # Só se aceita um avatar que tenha saído de `POST /user/avatar`; uma URL
    # externa arbitrária faria o navegador de toda a gente ir buscá-la.
    if dados.avatar_url is not None and (
        dados.avatar_url.startswith("/static/avatares/") or dados.avatar_url == ""
    ):
        ator.avatar_url = dados.avatar_url or None

    db.commit()
    db.refresh(ator)

    log_message(f"[conta] {ator.email} atualizou o perfil", "info")
    return obter_perfil(ator)


@router.post("/avatar", summary="Carregar avatar")
async def carregar_avatar(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    extensao = Path(file.filename or "").suffix.lower()
    if extensao not in EXTENSOES_AVATAR:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Formato não suportado. Use: {', '.join(sorted(EXTENSOES_AVATAR))}.",
        )

    conteudo = await file.read()
    if len(conteudo) > MAX_AVATAR_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="A imagem excede 2 MB.",
        )

    _DIR_AVATARES.mkdir(parents=True, exist_ok=True)

    # Nome gerado, nunca o do ficheiro enviado: um nome vindo do cliente pode
    # trazer `../` e escrever fora da pasta.
    destino = _DIR_AVATARES / f"{ator.id}-{uuid.uuid4().hex}{extensao}"
    destino.write_bytes(conteudo)

    # Substituir o avatar deixaria o anterior a ocupar disco para sempre.
    anterior = ator.avatar_url
    if anterior and anterior.startswith("/static/avatares/"):
        antigo = _DIR_AVATARES / Path(anterior).name
        if antigo.exists() and antigo != destino:
            try:
                antigo.unlink()
            except OSError as e:
                log_message(f"[conta] não removeu o avatar antigo: {e}", "warning")

    ator.avatar_url = f"/static/avatares/{destino.name}"
    db.commit()

    return {"url": ator.avatar_url}


# ══════════════════════════ segurança ══════════════════════════
@router.put("/security/password", summary="Alterar a minha palavra-passe")
def alterar_senha(
    dados: MudarSenhaIn,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    Trocar a palavra-passe termina as outras sessões.

    Quem muda a palavra-passe costuma fazê-lo por suspeitar que alguém lhe
    acedeu à conta; deixar os refresh tokens antigos válidos tornaria a troca
    inútil contra exatamente essa pessoa.
    """
    if not verify_password(dados.senhaAtual, ator.hashed_password):
        log_message(
            f"[conta] palavra-passe atual errada ao tentar mudar | user_id={ator.id}",
            "warning",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A palavra-passe atual está incorreta.",
        )

    if verify_password(dados.novaSenha, ator.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A nova palavra-passe tem de ser diferente da atual.",
        )

    ator.hashed_password = hash_password(dados.novaSenha)

    terminadas = (
        db.query(RefreshToken)
        .filter(RefreshToken.user_id == ator.id, RefreshToken.revoked.is_(False))
        .update({"revoked": True, "is_active": False}, synchronize_session=False)
    )
    db.commit()

    log_message(
        f"[conta] {ator.email} mudou a palavra-passe; {terminadas} sessão(ões) terminadas",
        "warning",
    )
    return {
        "detail": "Palavra-passe alterada. Por segurança, as outras sessões foram terminadas.",
        "sessoes_terminadas": terminadas,
    }


@router.get(
    "/sessions", response_model=List[SessaoOut], summary="As minhas sessões"
)
def listar_sessoes(db: Session = Depends(get_db), ator: User = Depends(get_current_user)):
    """
    Sessões reais, a partir dos refresh tokens — com IP e dispositivo, que já
    eram guardados e nunca tinham sido mostrados a ninguém.
    """
    agora = datetime.now(timezone.utc)
    tokens = (
        db.query(RefreshToken)
        .filter(RefreshToken.user_id == ator.id)
        .order_by(RefreshToken.created_at.desc())
        .limit(20)
        .all()
    )

    resultado: List[SessaoOut] = []
    for t in tokens:
        expira = t.expires_at
        if expira is not None and expira.tzinfo is None:
            expira = expira.replace(tzinfo=timezone.utc)

        resultado.append(
            SessaoOut(
                id=t.id,
                ip=t.user_IP,
                # O valor guardado é um hash longo; encurta-se aqui para a
                # interface não ter de saber disso.
                dispositivo=(t.user_agent or "")[:10] or None,
                criada_em=t.created_at,
                expira_em=t.expires_at,
                ativa=bool(not t.revoked and (expira is None or expira > agora)),
                atual=False,
            )
        )
    return resultado


@router.delete("/sessions/{session_id}", summary="Terminar uma sessão")
def terminar_sessao(
    session_id: int,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    token = (
        db.query(RefreshToken)
        .filter(RefreshToken.id == session_id, RefreshToken.user_id == ator.id)
        .first()
    )
    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Sessão não encontrada."
        )

    token.revoked = True
    token.is_active = False
    db.commit()

    log_message(f"[conta] {ator.email} terminou a sessão {session_id}", "info")
    return {"detail": "Sessão terminada."}


# ══════════════════════════ notificações ══════════════════════════
def _settings_de(db: Session, user_id: int) -> Settings:
    definicoes = db.query(Settings).filter(Settings.user_id == user_id).first()
    if definicoes is None:
        definicoes = Settings(user_id=user_id)
        db.add(definicoes)
        db.flush()
    return definicoes


@router.put(
    "/settings/notifications",
    response_model=NotificacoesOut,
    summary="Preferências de notificação",
)
def atualizar_notificacoes(
    dados: NotificacoesIn,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    Só `email` e `push` são guardados — são os dois canais que o modelo
    `Settings` tem. A resposta diz explicitamente o que foi ignorado, para a
    interface não mostrar como guardado algo que não foi.
    """
    definicoes = _settings_de(db, ator.id)
    definicoes.email_notifications = dados.email
    definicoes.app_notifications = dados.push
    db.commit()

    ignorados = [
        nome
        for nome, valor in (("sms", dados.sms), ("weeklyDigest", dados.weeklyDigest))
        if valor is not None
    ]

    return NotificacoesOut(
        email=definicoes.email_notifications,
        push=definicoes.app_notifications,
        suportados=["email", "push"],
        ignorados=ignorados,
    )


# ══════════════════════════ dados da conta ══════════════════════════
@router.get("/export", summary="Exportar os meus dados")
def exportar_dados(db: Session = Depends(get_db), ator: User = Depends(get_current_user)):
    """
    O que o sistema guarda sobre esta pessoa, em JSON.

    Sem segredos: as palavras-passe das conexões e o hash da própria conta não
    entram. Um ficheiro de exportação costuma acabar no Transferências, e não
    deve valer mais do que os dados que descreve.
    """
    definicoes = db.query(Settings).filter(Settings.user_id == ator.id).first()

    conexoes = db.query(DBConnection).filter(DBConnection.user_id == ator.id).all()
    projetos = db.query(Project).filter(Project.owner_id == ator.id).all()
    tarefas = db.query(Task).filter(Task.assigned_to_id == ator.id).all()
    historico = (
        db.query(QueryHistory)
        .filter(QueryHistory.user_id == ator.id)
        .order_by(QueryHistory.executed_at.desc())
        .limit(500)
        .all()
    )

    return {
        "exportado_em": datetime.now(timezone.utc).isoformat(),
        "perfil": {
            "id": ator.id,
            "nome": ator.nome,
            "apelido": ator.apelido,
            "email": ator.email,
            "telefone": ator.telefone,
            "criado_em": ator.created_at.isoformat() if ator.created_at else None,
            "empresa": ator.empresa.nome if ator.empresa else None,
            "cargo": ator.cargo.nome if ator.cargo else None,
        },
        "preferencias": (
            {
                "tema": definicoes.theme,
                "idioma": definicoes.language,
                "fuso": definicoes.timezone,
                "notificacoes_email": definicoes.email_notifications,
                "notificacoes_app": definicoes.app_notifications,
                "usar_dados_locais": definicoes.usar_dados_locais,
            }
            if definicoes
            else None
        ),
        "conexoes": [
            {"id": c.id, "nome": c.name, "tipo": c.type, "base": c.database_name}
            for c in conexoes
        ],
        "projetos": [{"id": p.id, "nome": p.name} for p in projetos],
        "tarefas": [{"id": t.id, "titulo": t.title, "estado": t.status} for t in tarefas],
        "historico_de_queries": [
            {
                "query": h.query,
                "tipo": h.query_type,
                "executada_em": h.executed_at.isoformat() if h.executed_at else None,
                "duracao_ms": h.duration_ms,
            }
            for h in historico
        ],
    }


@router.delete("", summary="Desativar a minha conta")
@router.delete("/", include_in_schema=False)
def desativar_conta(
    db: Session = Depends(get_db), ator: User = Depends(get_current_user)
):
    """
    Desativa em vez de apagar.

    `User` tem cascatas para projetos, conexões e histórico: um DELETE levaria
    com ele o trabalho da equipa que está pendurado neste utilizador. Desativar
    tira o acesso de imediato — `load_actor` recusa contas inativas e as sessões
    são revogadas — e deixa a decisão de apagar mesmo para um administrador, que
    a pode tomar sabendo o que se perde.
    """
    ator.is_active = False
    db.query(RefreshToken).filter(RefreshToken.user_id == ator.id).update(
        {"revoked": True, "is_active": False}, synchronize_session=False
    )
    db.commit()

    log_message(f"[conta] {ator.email} desativou a própria conta", "warning")
    return {
        "detail": (
            "Conta desativada e sessões terminadas. "
            "Para a reativar, contacte um administrador."
        )
    }
