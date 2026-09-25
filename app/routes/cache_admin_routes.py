"""
🗄️ Cache e dados locais, por utilizador.

Serve a secção "Cache & Dados Locais" da aba Monitoramento & Sistema: ver o
estado de um utilizador, limpar o cache dele e ligar ou desligar a consulta a
dados locais.

Fica atrás de `settings:system` — mexer no cache de outra pessoa é administração
do sistema, não uma preferência própria. Cada utilizador continua a poder mudar
a sua em `PATCH /geral/settings/me`.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.config.user_cache_policy import (
    definir_dados_locais,
    limpar_cache_do_utilizador,
    obter_geracao,
    usa_dados_locais,
)
from app.database import get_db
from app.models.geral_model import Settings
from app.models.user_model import User
from app.ultils.logger import log_message
from app.ultils.permissions import require_permission

from app.ultils.permissions import get_current_user

router = APIRouter(
    prefix="/system/cache",
    tags=["Cache & Dados Locais"],
    dependencies=[Depends(require_permission("settings:system"))],
)


# ══════════════════════════ modelos ══════════════════════════
class EstadoCacheUtilizador(BaseModel):
    user_id: int
    nome: str
    email: str
    usar_dados_locais: bool = Field(
        description="False = ignora metadados em cache e lê sempre da origem."
    )
    geracao: int = Field(
        description="Versão do cache. Sobe a cada limpeza; as entradas antigas "
        "deixam de ser alcançáveis."
    )


class AlterarDadosLocais(BaseModel):
    usar_dados_locais: bool


class ResultadoLimpeza(BaseModel):
    user_id: int
    geracao: int
    mensagem: str


# ══════════════════════════ helpers ══════════════════════════
def _utilizador_ou_404(db: Session, user_id: int) -> User:
    utilizador = db.query(User).filter(User.id == user_id).first()
    if not utilizador:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Utilizador {user_id} não encontrado.",
        )
    return utilizador


def _settings_de(db: Session, user_id: int) -> Optional[Settings]:
    return db.query(Settings).filter(Settings.user_id == user_id).first()


def _estado(db: Session, utilizador: User) -> EstadoCacheUtilizador:
    definicoes = _settings_de(db, utilizador.id)

    # A base de dados manda; o Redis é só o espelho que o cache consulta a
    # cada leitura. Se divergirem — Redis reiniciado, por exemplo — é a
    # preferência gravada que conta.
    if definicoes is not None:
        ativo = bool(definicoes.usar_dados_locais)
    else:
        ativo = usa_dados_locais(utilizador.id)

    return EstadoCacheUtilizador(
        user_id=utilizador.id,
        nome=utilizador.nome,
        email=utilizador.email,
        usar_dados_locais=ativo,
        geracao=obter_geracao(utilizador.id),
    )


# ══════════════════════════ rotas ══════════════════════════
@router.get(
    "/users",
    response_model=List[EstadoCacheUtilizador],
    summary="Estado de cache de todos os utilizadores",
)
def listar_estados(db: Session = Depends(get_db)):
    """Alimenta o seletor da aba: quem existe e quem tem dados locais desligados."""
    return [_estado(db, u) for u in db.query(User).order_by(User.nome).all()]


@router.get(
    "/users/{user_id}",
    response_model=EstadoCacheUtilizador,
    summary="Estado de cache de um utilizador",
)
def obter_estado(user_id: int, db: Session = Depends(get_db)):
    return _estado(db, _utilizador_ou_404(db, user_id))


@router.post(
    "/users/{user_id}/clear",
    response_model=ResultadoLimpeza,
    summary="Limpar o cache de um utilizador",
)
def limpar(
    user_id: int,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    Invalida tudo o que este utilizador tem em cache.

    Não afeta mais ninguém: só as chaves dele deixam de ser alcançáveis, porque
    a geração entra no hash da chave.
    """
    utilizador = _utilizador_ou_404(db, user_id)
    geracao = limpar_cache_do_utilizador(utilizador.id)

    log_message(
        f"[cache] {ator.email} limpou o cache de {utilizador.email} "
        f"(geração {geracao})",
        "warning",
    )
    return ResultadoLimpeza(
        user_id=utilizador.id,
        geracao=geracao,
        mensagem=f"Cache de {utilizador.nome} limpo.",
    )


@router.patch(
    "/users/{user_id}",
    response_model=EstadoCacheUtilizador,
    summary="Ligar ou desligar dados locais para um utilizador",
)
def alterar_dados_locais(
    user_id: int,
    corpo: AlterarDadosLocais,
    db: Session = Depends(get_db),
    ator: User = Depends(get_current_user),
):
    """
    Com `usar_dados_locais=False`, este utilizador passa a ler sempre da origem:
    nomes de tabelas, colunas, enums e contagens deixam de vir de cache.

    A preferência é gravada em `Settings` e espelhada no Redis, que é onde o
    cache a consulta.
    """
    utilizador = _utilizador_ou_404(db, user_id)

    definicoes = _settings_de(db, user_id)
    if definicoes is None:
        # Um utilizador pode nunca ter aberto as preferências.
        definicoes = Settings(user_id=user_id)
        db.add(definicoes)

    definicoes.usar_dados_locais = corpo.usar_dados_locais
    db.commit()

    definir_dados_locais(user_id, corpo.usar_dados_locais)

    log_message(
        f"[cache] {ator.email} definiu dados locais de {utilizador.email} "
        f"para {corpo.usar_dados_locais}",
        "info",
    )
    return _estado(db, utilizador)
