"""
Definições globais e o que elas realmente fazem.

Um interruptor que só muda uma linha na base de dados não é uma funcionalidade.
O que aqui se testa é o efeito: a redação de segredos na auditoria rigorosa, as
rotas que continuam abertas em manutenção, e o comportamento quando as
definições não são legíveis.
"""

import base64
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.database import Base  # noqa: E402
from app.middleware.system_guard import (  # noqa: E402
    REDIGIDO,
    ROTAS_SEMPRE_ABERTAS,
    _e_sensivel,
    _redigir,
    _rota_aberta,
)
from app.services import system_settings_service as svc  # noqa: E402


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessao = sessionmaker(bind=engine)()
    svc.limpar_cache()
    try:
        yield sessao
    finally:
        sessao.close()
        svc.limpar_cache()


# ══════════════════════════ catálogo e valores ══════════════════════════
def test_defaults_mantem_o_comportamento_anterior(db):
    """
    Nada muda para quem só migra a base de dados.

    Manutenção e auditoria a False é o que o sistema fazia antes de existirem.
    """
    valores = svc.obter_todas(db)
    assert valores["maintenance_mode"] is False
    assert valores["strict_audit"] is False
    assert valores["debug_logs"] is False


def test_gravar_e_ler(db):
    svc.definir(db, "maintenance_mode", True, ator_id=1)
    assert svc.obter(db, "maintenance_mode") is True

    svc.definir(db, "maintenance_mode", False, ator_id=1)
    assert svc.obter(db, "maintenance_mode") is False


def test_chave_desconhecida_e_recusada(db):
    with pytest.raises(KeyError):
        svc.definir(db, "modo_turbo", True)
    with pytest.raises(KeyError):
        svc.obter(db, "modo_turbo")


def test_base_de_dados_ilegivel_cai_no_default(db):
    """Uma definição que não se consegue ler não pode derrubar a aplicação."""
    class SessaoPartida:
        def query(self, *a, **kw):
            raise RuntimeError("base de dados em baixo")

    svc.limpar_cache()
    assert svc.obter(SessaoPartida(), "maintenance_mode") is False


def test_catalogo_diz_o_que_ainda_nao_age(db):
    """
    `auto_backup` é guardado mas não tem agendador.

    A aba mostra isso; um interruptor que finge agir é pior do que nenhum.
    """
    catalogo = {d["key"]: d for d in svc.descrever(db)}
    assert catalogo["auto_backup"]["ativa"] is False
    assert catalogo["maintenance_mode"]["ativa"] is True
    assert catalogo["strict_audit"]["ativa"] is True


# ══════════════════════════ auditoria rigorosa ══════════════════════════
@pytest.mark.parametrize(
    "campo",
    [
        "password",
        "Password",
        "nova_senha",
        "hashed_password",
        "access_token",
        "API_KEY",
        "client_secret",
        "authorization",
    ],
)
def test_campos_sensiveis_sao_reconhecidos(campo):
    assert _e_sensivel(campo) is True


@pytest.mark.parametrize("campo", ["nome", "email", "table_name", "query", "limit"])
def test_campos_normais_passam(campo):
    assert _e_sensivel(campo) is False


def test_redacao_apanha_segredos_aninhados():
    """
    Uma trilha que guarda credenciais em claro deixa de ser um controlo e passa
    a ser um problema de conformidade.
    """
    corpo = {
        "email": "ana@x.pt",
        "password": "muito-secreta",
        "connection": {"host": "db.local", "password": "outra-secreta"},
        "utilizadores": [
            {"nome": "Ana", "token": "abc123"},
            {"nome": "Rui", "senha": "xyz"},
        ],
    }

    limpo = _redigir(corpo)

    assert limpo["email"] == "ana@x.pt"                      # dado normal fica
    assert limpo["connection"]["host"] == "db.local"
    assert limpo["password"] == REDIGIDO
    assert limpo["connection"]["password"] == REDIGIDO
    assert limpo["utilizadores"][0]["token"] == REDIGIDO
    assert limpo["utilizadores"][1]["senha"] == REDIGIDO
    assert limpo["utilizadores"][0]["nome"] == "Ana"

    # E nenhum dos segredos sobrevive em lado nenhum da estrutura.
    import json

    texto = json.dumps(limpo)
    for segredo in ("muito-secreta", "outra-secreta", "abc123", "xyz"):
        assert segredo not in texto


# ══════════════════════════ modo manutenção ══════════════════════════
@pytest.mark.parametrize(
    "caminho",
    ["/auth/login", "/auth/refresh", "/health", "/health/ready", "/system/status"],
)
def test_manutencao_deixa_passar_o_essencial(caminho):
    """
    Sem o login e sem a aba de sistema, ligar a manutenção trancaria também
    quem a tem de desligar.
    """
    assert _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS) is True


@pytest.mark.parametrize(
    "caminho",
    ["/exe/update_row", "/sql-editor/execute", "/database/table/", "/conn/connections/"],
)
def test_manutencao_bloqueia_o_resto(caminho):
    assert _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS) is False
