"""
Política de cache por utilizador.

Duas garantias que interessam:

1. Limpar o cache de uma pessoa não mexe no de mais ninguém. As chaves são
   `cache:{função}:{sha256}` com o utilizador dentro do hash, portanto a
   limpeza é feita por versão — e é fácil versionar demais e apanhar toda a
   gente sem dar por isso.
2. Com dados locais desligados, a função decorada corre SEMPRE. É o ponto todo
   da opção: ver o estado real do schema, não a fotografia.
"""

import base64
import os

import pytest

os.environ.setdefault(
    "ENCRYPTION_KEY", base64.urlsafe_b64encode(b"chave-de-teste-32-bytes-exatos!!").decode()
)

from app.config import user_cache_policy as politica  # noqa: E402
from app.config.cache_manager import cache_result  # noqa: E402


@pytest.fixture
def redis_falso(monkeypatch):
    """Substitui o Redis por um dicionário, para o teste não depender de um servidor."""
    memoria = {}

    monkeypatch.setattr(politica, "read_cache", lambda k: memoria.get(k))
    monkeypatch.setattr(
        politica, "write_cache", lambda k, v, ttl=None: memoria.__setitem__(k, v)
    )

    import app.config.cache_manager as cm

    # O cache_manager tem a sua própria cópia dos nomes importados.
    monkeypatch.setattr(cm, "obter_geracao", politica.obter_geracao)
    monkeypatch.setattr(cm, "usa_dados_locais", politica.usa_dados_locais)
    # L2 (Redis) fora do caminho: o que se testa aqui é a política, não o Redis.
    monkeypatch.setattr(cm, "read_cache", lambda k: None)
    monkeypatch.setattr(cm, "write_cache", lambda k, v, ttl=None: None)
    monkeypatch.setattr(cm, "MEMORY_CACHE", {})

    return memoria


# ══════════════════════════ geração ══════════════════════════
def test_geracao_comeca_a_zero(redis_falso):
    assert politica.obter_geracao(7) == 0


def test_limpar_incrementa_a_geracao(redis_falso):
    assert politica.limpar_cache_do_utilizador(7) == 1
    assert politica.limpar_cache_do_utilizador(7) == 2
    assert politica.obter_geracao(7) == 2


def test_limpar_um_utilizador_nao_afeta_outro(redis_falso):
    """A garantia que dá sentido a existir uma geração por pessoa."""
    politica.limpar_cache_do_utilizador(7)
    politica.limpar_cache_do_utilizador(7)

    assert politica.obter_geracao(7) == 2
    assert politica.obter_geracao(99) == 0


def test_redis_em_baixo_nao_parte_a_leitura(redis_falso, monkeypatch):
    """Sem cache é lento; a recusar pedidos era pior."""
    def rebenta(_):
        raise ConnectionError("redis indisponível")

    monkeypatch.setattr(politica, "read_cache", rebenta)

    assert politica.obter_geracao(7) == 0
    assert politica.usa_dados_locais(7) is True


# ══════════════════════════ dados locais ══════════════════════════
def test_dados_locais_ligados_por_omissao(redis_falso):
    assert politica.usa_dados_locais(7) is True


def test_desligar_e_voltar_a_ligar(redis_falso):
    politica.definir_dados_locais(7, False)
    assert politica.usa_dados_locais(7) is False

    politica.definir_dados_locais(7, True)
    assert politica.usa_dados_locais(7) is True


def test_voltar_a_ligar_invalida_o_que_estava_guardado(redis_falso):
    """
    Enquanto esteve desligado nada foi escrito, mas o que lá estava de antes
    continuava guardado — e voltaria a ser servido, possivelmente obsoleto.
    """
    politica.definir_dados_locais(7, False)
    geracao_antes = politica.obter_geracao(7)

    politica.definir_dados_locais(7, True)

    assert politica.obter_geracao(7) > geracao_antes


# ══════════════════════════ efeito no cache ══════════════════════════
def _funcao_contada():
    chamadas = {"n": 0}

    @cache_result(ttl=60, user_id="tabelas_{user_id}")
    def listar_tabelas(connection_id: int, user_id: int):
        chamadas["n"] += 1
        return ["clientes", "faturas"]

    return listar_tabelas, chamadas


def test_com_dados_locais_a_funcao_so_corre_uma_vez(redis_falso):
    listar, chamadas = _funcao_contada()

    listar(10, 7)
    listar(10, 7)

    assert chamadas["n"] == 1


def test_sem_dados_locais_a_funcao_corre_sempre(redis_falso):
    listar, chamadas = _funcao_contada()
    politica.definir_dados_locais(7, False)

    listar(10, 7)
    listar(10, 7)
    listar(10, 7)

    assert chamadas["n"] == 3


def test_desligar_para_um_nao_desliga_para_outro(redis_falso):
    listar, chamadas = _funcao_contada()
    politica.definir_dados_locais(7, False)

    listar(10, 7)   # sem cache  -> corre
    listar(10, 7)   # sem cache  -> corre
    listar(10, 9)   # com cache  -> corre e guarda
    listar(10, 9)   # com cache  -> não corre

    assert chamadas["n"] == 3


def test_limpar_o_cache_obriga_a_consultar_de_novo(redis_falso):
    listar, chamadas = _funcao_contada()

    listar(10, 7)
    assert chamadas["n"] == 1

    politica.limpar_cache_do_utilizador(7)

    listar(10, 7)
    assert chamadas["n"] == 2


def test_limpar_o_cache_de_um_nao_obriga_o_outro_a_reconsultar(redis_falso):
    listar, chamadas = _funcao_contada()

    listar(10, 7)
    listar(10, 9)
    assert chamadas["n"] == 2

    politica.limpar_cache_do_utilizador(7)

    listar(10, 9)   # o 9 não foi tocado: continua servido do cache
    assert chamadas["n"] == 2

    listar(10, 7)   # o 7 foi limpo: volta a correr
    assert chamadas["n"] == 3


def test_o_dono_e_o_user_id_e_nao_o_primeiro_inteiro(redis_falso):
    """
    Em `f(connection_id, user_id, db)` o primeiro inteiro é a conexão.

    Resolver o dono por "primeiro inteiro nos argumentos" atribuiria o cache à
    pessoa errada — limpar o cache do utilizador 7 não teria efeito, e limpar o
    da conexão 7 teria.
    """
    listar, chamadas = _funcao_contada()

    listar(7, 99)          # connection_id=7, user_id=99
    assert chamadas["n"] == 1

    politica.limpar_cache_do_utilizador(7)   # limpa o UTILIZADOR 7, não a conexão
    listar(7, 99)
    assert chamadas["n"] == 1, "o cache do utilizador 99 não devia ter sido tocado"

    politica.limpar_cache_do_utilizador(99)
    listar(7, 99)
    assert chamadas["n"] == 2
