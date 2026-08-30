"""
🗄️ Política de cache por utilizador: limpar e desligar.

Duas necessidades que o `cache_manager` sozinho não resolvia:

**Limpar o cache de UM utilizador.** As chaves são `cache:{função}:{sha256}` e
o utilizador vai dentro do hash — não há prefixo por onde varrer, e apagar por
padrão significaria apagar o cache de toda a gente. A saída é versionar: cada
utilizador tem um número de geração que entra na chave. Limpar é incrementar
esse número; as entradas antigas deixam de ser alcançáveis e morrem sozinhas
quando o TTL expira. É O(1) e não precisa de percorrer o Redis.

**Não consultar dados locais.** Nomes de tabelas, colunas e enums são
consultados uma vez e reutilizados. Quem está a mexer no schema quer ver o
estado real, não a fotografia. A flag vive em `Settings.usar_dados_locais`, mas
é lida a cada acesso ao cache — daí ficar espelhada aqui em Redis, para não
haver uma query à base de dados por leitura de cache.

O Redis pode estar em baixo. Nesse caso ambas as funções escolhem o lado
inofensivo: geração 0 (o cache continua a funcionar como antes) e cache ligado.
Perder a capacidade de limpar é um incómodo; recusar servir pedidos porque o
cache está indisponível seria bem pior.
"""

from __future__ import annotations

from typing import Optional

from app.config.redis import read_cache, write_cache
from app.ultils.logger import log_message

#: Sem TTL: a geração tem de sobreviver mais do que as entradas que invalida.
_GERACAO = "cachepolicy:gen:user:{user_id}"

#: Espelho de `Settings.usar_dados_locais`, para não ir à base de dados a cada
#: leitura de cache. Se faltar, assume-se que os dados locais são usados.
_DADOS_LOCAIS = "cachepolicy:local:user:{user_id}"


def _chave_geracao(user_id) -> str:
    return _GERACAO.format(user_id=user_id)


def _chave_dados_locais(user_id) -> str:
    return _DADOS_LOCAIS.format(user_id=user_id)


def obter_geracao(user_id) -> int:
    """
    Versão atual do cache deste utilizador. Entra na chave, por isso mudá-la
    invalida tudo o que ele tinha guardado.
    """
    if user_id is None:
        return 0
    try:
        valor = read_cache(_chave_geracao(user_id))
        return int(valor) if valor is not None else 0
    except Exception as e:  # noqa: BLE001
        log_message(f"[cache-policy] falha a ler geração de {user_id}: {e}", "warning")
        return 0


def limpar_cache_do_utilizador(user_id) -> int:
    """
    Invalida tudo o que este utilizador tem em cache. Devolve a nova geração.

    Não apaga chave nenhuma: incrementa a versão, e as entradas antigas ficam
    órfãs até o TTL as levar. Apagá-las à mão obrigaria a varrer o Redis inteiro
    e a desserializar cada chave para descobrir de quem era.
    """
    nova = obter_geracao(user_id) + 1
    try:
        write_cache(_chave_geracao(user_id), nova)
        log_message(
            f"[cache-policy] cache do utilizador {user_id} invalidado (geração {nova})",
            "info",
        )
    except Exception as e:  # noqa: BLE001
        log_message(f"[cache-policy] falha a limpar cache de {user_id}: {e}", "error")
        raise
    return nova


def usa_dados_locais(user_id) -> bool:
    """True se este utilizador pode ler e escrever no cache."""
    if user_id is None:
        return True
    try:
        valor = read_cache(_chave_dados_locais(user_id))
        # Ausente = nunca foi configurado = comportamento por omissão.
        return True if valor is None else bool(valor)
    except Exception as e:  # noqa: BLE001
        log_message(f"[cache-policy] falha a ler flag de {user_id}: {e}", "warning")
        return True


def definir_dados_locais(user_id, ativo: bool) -> None:
    """
    Espelha a preferência no Redis.

    Ao voltar a ligar, incrementa a geração: enquanto esteve desligado o
    utilizador não escreveu nada no cache, mas o que lá estava de antes
    continuava guardado e voltaria a ser servido — possivelmente já obsoleto,
    que é exatamente o que ele estava a tentar evitar.
    """
    try:
        write_cache(_chave_dados_locais(user_id), bool(ativo))
        if ativo:
            limpar_cache_do_utilizador(user_id)
    except Exception as e:  # noqa: BLE001
        log_message(f"[cache-policy] falha a definir flag de {user_id}: {e}", "error")
        raise


def sincronizar_das_settings(user_id, usar_dados_locais_bd: Optional[bool]) -> None:
    """
    Alinha o espelho com o que está guardado em `Settings`.

    Chamado quando as preferências são gravadas e no arranque de sessão, para
    que um Redis reiniciado não devolva silenciosamente o cache a quem o tinha
    desligado.
    """
    if usar_dados_locais_bd is None:
        return
    try:
        write_cache(_chave_dados_locais(user_id), bool(usar_dados_locais_bd))
    except Exception as e:  # noqa: BLE001
        log_message(f"[cache-policy] falha a sincronizar {user_id}: {e}", "warning")
