"""
⚙️ Leitura e escrita das definições globais.

O catálogo (`DEFINICOES`) é a fonte da verdade: nome, tipo, valor por omissão,
descrição e o que a opção faz de facto. O frontend desenha a lista a partir
daqui, em vez de a manter numa cópia própria que fica dessincronizada.

Há um cache em processo com TTL curto porque o modo de manutenção é consultado
em **todos** os pedidos — sem ele, ligar a manutenção acrescentaria uma query à
base de dados a cada chamada da API. O TTL é o preço: uma alteração pode demorar
até `TTL_CACHE` segundos a chegar aos outros workers (o `start.bat` corre com
`--workers 4`). Quem alterou vê o efeito imediato, porque a escrita limpa o
cache do próprio processo.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.system_settings_models import SystemSetting
from app.ultils.logger import log_message

#: Segundos que um valor lido fica válido em memória.
TTL_CACHE = 10.0


@dataclass(frozen=True)
class Definicao:
    key: str
    titulo: str
    descricao: str
    default: bool
    criticidade: str  # low | medium | high
    #: Permissão necessária para alterar.
    permissao: str
    #: False quando a opção é guardada mas ainda não tem nada a consumi-la.
    #: Mostrado no frontend — um interruptor que finge agir é pior que nenhum.
    ativa: bool = True


DEFINICOES: List[Definicao] = [
    Definicao(
        key="maintenance_mode",
        titulo="Modo Manutenção",
        descricao=(
            "Recusa pedidos de quem não é administrador, com 503. "
            "O login e os endpoints de health continuam abertos."
        ),
        default=False,
        criticidade="high",
        permissao="settings:system",
    ),
    Definicao(
        key="strict_audit",
        titulo="Modo de Auditoria Rigorosa",
        descricao=(
            "Regista o corpo dos pedidos POST, PUT, PATCH e DELETE. "
            "Palavras-passe e tokens são substituídos antes de gravar."
        ),
        default=False,
        criticidade="medium",
        permissao="audit:read",
    ),
    Definicao(
        key="debug_logs",
        titulo="Debug Logs",
        descricao="Passa o logger para nível DEBUG, sem reiniciar o serviço.",
        default=False,
        criticidade="low",
        permissao="logs:view",
    ),
    Definicao(
        key="auto_backup",
        titulo="Backup Incremental",
        descricao=(
            "Preferência guardada, mas ainda sem agendador que a execute. "
            "Ligar não faz correr backups."
        ),
        default=False,
        criticidade="medium",
        permissao="backup:configure",
        ativa=False,
    ),
]

_POR_CHAVE: Dict[str, Definicao] = {d.key: d for d in DEFINICOES}

# chave -> (valor, instante da leitura)
_cache: Dict[str, tuple[bool, float]] = {}


def _serializar(valor: bool) -> str:
    return "true" if valor else "false"


def _desserializar(texto: str) -> bool:
    return str(texto).strip().lower() in {"true", "1", "yes", "on"}


def definicao(key: str) -> Definicao:
    if key not in _POR_CHAVE:
        raise KeyError(f"Definição desconhecida: '{key}'.")
    return _POR_CHAVE[key]


def limpar_cache() -> None:
    _cache.clear()


def obter(db: Session, key: str) -> bool:
    """Valor atual, com cache curto. Nunca levanta por falha de base de dados."""
    d = definicao(key)

    em_cache = _cache.get(key)
    if em_cache and (time.time() - em_cache[1]) < TTL_CACHE:
        return em_cache[0]

    try:
        linha = db.query(SystemSetting).filter(SystemSetting.key == key).first()
        valor = _desserializar(linha.value) if linha else d.default
    except Exception as e:  # noqa: BLE001
        # Uma definição ilegível não pode derrubar a aplicação: cai no default,
        # que para todas as opções é o comportamento normal.
        log_message(f"[system-settings] falha a ler '{key}': {e}", "warning")
        return d.default

    _cache[key] = (valor, time.time())
    return valor


def obter_todas(db: Session) -> Dict[str, bool]:
    return {d.key: obter(db, d.key) for d in DEFINICOES}


def definir(
    db: Session, key: str, valor: bool, *, ator_id: Optional[int] = None
) -> bool:
    """Grava e invalida o cache deste processo."""
    definicao(key)  # valida a chave antes de escrever

    linha = db.query(SystemSetting).filter(SystemSetting.key == key).first()
    if linha is None:
        linha = SystemSetting(key=key)
        db.add(linha)

    linha.value = _serializar(valor)
    linha.updated_by_id = ator_id
    db.commit()

    _cache[key] = (valor, time.time())
    _aplicar_efeitos(key, valor)

    log_message(
        f"[system-settings] '{key}' = {valor} (por user_id={ator_id})", "warning"
    )
    return valor


def _aplicar_efeitos(key: str, valor: bool) -> None:
    """
    Efeitos que têm de acontecer no momento, e não à próxima leitura.

    O nível do logger é global ao processo: mudá-lo aqui evita ter de consultar
    a definição a cada linha escrita.
    """
    if key == "debug_logs":
        try:
            import logging

            from app.ultils import logger as modulo_logger

            nivel = logging.DEBUG if valor else logging.INFO
            for nome in ("app", "uvicorn.error", "uvicorn.access"):
                logging.getLogger(nome).setLevel(nivel)
            interno = getattr(modulo_logger, "logger", None)
            if interno is not None:
                interno.setLevel(nivel)
        except Exception as e:  # noqa: BLE001
            log_message(f"[system-settings] falha a aplicar debug_logs: {e}", "warning")


def descrever(db: Session) -> List[Dict[str, Any]]:
    """Catálogo + valores atuais, para o frontend desenhar a lista."""
    atuais = obter_todas(db)
    return [
        {
            "key": d.key,
            "titulo": d.titulo,
            "descricao": d.descricao,
            "criticidade": d.criticidade,
            "permissao": d.permissao,
            "ativa": d.ativa,
            "valor": atuais[d.key],
        }
        for d in DEFINICOES
    ]
