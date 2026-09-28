"""
🔗 Ligações a partir de uma URL (connection string) completa.

Porque existe
-------------
O caminho normal desta app compõe a URI a partir de campos separados
(`DatabaseManager.DB_URIS`). Mas os fornecedores modernos — Neon, Supabase,
Atlas, Railway, Render — entregam uma **URL única**, muitas vezes com opções
que não têm campo no formulário (`channel_binding`, `retryWrites`,
`options=endpoint%3D…`). Reconstruí-la a partir de campos perde essas opções e
a ligação falha por razões difíceis de diagnosticar.

Aqui a URL é usada **tal como o utilizador a colou**. Só se mexe no mínimo
indispensável:

* o esquema passa a incluir o driver que este backend tem instalado
  (`postgres://` → `postgresql+psycopg2://`, ou `+asyncpg` no caminho async);
* `localhost` é remapeado quando o backend corre em contentor, como já se faz
  para os campos separados (ver `_maybe_remap_host`);
* no caminho async com asyncpg, `sslmode`/`channel_binding` saem da query e
  passam a `connect_args` — o asyncpg não conhece esses parâmetros e rebentaria
  com TypeError;
* em PostgreSQL, parâmetros de ORM que não são do libpq (`pgbouncer`,
  `schema`, `connection_limit`, ... — comuns em URLs do Neon/Supabase/Prisma)
  saem da query, com tradução quando existe (`schema` → search_path,
  `pgbouncer=true` → sem cache de prepared statements). Deixados, o psycopg2
  recusava a URL inteira ("invalid connection option").

`parse_db_url` extrai host/porta/base/utilizador só para **preencher as colunas
da nossa tabela** (são NOT NULL e a listagem de conexões mostra host e base). A
ligação em si nunca é remontada a partir desses valores.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

from pymongo import MongoClient
from sqlalchemy import create_engine

from app.ultils.db_ssl import asyncpg_ssl_arg, resolve_pg_sslmode
from app.ultils.logger import log_message


# Esquemas aceites → chave usada em DATABASE_TYPES / DB_URIS.
SCHEME_TO_TYPE: Dict[str, str] = {
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "pg": "PostgreSQL",
    "mysql": "MySQL",
    "mariadb": "MariaDB",
    "mssql": "SQL Server",
    "sqlserver": "SQL Server",
    "oracle": "Oracle",
    "mongodb": "MongoDB",
    "sqlite": "SQLite",
    "sqlite3": "SQLite",
}

# Drivers instalados neste backend (ver requirements.txt).
SYNC_DRIVERS: Dict[str, str] = {
    "PostgreSQL": "postgresql+psycopg2",
    "MySQL": "mysql+pymysql",
    "MariaDB": "mariadb+mariadbconnector",
    "SQL Server": "mssql+pyodbc",
    "Oracle": "oracle+cx_oracle",
    "SQLite": "sqlite",
}

ASYNC_DRIVERS: Dict[str, str] = {
    "PostgreSQL": "postgresql+asyncpg",
    "MySQL": "mysql+aiomysql",
    "MariaDB": "mariadb+aiomysql",
    "SQL Server": "mssql+aioodbc",
    "Oracle": "oracle+cx_oracle",
    "SQLite": "sqlite+aiosqlite",
}

# Portas assumidas quando a URL não as indica (o campo `port` é NOT NULL).
DEFAULT_PORTS: Dict[str, int] = {
    "PostgreSQL": 5432,
    "MySQL": 3306,
    "MariaDB": 3306,
    "SQL Server": 1433,
    "Oracle": 1521,
    "MongoDB": 27017,
    "SQLite": 0,
}

_ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

# Palavras-chave de ligação do libpq (PostgreSQL 17, "Parameter Key Words").
# É o que o psycopg2 aceita na query de uma URL postgresql://. As URLs do Neon,
# Supabase ou copiadas do Prisma trazem também opções do ORM (`pgbouncer`,
# `schema`, `connection_limit`, `pool_timeout`, ...) que o libpq recusa com
# `invalid dsn: invalid connection option "pgbouncer"` — por isso a query é
# filtrada por esta lista antes de ligar.
_LIBPQ_PARAMS = {
    "host", "hostaddr", "port", "dbname", "user", "password", "passfile",
    "require_auth", "channel_binding", "connect_timeout", "client_encoding",
    "options", "application_name", "fallback_application_name", "keepalives",
    "keepalives_idle", "keepalives_interval", "keepalives_count",
    "tcp_user_timeout", "replication", "gssencmode", "sslmode", "requiressl",
    "sslnegotiation", "sslcompression", "sslcert", "sslkey", "sslpassword",
    "sslcertmode", "sslrootcert", "sslcrl", "sslcrldir", "sslsni",
    "requirepeer", "ssl_min_protocol_version", "ssl_max_protocol_version",
    "krbsrvname", "gsslib", "gssdelegation", "service", "target_session_attrs",
    "load_balance_hosts",
}

_TRUE = {"1", "true", "yes", "on"}


def _split_pg_query(url: str) -> Tuple[str, Dict[str, str]]:
    """
    Separa da query os parâmetros que não são do libpq.

    Devolve a URL só com parâmetros libpq e um dict (chaves em minúsculas) com
    os restantes, para quem chama traduzir os que têm equivalente (`schema`,
    `pgbouncer`). Os descartados ficam registados no log.
    """
    mantidos, extras = [], {}
    for chave, valor in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if chave.lower() in _LIBPQ_PARAMS:
            mantidos.append((chave, valor))
        else:
            extras[chave.lower()] = valor

    if extras:
        log_message(
            f"🔧 URL PostgreSQL: parâmetros que não são do libpq tratados/ignorados: {sorted(extras)}",
            level="info",
        )
    return _set_query(url, mantidos), extras


def _pg_explicit_port(url: str) -> str:
    """
    Põe a porta na URL quando ela não a traz (5432).

    Sem porta, o libpq e o asyncpg usam a variável de ambiente PGPORT. O .env
    define PGPORT=5431 (de outra base), e o processo carrega-o: uma URL do Neon
    sem porta ia bater a :5431 e morria em timeout. O mesmo em produção com
    qualquer PG* no ambiente — a URL colada tem de valer por si.
    """
    partes = urlsplit(url)
    autoridade = partes.netloc.rsplit("@", 1)[-1]
    # Sem host (socket local) ou com vários hosts, não se mexe.
    if not autoridade or "," in autoridade or _porta_tolerante(partes) is not None:
        return url
    userinfo = partes.netloc[: len(partes.netloc) - len(autoridade)]
    return _set_netloc(url, f"{userinfo}{autoridade}:{DEFAULT_PORTS['PostgreSQL']}")


class InvalidDatabaseUrl(ValueError):
    """URL de conexão que não dá para usar — a mensagem é para o utilizador."""


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------


def _scheme(url: str) -> str:
    return (urlsplit(url).scheme or "").lower()


def _scheme_base(url: str) -> str:
    """`postgresql+psycopg2` → `postgresql`; `mongodb+srv` fica intacto."""
    scheme = _scheme(url)
    if scheme == "mongodb+srv":
        return scheme
    return scheme.split("+")[0]


def is_database_url(value: Optional[str]) -> bool:
    """True se o texto se parece com uma URL de conexão suportada."""
    if not value or "://" not in value and not value.lower().startswith("sqlite"):
        return False
    base = _scheme_base(value)
    return base in SCHEME_TO_TYPE or base == "mongodb+srv"


def is_mongo_url(url: Optional[str]) -> bool:
    return _scheme_base(url or "") in {"mongodb", "mongodb+srv"}


def db_type_from_url(url: str) -> str:
    """Devolve a chave de tipo ("PostgreSQL", "MongoDB", …)."""
    base = _scheme_base(url)
    if base == "mongodb+srv":
        return "MongoDB"

    tipo = SCHEME_TO_TYPE.get(base)
    if not tipo:
        raise InvalidDatabaseUrl(
            f"Base de dados '{base or '?'}' não suportada na URL. "
            "Esquemas aceites: postgresql, mysql, mariadb, sqlserver/mssql, "
            "oracle, mongodb, sqlite."
        )
    return tipo


def _porta_tolerante(partes) -> Optional[int]:
    """`partes.port` sem rebentar: uma URI de replica set não tem porta única."""
    try:
        return partes.port
    except ValueError:
        return None


def _primeiro_host(partes) -> Tuple[str, Optional[int]]:
    """
    Host e porta a guardar nas colunas.

    Numa URI de replica set (`mongodb://h1:27017,h2:27017/db`) não há um host
    só; guarda-se o primeiro, apenas para identificar a conexão na listagem. A
    ligação continua a usar a URL inteira, com todos os nós.
    """
    porta = _porta_tolerante(partes)
    if porta is not None or not partes.netloc:
        return (partes.hostname or "", porta)

    autoridade = partes.netloc.rsplit("@", 1)[-1]
    primeiro = autoridade.split(",")[0]

    if primeiro.startswith("["):  # IPv6
        host, _, resto = primeiro.partition("]")
        host = f"{host}]"
        porta_txt = resto.lstrip(":")
    else:
        host, _, porta_txt = primeiro.partition(":")

    return (host.lower(), int(porta_txt) if porta_txt.isdigit() else None)


def parse_db_url(url: str) -> Dict[str, Any]:
    """
    Extrai os campos que a nossa tabela precisa de guardar.

    Não é usado para ligar — só para preencher colunas e para a listagem
    mostrar host/base. Ver o docstring do módulo.
    """
    if not url or not url.strip():
        raise InvalidDatabaseUrl("URL de conexão vazia.")

    url = url.strip()
    tipo = db_type_from_url(url)

    if tipo == "SQLite":
        # `sqlite:///rel.db` (3 barras) é relativo; `sqlite:////abs.db` absoluto.
        resto = url.split(":", 1)[1]
        barras = len(resto) - len(resto.lstrip("/"))
        caminho = resto.lstrip("/")
        return {
            "type": tipo,
            "host": f"/{caminho}" if barras >= 4 else caminho,
            "port": 0,
            "database": f"/{caminho}" if barras >= 4 else caminho,
            "username": "",
            "password": "",
            "service": "",
            "sslmode": "",
            "trustServerCertificate": "yes",
        }

    partes = urlsplit(url)
    params = {chave.lower(): valor for chave, valor in parse_qsl(partes.query)}

    hostname, porta = _primeiro_host(partes)
    if not hostname:
        raise InvalidDatabaseUrl("A URL de conexão não indica o host.")

    database = unquote(partes.path or "").lstrip("/")

    # No Oracle o que vem depois do host é o service name, não uma base.
    service = ""
    if tipo == "Oracle":
        service = params.get("service_name") or params.get("service") or database
        database = ""
    elif tipo == "MongoDB":
        # O resto do código usa `service` como authSource (ver
        # DatabaseManager.get_engine) — a base onde a credencial foi criada.
        service = params.get("authsource") or params.get("auth_source") or ""

    if tipo == "SQL Server" and not database:
        database = params.get("databasename") or params.get("database") or ""

    trust = params.get("trustservercertificate") or params.get("encrypt") or ""

    return {
        "type": tipo,
        "host": hostname,
        "port": porta or DEFAULT_PORTS.get(tipo, 0),
        "database": database,
        "username": unquote(partes.username or "") or params.get("user", ""),
        "password": unquote(partes.password or "") or params.get("password", ""),
        "service": service,
        "sslmode": params.get("sslmode", "") if tipo == "PostgreSQL" else "",
        "trustServerCertificate": (
            "yes" if trust.strip().lower() in {"true", "1", "yes", "y", "on"} else "no"
        )
        if trust
        else "yes",
    }


# ---------------------------------------------------------------------------
# Escrita / normalização
# ---------------------------------------------------------------------------


# As três funções seguintes fazem cirurgia de texto em vez de usar
# `urlunsplit`. Razão: `urlunsplit` colapsa as barras quando o netloc é vazio
# ("sqlite:///app.db" saía "sqlite:/app.db", que o SQLAlchemy já não abre).
# Trocar só o pedaço que interessa preserva o resto da URL byte a byte.


def _set_scheme(url: str, novo: str) -> str:
    idx = url.find(":")
    return f"{novo}{url[idx:]}" if idx > 0 else url


def _set_query(url: str, params) -> str:
    sem_frag, _, frag = url.partition("#")
    base = sem_frag.split("?", 1)[0]
    query = urlencode(params)

    return base + (f"?{query}" if query else "") + (f"#{frag}" if frag else "")


def _set_netloc(url: str, netloc: str) -> str:
    idx = url.find("://")
    if idx < 0:
        return url

    resto = url[idx + 3 :]
    corte = len(resto)
    for sep in ("/", "?", "#"):
        pos = resto.find(sep)
        if pos != -1:
            corte = min(corte, pos)

    return f"{url[: idx + 3]}{netloc}{resto[corte:]}"


def _netloc(
    username: str, password: str, host: str, port: Optional[int]
) -> str:
    userinfo = ""
    if username:
        # Re-encode: a password pode ter @ : / ? # % — sem isto a URI parte-se.
        userinfo = quote(username, safe="")
        if password:
            userinfo += f":{quote(password, safe='')}"
        userinfo += "@"

    alvo = host if host.startswith("[") or ":" not in host else f"[{host}]"
    return f"{userinfo}{alvo}" + (f":{port}" if port else "")


def remap_url_host(url: str, remapper: Callable[[str], str]) -> str:
    """
    Aplica o remapeamento de host (localhost → host.docker.internal) à URL.

    Sem isto, uma URL com `localhost` colada num backend em contentor liga ao
    próprio contentor e dá "connection refused" — exactamente o problema que
    `_maybe_remap_host` já resolve para os campos separados.
    """
    if is_mongo_url(url) and _scheme_base(url) == "mongodb+srv":
        return url  # SRV resolve por DNS; não há host para remapear

    partes = urlsplit(url)
    host = partes.hostname or ""
    if not host:
        return url

    novo = remapper(host)
    if novo == host:
        return url

    return _set_netloc(
        url,
        _netloc(
            unquote(partes.username or ""),
            unquote(partes.password or ""),
            novo,
            _porta_tolerante(partes),
        ),
    )


def _with_driver(url: str, drivers: Dict[str, str]) -> str:
    """Troca o esquema pelo driver instalado, mantendo o resto da URL."""
    tipo = db_type_from_url(url)
    scheme_atual = _scheme(url)

    # O utilizador já indicou um driver (`postgresql+asyncpg://`) → respeitar.
    if "+" in scheme_atual and scheme_atual != "mongodb+srv":
        return url

    novo = drivers.get(tipo)
    if not novo:
        raise InvalidDatabaseUrl(f"Sem driver disponível para {tipo}.")

    return _set_scheme(url, novo)


def _ensure_odbc_driver(url: str) -> str:
    """
    Acrescenta `driver=…` às URLs de SQL Server.

    Nem o pyodbc nem o aioodbc adivinham o driver ODBC, e ninguém o cola numa
    connection string. Sem isto o erro é "Data source name not found".
    """
    params = parse_qsl(urlsplit(url).query)

    if any(chave.lower() == "driver" for chave, _ in params):
        return url

    params.append(("driver", _ODBC_DRIVER))
    return _set_query(url, params)


def sync_url(url: str) -> str:
    """URL pronta para `create_engine` (drivers síncronos)."""
    url = _with_driver(url.strip(), SYNC_DRIVERS)

    if db_type_from_url(url) == "SQL Server":
        url = _ensure_odbc_driver(url)

    if db_type_from_url(url) == "PostgreSQL":
        url, extras = _split_pg_query(_pg_explicit_port(url))
        params = parse_qsl(urlsplit(url).query, keep_blank_values=True)
        chaves = {k.lower() for k, _ in params}
        # `schema=` (Prisma) → search_path, se a URL não trouxer já `options`.
        schema = extras.get("schema")
        if schema and "options" not in chaves:
            params.append(("options", f"-csearch_path={schema}"))
        # sslmode explícito, pela regra da app, para o PGSSLMODE do ambiente
        # (o .env tem `require`) não decidir por ela.
        if "sslmode" not in chaves:
            params.append(("sslmode", resolve_pg_sslmode(urlsplit(url).hostname, "")))
        url = _set_query(url, params)

    return url


def async_url(url: str) -> Tuple[str, Dict[str, Any]]:
    """
    URL + `connect_args` para `create_async_engine`.

    Devolve os dois juntos porque o asyncpg recusa `sslmode`/`channel_binding`
    na query (TypeError em connect()); a intenção passa para `connect_args`.
    """
    url = _with_driver(url.strip(), ASYNC_DRIVERS)
    connect_args: Dict[str, Any] = {}

    if db_type_from_url(url) == "SQL Server":
        url = _ensure_odbc_driver(url)

    if _scheme(url).endswith("asyncpg"):
        url = _pg_explicit_port(url)
        partes = urlsplit(url)
        url, extras = _split_pg_query(url)

        # O SQLAlchemy passa a query ao asyncpg.connect() como argumentos, e o
        # asyncpg não conhece as palavras-chave do libpq (TypeError). Nada da
        # query segue na URL: traduz-se o que tem equivalente e o resto
        # (sslmode, channel_binding, ...) é só lido.
        modo = ""
        server_settings: Dict[str, str] = {}
        for chave, valor in parse_qsl(urlsplit(url).query, keep_blank_values=True):
            k = chave.lower()
            if k in {"sslmode", "ssl_mode"}:
                modo = valor.lower()
            elif k == "application_name":
                server_settings["application_name"] = valor
            elif k == "connect_timeout" and valor.strip().isdigit():
                connect_args["timeout"] = float(valor)
            elif k in {"host", "user", "password"}:
                connect_args[k] = valor
            elif k == "port" and valor.strip().isdigit():
                connect_args["port"] = int(valor)
            elif k == "dbname":
                connect_args["database"] = valor
        url = _set_query(url, [])

        if extras.get("schema"):
            server_settings["search_path"] = extras["schema"]
        if server_settings:
            connect_args["server_settings"] = server_settings

        # `pgbouncer=true` (Prisma): pooler em modo transação, onde prepared
        # statements com nome falham ("prepared statement ... already
        # exists"). Desliga as caches do asyncpg e do SQLAlchemy.
        if (extras.get("pgbouncer") or "").lower() in _TRUE:
            connect_args["statement_cache_size"] = 0
            url = _set_query(url, [("prepared_statement_cache_size", "0")])

        # Mesma regra de SSL das conexões por campos (app/ultils/db_ssl.py):
        # local → sem SSL; require/verify-* respeitados; remoto → prefer.
        connect_args["ssl"] = asyncpg_ssl_arg(partes.hostname, modo)

    return url, connect_args


# ---------------------------------------------------------------------------
# Ligação
# ---------------------------------------------------------------------------


def engine_from_url(url: str, **extra: Any):
    """
    Cria o Engine (SQLAlchemy) ou MongoClient a partir da URL, tal como está.

    O `extra` é passado ao `create_engine` (pool, echo…). Para MongoDB é
    ignorado: os timeouts são os mesmos do resto do código.
    """
    if not url or not url.strip():
        raise InvalidDatabaseUrl("URL de conexão vazia.")

    url = url.strip()

    if is_mongo_url(url):
        log_message("🔌 Criando MongoClient a partir de URL", level="debug")
        return MongoClient(
            url,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )

    final = sync_url(url)
    log_message(
        f"🔌 Criando Engine a partir de URL ({_scheme(final)})", level="debug"
    )

    opcoes: Dict[str, Any] = {"pool_pre_ping": True, "pool_recycle": 1800}
    if _scheme_base(final) not in {"sqlite", "sqlite3"}:
        opcoes.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
    opcoes.update(extra)

    return create_engine(final, **opcoes)
