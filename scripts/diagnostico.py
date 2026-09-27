"""
Diagnóstico de arranque: diz porque é que a app (ou o alembic) não liga.

Corre isto ANTES de ler tracebacks. Verifica, pela ordem em que as coisas
falham no arranque real:

  1. qual o .env efectivamente carregado
  2. variáveis obrigatórias em falta
  3. DATABASE_URL vs DATABASE_URL_ALEMBIC — host/utilizador/base de cada uma
  4. ligação real ao Postgres (e quem o servidor diz que somos)
  5. estado das migrações: revisão na base vs head no disco
  6. tabelas do núcleo em falta
  7. Redis
  8. storage S3/MinIO

Não importa a app (`app.main`): é de propósito. O objectivo é funcionar
mesmo quando a app não arranca, e sem esperar pelos imports pesados do OCR.

Uso:
    python scripts/diagnostico.py

Dentro do contentor:
    docker exec -it <contentor> python scripts/diagnostico.py

Nenhuma password é impressa — só o comprimento e os primeiros caracteres,
para se poder colar a saída num chat sem vazar segredos.
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

# `python scripts/diagnostico.py` põe scripts/ no sys.path, não a raiz. No
# contentor o PYTHONPATH=/app salva a situação; fora dele, não.
_RAIZ = Path(__file__).resolve().parent.parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

# Carrega o .env pelas mesmas regras da app e imprime qual ficheiro venceu.
from app.config.dotenv import get_env  # noqa: E402  (depois do sys.path acima)

OK = "[ ok ]"
ERRO = "[FALHA]"
AVISO = "[aviso]"

_falhas: list[str] = []


def titulo(texto: str) -> None:
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")


def falha(msg: str) -> None:
    _falhas.append(msg)
    print(f"{ERRO} {msg}")


def mascara(valor: str | None) -> str:
    """
    Segredo reconhecível sem ser legível.

    Num valor curto não se mostra caracter nenhum: em 4 chars, revelar 2 é
    revelar metade da password.
    """
    if not valor:
        return "(vazio)"
    if len(valor) < 12:
        return f"(definida, {len(valor)} chars)"
    return f"{valor[:3]}…{valor[-2:]} ({len(valor)} chars)"


# ----------------------------------------------------------------------
# 1. Ficheiro .env
# ----------------------------------------------------------------------
def secao_env_carregado() -> None:
    titulo("1. Ficheiro .env")
    # O app.config.dotenv já imprimiu "[ENV] Carregado: <caminho>" ao ser
    # importado. Aqui mostra-se o contexto que decide qual ficheiro ele vê.
    print(f"  cwd                : {os.getcwd()}")
    print(f"  ENV_PATH           : {os.environ.get('ENV_PATH') or '(não definido)'}")
    for caminho in (".env", ".env.production"):
        existe = "existe" if os.path.exists(caminho) else "não existe"
        print(f"  {caminho:19}: {existe}")
    print(
        "\n  Nota: o load_dotenv usa override=False — uma variável definida no\n"
        "  ambiente (plataforma de deploy, docker -e) GANHA sobre o .env."
    )


# ----------------------------------------------------------------------
# 2. Variáveis obrigatórias
# ----------------------------------------------------------------------
def secao_obrigatorias() -> None:
    titulo("2. Variáveis sem as quais a app não arranca")
    obrigatorias = {
        "DATABASE_URL": "ligação da app (sem ela cai para sqlite:///./test.db)",
        "DATABASE_URL_ALEMBIC": "ligação das migrações (usada pelo entrypoint)",
        "SECRET_KEY": "assinatura dos JWT",
        "ENCRYPTION_KEY": "credenciais das ligações guardadas na base",
    }
    opcionais = {
        "GEMINI_API_KEY": "GeminiService levanta erro se for usado sem isto",
        "app_cache_REDIS_URL": "sem isto o rate limit não é partilhado entre workers",
        "STORAGE_BUCKET": "uploads",
    }

    for nome, porque in obrigatorias.items():
        valor = get_env(nome)
        if valor:
            print(f"{OK} {nome:22} definida")
        else:
            falha(f"{nome} não definida — {porque}")

    for nome, porque in opcionais.items():
        valor = get_env(nome)
        estado = "definida" if valor else "(não definida)"
        marca = OK if valor else AVISO
        print(f"{marca} {nome:22} {estado}" + ("" if valor else f" — {porque}"))


# ----------------------------------------------------------------------
# 3. As duas URLs, lado a lado
# ----------------------------------------------------------------------
def _partes(url: str) -> dict[str, str]:
    p = urlparse(url)
    return {
        "driver": p.scheme,
        "utilizador": unquote(p.username or "") or "(nenhum)",
        "password": mascara(unquote(p.password) if p.password else None),
        "host": p.hostname or "(nenhum)",
        "porta": str(p.port or "(omissão)"),
        "base": (p.path or "").lstrip("/") or "(nenhuma)",
        "query": p.query or "(nenhuma)",
    }


def secao_urls() -> dict[str, dict[str, str]]:
    titulo("3. DATABASE_URL vs DATABASE_URL_ALEMBIC")
    print(
        "  São duas variáveis independentes. Se discordarem, o alembic migra uma\n"
        "  base e a app lê outra — e o sintoma é 'tabela não existe' com as\n"
        "  migrações 'aplicadas com sucesso'.\n"
    )

    resultado: dict[str, dict[str, str]] = {}
    for nome in ("DATABASE_URL", "DATABASE_URL_ALEMBIC"):
        url = get_env(nome)
        print(f"  {nome}")
        if not url:
            print("    (não definida)\n")
            continue
        partes = _partes(url)
        resultado[nome] = partes
        for chave, valor in partes.items():
            print(f"    {chave:12}: {valor}")
        print()

    a, b = resultado.get("DATABASE_URL"), resultado.get("DATABASE_URL_ALEMBIC")
    if a and b:
        for campo in ("host", "porta", "base", "utilizador"):
            if a[campo] != b[campo]:
                falha(
                    f"As duas URLs divergem em '{campo}': "
                    f"app={a[campo]} vs alembic={b[campo]}"
                )
        if a["driver"] == b["driver"]:
            print(
                f"{AVISO} Ambas usam o driver '{a['driver']}'. A do alembic devia ser "
                "postgresql+psycopg2://"
            )
    return resultado


# ----------------------------------------------------------------------
# 4. Ligação real
# ----------------------------------------------------------------------
def secao_ligacao() -> bool:
    titulo("4. Ligação ao Postgres")
    url = get_env("DATABASE_URL_ALEMBIC") or get_env("DATABASE_URL")
    if not url:
        falha("Sem URL para testar.")
        return False

    from sqlalchemy import create_engine, text

    # URL do alembic para o psycopg2 síncrono; a da app pode vir com +asyncpg.
    url_sync = url.replace("+asyncpg", "+psycopg2")

    try:
        engine = create_engine(url_sync, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            quem = conn.execute(
                text("SELECT current_user, current_database(), version()")
            ).one()
        print(f"{OK} Ligou.")
        print(f"  current_user     : {quem[0]}")
        print(f"  current_database : {quem[1]}")
        print(f"  versão           : {quem[2].split(',')[0]}")
        return True

    except Exception as exc:
        texto = str(exc)
        falha(f"Não ligou: {type(exc).__name__}")
        # A linha FATAL do Postgres é a única que interessa do traceback todo.
        for linha in texto.splitlines():
            if "FATAL" in linha or "could not" in linha or "timeout" in linha:
                print(f"       → {linha.strip()}")
        if "password authentication failed" in texto:
            print(
                "\n  O servidor respondeu, logo o host e a porta estão certos: o que\n"
                "  está errado é o par utilizador/password da URL. Compara o\n"
                "  'utilizador' da secção 3 com o que o serviço de Postgres anuncia.\n"
                "  Atenção a passwords com @ : / # — têm de ir percent-encoded."
            )
        elif 'does not exist' in texto and 'database' in texto:
            print("\n  Utilizador aceite, base de dados inexistente. Cria-a ou corrige o nome.")
        return False


# ----------------------------------------------------------------------
# 5. Migrações
# ----------------------------------------------------------------------
def secao_migracoes() -> None:
    titulo("5. Estado das migrações")
    url = get_env("DATABASE_URL_ALEMBIC")
    if not url:
        falha("DATABASE_URL_ALEMBIC não definida — o entrypoint não consegue migrar.")
        return

    from sqlalchemy import create_engine, text
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    try:
        cfg = Config("alembic.ini")
        script = ScriptDirectory.from_config(cfg)
        heads = script.get_heads()
        print(f"  head(s) no disco : {', '.join(heads) or '(nenhum)'}")
        if len(heads) > 1:
            falha(f"{len(heads)} heads — o alembic não sabe para onde subir. Faz merge.")
    except Exception as exc:
        falha(f"Não consegui ler o alembic/: {exc}")
        return

    try:
        engine = create_engine(
            url.replace("+asyncpg", "+psycopg2"), connect_args={"connect_timeout": 10}
        )
        with engine.connect() as conn:
            existe = conn.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'alembic_version')"
                )
            ).scalar()
            if not existe:
                print(
                    f"{AVISO} Tabela alembic_version não existe: nenhuma migração foi "
                    "aplicada a esta base."
                )
                print("       → o `alembic upgrade head` do entrypoint resolve.")
                return
            atual = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
            print(f"  revisão na base  : {', '.join(atual) or '(vazia)'}")
            if set(atual) == set(heads):
                print(f"{OK} Base ao nível do head.")
            else:
                print(f"{AVISO} Base atrasada — falta correr `alembic upgrade head`.")
    except Exception as exc:
        falha(f"Não consegui ler o estado das migrações: {type(exc).__name__}")


# ----------------------------------------------------------------------
# 6. Tabelas do núcleo
# ----------------------------------------------------------------------
def secao_tabelas() -> None:
    titulo("6. Tabelas do núcleo (as que o seed precisa)")
    url = get_env("DATABASE_URL_ALEMBIC") or get_env("DATABASE_URL")
    if not url:
        return

    from sqlalchemy import create_engine, inspect

    nucleo = ("empresas", "users", "roles", "permissions", "plans")
    try:
        engine = create_engine(
            url.replace("+asyncpg", "+psycopg2"), connect_args={"connect_timeout": 10}
        )
        existentes = set(inspect(engine).get_table_names())
        print(f"  tabelas na base  : {len(existentes)}")
        em_falta = [t for t in nucleo if t not in existentes]
        if em_falta:
            falha(f"Em falta: {', '.join(em_falta)} — o seed vai rebentar.")
        else:
            print(f"{OK} Todas presentes.")
    except Exception as exc:
        falha(f"Não consegui inspecionar o esquema: {type(exc).__name__}")


# ----------------------------------------------------------------------
# 7. Redis
# ----------------------------------------------------------------------
def secao_redis() -> None:
    titulo("7. Redis")
    url = get_env("app_cache_REDIS_URL")
    if url:
        p = urlparse(url)
        print(f"  origem           : app_cache_REDIS_URL")
        print(f"  host:porta       : {p.hostname}:{p.port or 6379}")
        print(f"  password         : {mascara(p.password)}")
    else:
        print(f"  origem           : REDIS_HOST/PORT/PASSWORD (app_cache_REDIS_URL vazia)")
        print(f"  host:porta       : {get_env('REDIS_HOST', 'localhost')}:{get_env('REDIS_PORT', '6379')}")
        print(f"  password         : {mascara(get_env('REDIS_PASSWORD'))}")

    try:
        import redis as redis_lib

        from app.config.redis import _build_url

        cliente = redis_lib.Redis.from_url(
            _build_url(), socket_timeout=5, socket_connect_timeout=5
        )
        cliente.ping()
        print(f"{OK} PING respondido.")
    except Exception as exc:
        # Redis em baixo não impede o arranque — degrada o cache e o rate limit.
        print(f"{AVISO} Sem Redis: {type(exc).__name__}: {exc}")
        print("       → a app arranca, mas o cache e o rate limit de login ficam locais.")


# ----------------------------------------------------------------------
# 8. Storage
# ----------------------------------------------------------------------
def secao_storage() -> None:
    titulo("8. Storage S3 / MinIO")
    print(f"  STORAGE_BUCKET          : {get_env('STORAGE_BUCKET') or '(não definido)'}")
    print(f"  STORAGE_ENDPOINT        : {get_env('STORAGE_ENDPOINT') or '(não definido)'}")
    print(f"  STORAGE_PUBLIC_ENDPOINT : {get_env('STORAGE_PUBLIC_ENDPOINT') or '(usa o interno)'}")
    print(f"  STORAGE_ACCESS_KEY      : {mascara(get_env('STORAGE_ACCESS_KEY'))}")
    print(f"  STORAGE_SECRET_KEY      : {mascara(get_env('STORAGE_SECRET_KEY'))}")
    print(
        "\n  O STORAGE_PUBLIC_ENDPOINT é o host que assina as URLs que o BROWSER\n"
        "  abre. Se for interno (minio:9000, host.docker.internal), os downloads\n"
        "  falham no cliente mesmo com o upload a funcionar."
    )


# ----------------------------------------------------------------------
def main() -> int:
    print("Diagnóstico de arranque — gmTGBackend")
    print(f"ENV={get_env('ENV', '(não definido)')}  python={sys.version.split()[0]}")

    secao_env_carregado()
    secao_obrigatorias()
    secao_urls()
    ligou = secao_ligacao()
    if ligou:
        secao_migracoes()
        secao_tabelas()
    else:
        print(
            f"\n{AVISO} Secções 5 e 6 saltadas: sem ligação não há nada para inspecionar."
        )
    secao_redis()
    secao_storage()

    titulo("Resumo")
    if _falhas:
        print(f"{len(_falhas)} problema(s) que impedem ou degradam o arranque:\n")
        for i, msg in enumerate(_falhas, 1):
            print(f"  {i}. {msg}")
        return 1

    print(f"{OK} Nenhum problema bloqueante encontrado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
