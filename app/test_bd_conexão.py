import socket
import traceback
import psycopg2

from config.dotenv import get_env, get_env_int


# -------------------------
# ENV
# -------------------------

PGUSER = get_env("PGUSER")
PGPASSWORD = get_env("PGPASSWORD")
PGHOST = get_env("PGHOST")
PGPORT = get_env_int("PGPORT")
PGDATABASE = get_env("PGDATABASE")
PGSSLMODE = "disable"  # ou "disable"


# -------------------------
# TESTE DE PORTA
# -------------------------


def test_socket(host: str, port: int) -> bool:
    print(f"\nTestando acesso TCP {host}:{port}...")

    sock = socket.socket()
    sock.settimeout(5)

    try:
        sock.connect((host, port))
        print("✅ Porta acessível")
        return True

    except socket.gaierror as e:
        # getaddrinfo falhou: o NOME não resolve. Não é firewall nem porta
        # fechada — não se chegou a tentar ligar a nada. Acontece sempre que
        # se corre isto fora do cluster com um host interno do Docker/Swarm
        # (ex.: mustainfocloud-pgmustainfodb-ifp14h), que só existe na rede
        # overlay da plataforma.
        print(f"❌ Nome '{host}' não resolve (DNS): {e}")
        print("   Não é firewall. Ou o nome está errado, ou é um host interno")
        print("   do cluster e este teste tem de correr DENTRO do contentor:")
        print("       docker exec -it <contentor> python scripts/diagnostico.py")
        return False

    except (socket.timeout, TimeoutError) as e:
        print(f"❌ Timeout a ligar a {host}:{port}: {e}")
        print("   O nome resolveu mas ninguém respondeu — aí sim, é")
        print("   firewall/security group ou o serviço está em baixo.")
        return False

    except ConnectionRefusedError as e:
        print(f"❌ Ligação recusada por {host}:{port}: {e}")
        print("   O host existe e respondeu: nada está à escuta nessa porta.")
        return False

    except Exception as e:
        print("❌ Porta inacessível")
        print(f"   {type(e).__name__}: {e}")
        return False

    finally:
        sock.close()


# -------------------------
# TESTE POSTGRES
# -------------------------


def test_postgres():

    print("\nTestando conexão PostgreSQL...")

    try:
        conn = psycopg2.connect(
            host=PGHOST,
            port=PGPORT,
            dbname=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD,
            sslmode=PGSSLMODE,
            connect_timeout=10,
        )

        print("✅ Conexão com banco OK")

        cur = conn.cursor()

        cur.execute("SELECT current_database(), current_user, version();")

        db, user, version = cur.fetchone()

        print(f"Database: {db}")
        print(f"User: {user}")
        print(f"Version: {version}")

        cur.close()
        conn.close()

        return True

    except Exception as e:
        print("❌ Falha ao conectar no PostgreSQL")
        print(e)
        print(traceback.format_exc())

        return False


# -------------------------
# MAIN
# -------------------------

if __name__ == "__main__":

    print("=" * 50)
    print("DIAGNÓSTICO POSTGRES")
    print("=" * 50)

    if test_socket(PGHOST, PGPORT):
        test_postgres()
    else:
        print("\nNão se chegou a testar credenciais — ver a causa acima.")
