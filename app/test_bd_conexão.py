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
PGSSLMODE = "require"
# PGSSLMODE = "disable"

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

    except Exception as e:
        print("❌ Porta inacessível")
        print(e)
        return False

    finally:
        sock.close()


# -------------------------
# TESTE POSTGRES
# -------------------------


def test_postgres():

    print("\nTestando conexão PostgreSQL...")
    # pg.mustainfo.com
    try:
        conn = psycopg2.connect(
            host="pg.mustainfo.com",
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
        print("\nProblema é rede/firewall/security group.")
