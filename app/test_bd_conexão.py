import socket
import traceback
import oracledb

from config.dotenv import get_env, get_env_int

# -------------------------
# ENV
# -------------------------
ORACLE_HOST = get_env("ORACLE_HOST")
ORACLE_PORT = get_env_int("ORACLE_PORT")
ORACLE_USER = get_env("ORACLE_USER")
ORACLE_PASSWORD = get_env("ORACLE_PASSWORD")
ORACLE_SERVICE = get_env("ORACLE_SERVICE")


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
# TESTE ORACLE
# -------------------------
def test_oracle():

    print("\nTestando conexão Oracle...")

    try:
        dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"

        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)

        print("✅ Conexão com Oracle OK")

        cursor = conn.cursor()

        cursor.execute("SELECT sysdate FROM dual")

        result = cursor.fetchone()

        print(f"Data do servidor: {result[0]}")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print("❌ Falha ao conectar no Oracle")
        print(e)
        print(traceback.format_exc())

        return False


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":

    print("=" * 50)
    print("DIAGNÓSTICO ORACLE")
    print("=" * 50)

    if test_socket(ORACLE_HOST, ORACLE_PORT):
        test_oracle()
    else:
        print("\nProblema é rede/firewall/security group.")
# # -------------------------
# # ENV
# # -------------------------
# PGUSER = get_env("PGUSER")
# PGPASSWORD = get_env("PGPASSWORD")
# PGHOST = get_env("PGHOST")
# PGPORT = get_env_int("PGPORT")
# PGDATABASE = get_env("PGDATABASE")
# PGSSLMODE = "require"
# # PGSSLMODE = "disable"

# # -------------------------
# # TESTE DE PORTA
# # -------------------------


# def test_socket(host: str, port: int) -> bool:
#     print(f"\nTestando acesso TCP {host}:{port}...")

#     sock = socket.socket()
#     sock.settimeout(5)

#     try:
#         sock.connect((host, port))
#         print("✅ Porta acessível")
#         return True

#     except Exception as e:
#         print("❌ Porta inacessível")
#         print(e)
#         return False

#     finally:
#         sock.close()


# # -------------------------
# # TESTE POSTGRES
# # -------------------------


# def test_postgres():

#     print("\nTestando conexão PostgreSQL...")
#     # pg.mustainfo.com
#     try:
#         conn = psycopg2.connect(
#             host="pg.mustainfo.com",
#             port=PGPORT,
#             dbname=PGDATABASE,
#             user=PGUSER,
#             password=PGPASSWORD,
#             sslmode=PGSSLMODE,
#             connect_timeout=10,
#         )

#         print("✅ Conexão com banco OK")

#         cur = conn.cursor()

#         cur.execute("SELECT current_database(), current_user, version();")

#         db, user, version = cur.fetchone()

#         print(f"Database: {db}")
#         print(f"User: {user}")
#         print(f"Version: {version}")

#         cur.close()
#         conn.close()

#         return True

#     except Exception as e:
#         print("❌ Falha ao conectar no PostgreSQL")
#         print(e)
#         print(traceback.format_exc())

#         return False


# # -------------------------
# # MAIN
# # -------------------------

# if __name__ == "__main__":

#     print("=" * 50)
#     print("DIAGNÓSTICO POSTGRES")
#     print("=" * 50)

#     if test_socket(PGHOST, PGPORT):
#         test_postgres()
#     else:
#         print("\nProblema é rede/firewall/security group.")
