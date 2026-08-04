"""
Migra as credenciais de conexão do envelope antigo para AES-256-GCM
com chave-mestra (ENCRYPTION_KEY).

O formato antigo guardava a chave dentro do próprio texto cifrado, o que
significa que qualquer dump da tabela `db_connections` expunha host,
utilizador e password de todas as bases de dados dos clientes.

A leitura já aceita ambos os formatos (ver `secret_decrypt`), portanto a
aplicação funciona antes e depois desta migração. Este script existe para
fechar a janela: depois de correr, deixa de haver credenciais decifráveis
sem a ENCRYPTION_KEY.

Uso:
    # 1. ver o que seria alterado, sem gravar
    python -m scripts.migrate_connection_secrets --dry-run

    # 2. aplicar
    python -m scripts.migrate_connection_secrets

Antes de aplicar: faz backup da base de dados e garante que ENCRYPTION_KEY
está definida (e guardada em local seguro). Perder essa chave torna as
credenciais irrecuperáveis.
"""

from __future__ import annotations

import argparse
import sys

from app.database import SessionLocal
from app.models.connection_models import DBConnection
from app.services.crypto_utils import (
    get_master_key,
    is_encrypted_at_rest,
    reencrypt_at_rest,
)

SECRET_FIELDS = ("host", "username", "password")


def migrate(dry_run: bool = False) -> int:
    """Recifra todas as conexões ainda em formato legado. Devolve exit code."""

    try:
        get_master_key()
    except RuntimeError as exc:
        print(f"❌ {exc}")
        return 1

    session = SessionLocal()
    migradas = 0
    ja_ok = 0
    falhas = 0

    try:
        conexoes = session.query(DBConnection).all()
        print(f"🔍 {len(conexoes)} conexões encontradas.\n")

        for conn in conexoes:
            pendentes = [
                f for f in SECRET_FIELDS
                if getattr(conn, f) and not is_encrypted_at_rest(getattr(conn, f))
            ]

            if not pendentes:
                ja_ok += 1
                continue

            try:
                for field in pendentes:
                    setattr(conn, field, reencrypt_at_rest(str(getattr(conn, field))))

                conn.is_encrypted = True
                migradas += 1
                print(f"  ✅ id={conn.id} '{conn.name}' → {', '.join(pendentes)}")

            except Exception as exc:
                # Uma linha corrompida não deve travar a migração das outras.
                falhas += 1
                print(f"  ⚠️  id={conn.id} '{conn.name}' falhou: {exc}")
                session.expunge(conn)

        if dry_run:
            session.rollback()
            print("\n🧪 DRY-RUN — nada foi gravado.")
        else:
            session.commit()
            print("\n💾 Alterações gravadas.")

        print(
            f"   migradas={migradas}  já em v2={ja_ok}  falhas={falhas}"
        )

        return 1 if falhas else 0

    except Exception as exc:
        session.rollback()
        print(f"❌ Migração abortada: {exc}")
        return 1

    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="mostra o que seria alterado sem gravar",
    )
    args = parser.parse_args()

    sys.exit(migrate(dry_run=args.dry_run))
