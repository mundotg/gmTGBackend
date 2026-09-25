"""add storage_folders (árvore com senha) e files.folder_id

Pastas do storage em árvore, com senha opcional por pasta (hash bcrypt — é
controlo de acesso, não cifragem). A pasta é apenas metadado: a chave do objeto
no bucket continua a ser `{user_id}/{uuid}.ext`.

`files.folder_id` é nullable: NULL = ficheiro na raiz, que é o estado de todos
os ficheiros já existentes.

Revision ID: a6b9d4e28c71
Revises: f5a8c3d71b64
Create Date: 2026-08-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a6b9d4e28c71"
down_revision: Union[str, None] = "f5a8c3d71b64"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "storage_folders",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("parent_id", sa.Uuid(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=True),
        sa.Column(
            "is_deleted", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["storage_folders.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "parent_id", "name", name="uq_folder_user_parent_name"
        ),
    )
    op.create_index(
        "ix_storage_folders_user_id", "storage_folders", ["user_id"]
    )
    op.create_index(
        "ix_storage_folders_parent_id", "storage_folders", ["parent_id"]
    )

    op.add_column("files", sa.Column("folder_id", sa.Uuid(), nullable=True))
    op.create_index("ix_files_folder_id", "files", ["folder_id"])
    op.create_foreign_key(
        "fk_files_folder_id",
        "files",
        "storage_folders",
        ["folder_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_files_folder_id", "files", type_="foreignkey")
    op.drop_index("ix_files_folder_id", table_name="files")
    op.drop_column("files", "folder_id")

    op.drop_index("ix_storage_folders_parent_id", table_name="storage_folders")
    op.drop_index("ix_storage_folders_user_id", table_name="storage_folders")
    op.drop_table("storage_folders")
