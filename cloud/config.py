import boto3
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)
from typing import List
import os
import tempfile
import shutil

from app.ultils.logger import log_message


def _atomic_write(path: str, data_bytes: bytes) -> None:
    """Grava de forma atômica para evitar ficheiros corrompidos."""
    dir_name = os.path.dirname(path)
    os.makedirs(dir_name, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    os.close(fd)
    try:
        with open(tmp_path, "wb") as f:
            f.write(data_bytes)
        shutil.move(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


class StorageService:
    def __init__(self):
        self.bucket = os.getenv("STORAGE_BUCKET")
        self.local_cache = os.getenv("STORAGE_LOCAL_PATH", "./storage_cache")

        os.makedirs(self.local_cache, exist_ok=True)

        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("STORAGE_ENDPOINT"),
            aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY"),
            region_name=os.getenv("STORAGE_REGION", "us-east-1"),
            config=Config(signature_version="s3v4"),
        )

    # -------------------------
    # 🔒 Validações
    # -------------------------
    def _validate_filename(self, filename: str):
        if not filename or filename.strip() == "":
            raise ValueError("Nome do ficheiro inválido")

        if ".." in filename or filename.startswith("/"):
            raise ValueError("Path inválido (tentativa de ataque detectada 😅)")

    def _get_local_path(self, filename: str) -> str:
        return os.path.join(self.local_cache, filename)

    # -------------------------
    # 📤 Upload
    # -------------------------
    def upload_file(self, file, filename: str) -> str:
        try:
            self._validate_filename(filename)

            if file is None:
                raise ValueError("Ficheiro inválido")

            data = file.read()

            if not data:
                raise ValueError("Ficheiro vazio")

            # tenta cloud
            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=filename,
                    Body=data,
                )
                log_message(f"Upload feito para cloud: {filename}", "info")
                return filename

            except (EndpointConnectionError, ClientError) as e:
                # fallback local
                local_path = self._get_local_path(filename)
                _atomic_write(local_path, data)

                log_message(
                    f"Cloud indisponível, salvo localmente: {filename} | {str(e)}",
                    "warning",
                )

                return filename

        except ValueError:
            raise

        except NoCredentialsError:
            raise PermissionError("Credenciais inválidas")

        except Exception as e:
            log_message(f"Erro inesperado no upload: {str(e)}", "error")
            raise RuntimeError(f"Erro inesperado no upload: {str(e)}")

    # -------------------------
    # 📂 Listar
    # -------------------------
    def list_files(self) -> List[str]:
        files = []

        # tenta cloud
        try:
            res = self.s3.list_objects_v2(Bucket=self.bucket)
            files.extend([obj["Key"] for obj in res.get("Contents", [])])
        except Exception as e:
            log_message(f"Erro ao listar cloud: {str(e)}", "warning")

        # fallback local
        try:
            local_files = os.listdir(self.local_cache)
            files.extend(local_files)
        except Exception as e:
            log_message(f"Erro ao listar local: {str(e)}", "error")

        return list(set(files))  # remove duplicados

    # -------------------------
    # 🗑️ Delete
    # -------------------------
    def delete_file(self, filename: str):
        try:
            self._validate_filename(filename)

            # tenta cloud
            try:
                self.s3.delete_object(Bucket=self.bucket, Key=filename)
                log_message(f"Removido da cloud: {filename}", "info")
            except Exception:
                pass

            # remove local
            local_path = self._get_local_path(filename)
            if os.path.exists(local_path):
                os.remove(local_path)
                log_message(f"Removido local: {filename}", "info")

        except Exception as e:
            log_message(f"Erro ao apagar ficheiro: {str(e)}", "error")
            raise RuntimeError(f"Erro ao apagar ficheiro: {str(e)}")

    # -------------------------
    # 🔗 URL
    # -------------------------
    def generate_url(self, filename: str, expires: int = 3600) -> str:
        try:
            self._validate_filename(filename)

            # tenta cloud
            try:
                return self.s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.bucket, "Key": filename},
                    ExpiresIn=expires,
                )
            except Exception:
                pass

            # fallback local
            local_path = self._get_local_path(filename)
            if os.path.exists(local_path):
                return f"http://localhost:8000/storage/local/{filename}"

            raise FileNotFoundError("Ficheiro não encontrado")

        except Exception as e:
            log_message(f"Erro ao gerar URL: {str(e)}", "error")
            raise RuntimeError(f"Erro ao gerar URL: {str(e)}")

        # -------------------------

    # 📦 Upload de bytes (para cache)
    # -------------------------
    def upload_bytes(self, data: bytes, key: str):
        try:
            self._validate_filename(key)

            if not data:
                raise ValueError("Dados vazios")

            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=data,
                )
                log_message(f"[CACHE] Upload bytes: {key}", "warning")

            except (EndpointConnectionError, ClientError) as e:
                # fallback local
                local_path = self._get_local_path(key)
                _atomic_write(local_path, data)

                log_message(
                    f"[CACHE] Fallback local (upload_bytes): {key} | {str(e)}",
                    "warning",
                )

        except Exception as e:
            log_message(f"[CACHE] Erro upload_bytes: {e}", "error")
            raise

    # -------------------------
    # 📥 Ler bytes (para cache)
    # -------------------------
    def get_file_bytes(self, key: str) -> bytes | None:
        try:
            self._validate_filename(key)

            # tenta cloud
            try:
                response = self.s3.get_object(
                    Bucket=self.bucket,
                    Key=key,
                )
                return response["Body"].read()

            except self.s3.exceptions.NoSuchKey:
                return None

            except (EndpointConnectionError, ClientError) as e:
                log_message(
                    f"[CACHE] Cloud falhou ao ler {key}, tentando local...",
                    "warning",
                )

            # fallback local
            local_path = self._get_local_path(key)

            if os.path.exists(local_path):
                with open(local_path, "rb") as f:
                    return f.read()

            return None

        except Exception as e:
            log_message(f"[CACHE] Erro get_file_bytes: {e}", "error")
            return None

    # -------------------------
    # 📥 STREAM (Download eficiente)
    # -------------------------
    def get_file_stream(self, filename: str, chunk_size: int = 1024 * 1024):
        """
        Retorna um generator que faz streaming do ficheiro em chunks.
        Suporta cloud (MinIO/S3) e fallback local.
        """

        try:
            self._validate_filename(filename)

            # 🔹 Tenta cloud (MinIO/S3)
            try:
                response = self.s3.get_object(
                    Bucket=self.bucket,
                    Key=filename,
                )

                body = response["Body"]

                def stream():
                    try:
                        while True:
                            chunk = body.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk
                    finally:
                        body.close()  # 🔥 MUITO IMPORTANTE

                log_message(f"Streaming da cloud: {filename}", "info")
                return stream()

            except self.s3.exceptions.NoSuchKey:
                raise FileNotFoundError("Ficheiro não encontrado na cloud")

            except (EndpointConnectionError, ClientError) as e:
                log_message(
                    f"Cloud falhou, fallback local: {filename} | {str(e)}",
                    "warning",
                )

            # 🔹 Fallback local
            local_path = self._get_local_path(filename)

            if not os.path.exists(local_path):
                raise FileNotFoundError("Ficheiro não encontrado")

            def stream_local():
                with open(local_path, "rb") as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk

            log_message(f"Streaming local: {filename}", "info")
            return stream_local()

        except ValueError:
            raise

        except Exception as e:
            log_message(f"Erro no stream: {str(e)}", "error")
            raise RuntimeError(f"Erro ao fazer stream: {str(e)}")
