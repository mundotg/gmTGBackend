import boto3
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)
from typing import Iterator, List, Optional, Tuple
import os
import tempfile
import threading
import shutil
import logging

_logger = logging.getLogger("cloud.config")


def log_message(message: str, level: str = "info") -> None:
    """Wrapper com import preguiçoso para evitar importação circular com app/main/storage_routes."""
    try:
        from app.ultils.logger import log_message as _app_log
        _app_log(message, level=level)
    except Exception:
        log_func = getattr(_logger, level if hasattr(_logger, level) else "info")
        log_func(message)

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


def _atomic_copy(path: str, source) -> None:
    """Como `_atomic_write`, mas copiando de um ficheiro-objeto em chunks."""
    dir_name = os.path.dirname(path)
    os.makedirs(dir_name, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    os.close(fd)
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(source, f, length=1024 * 1024)
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
        self.region = os.getenv("STORAGE_REGION", "us-east-1")

        os.makedirs(self.local_cache, exist_ok=True)

        access_key = os.getenv("STORAGE_ACCESS_KEY")
        secret_key = os.getenv("STORAGE_SECRET_KEY")
        internal_endpoint = os.getenv("STORAGE_ENDPOINT")
        # Endpoint PÚBLICO para URLs pré-assinadas (o browser não resolve
        # `host.docker.internal`). Se não definido, usa o interno.
        public_endpoint = os.getenv("STORAGE_PUBLIC_ENDPOINT") or internal_endpoint

        _cfg = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=2,
            retries={"max_attempts": 1},
        )

        # Cliente interno (upload/list/delete/stream) — dentro do contentor.
        self.s3 = boto3.client(
            "s3",
            endpoint_url=internal_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=self.region,
            config=_cfg,
        )

        # Cliente para PRÉ-ASSINAR (assina com o host público → válido no browser).
        if public_endpoint and public_endpoint != internal_endpoint:
            self.s3_public = boto3.client(
                "s3",
                endpoint_url=public_endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=self.region,
                config=_cfg,
            )
        else:
            self.s3_public = self.s3

        # Garante o bucket (idempotente). Nunca derruba o arranque.
        try:
            self.ensure_bucket()
        except Exception as e:  # noqa: BLE001
            log_message(f"[STORAGE] ensure_bucket falhou (segue com fallback local): {e}", "warning")

    def ensure_bucket(self) -> None:
        """Cria o bucket se ainda não existir (idempotente)."""
        if not self.bucket:
            return
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchBucket", "NotFound"):
                try:
                    self.s3.create_bucket(Bucket=self.bucket)
                    log_message(f"[STORAGE] Bucket criado: {self.bucket}", "info")
                except Exception as create_err:
                    log_message(f"[STORAGE] Não foi possível criar bucket na cloud ({create_err}), fallback local ativo.", "warning")
            else:
                log_message(f"[STORAGE] head_bucket falhou ({e}), fallback local ativo.", "warning")
        except Exception as e:
            log_message(f"[STORAGE] Verificação do bucket falhou ({e}), fallback local ativo.", "warning")

    # -------------------------
    # 🔒 Validações
    # -------------------------
    def _validate_filename(self, key: str):
        """Valida a *key* do objeto.

        A key inclui o prefixo do dono (`{user_id}/{uuid}.ext`), por isso `/` é
        permitido — mas nunca `..`, caminho absoluto ou separador do Windows.
        """
        if not key or key.strip() == "":
            raise ValueError("Nome do ficheiro inválido")

        if ".." in key or key.startswith("/") or "\\" in key:
            raise ValueError("Path inválido (tentativa de ataque detectada 😅)")

    def _get_local_path(self, key: str) -> str:
        """Mapeia a key para o cache local preservando a pasta do utilizador."""
        return os.path.join(self.local_cache, *key.split("/"))

    # -------------------------
    # 📤 Upload
    # -------------------------
    def _sync_local_to_cloud(self, local_path: str, key: str, content_type: Optional[str] = None) -> None:
        if not (self.bucket and self.s3 and os.path.exists(local_path)):
            return
        extra = {"ContentType": content_type} if content_type else {}
        try:
            with open(local_path, "rb") as f_cloud:
                self.s3.upload_fileobj(
                    f_cloud,
                    self.bucket,
                    key,
                    ExtraArgs=extra or None,
                )
            log_message(f"Upload sincronizado com a cloud: {key}", "info")
        except (EndpointConnectionError, ClientError) as e:
            log_message(
                f"Cloud indisponível ({str(e)}), ficheiro mantido no storage local: {key}",
                "warning",
            )
        except Exception as e:
            log_message(
                f"Falha ao sincronizar com cloud ({str(e)}), ficheiro mantido no storage local: {key}",
                "warning",
            )

    def upload_file(
        self,
        file,
        key: str,
        content_type: Optional[str] = None,
        sync_async: bool = True,
    ) -> str:
        try:
            self._validate_filename(key)

            if file is None:
                raise ValueError("Ficheiro inválido")

            # Valida e verifica se o ficheiro está acessível e não está vazio
            try:
                file.seek(0, os.SEEK_END)
                if file.tell() == 0:
                    raise ValueError("Ficheiro vazio")
                file.seek(0)
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"Ficheiro inacessível ou inválido: {e}")

            # 1. Salva localmente primeiro de forma atômica (garante persistência 100% segura e resposta imediata)
            local_path = self._get_local_path(key)
            _atomic_copy(local_path, file)

            # 2. Sincroniza com o MinIO/S3 remoto em background para não bloquear o request HTTP (evita timeout de 10s)
            if self.bucket and self.s3:
                if sync_async:
                    threading.Thread(
                        target=self._sync_local_to_cloud,
                        args=(local_path, key, content_type),
                        daemon=True,
                    ).start()
                else:
                    self._sync_local_to_cloud(local_path, key, content_type)

            return key

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
    def generate_url(
        self,
        key: str,
        expires: int = 3600,
        fallback_url: Optional[str] = None,
    ) -> str:
        """URL pré-assinada do objeto, ou `fallback_url` se ele só existir local.

        `generate_presigned_url` é uma operação **offline** — assina sem tocar na
        rede e por isso nunca falha, mesmo com o storage em baixo. Sem o
        `head_object` abaixo, devolvia links para objetos que nunca chegaram ao
        bucket (o upload tinha caído no cache local) e o browser recebia 404.
        """
        try:
            self._validate_filename(key)

            # confirma que o objeto existe E que a cloud responde
            try:
                self.s3.head_object(Bucket=self.bucket, Key=key)
                return self.s3_public.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=expires,
                )
            except Exception as e:
                log_message(
                    f"Objeto indisponível na cloud ({key}): {e}. A usar fallback.",
                    "warning",
                )

            # fallback local → serve pela rota autenticada de download
            local_path = self._get_local_path(key)
            if os.path.exists(local_path) and fallback_url:
                return fallback_url

            raise FileNotFoundError("Ficheiro não encontrado")

        except FileNotFoundError:
            raise

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

            # Salva localmente primeiro de forma atômica
            local_path = self._get_local_path(key)
            _atomic_write(local_path, data)

            if self.bucket and self.s3:
                try:
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=data,
                    )
                    log_message(f"[CACHE] Upload bytes cloud: {key}", "info")
                except Exception as e:
                    log_message(
                        f"[CACHE] Cloud falhou ({e}), salvo apenas local: {key}",
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

            except Exception as e:
                # NoSuchKey, ClientError, etc. caem para o local
                log_message(
                    f"[CACHE] Cloud falhou ao ler {key} ({e}), tentando local...",
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
    def get_file_stream(
        self,
        key: str,
        chunk_size: int = 1024 * 1024,
    ) -> Tuple[Iterator[bytes], int, Optional[str]]:
        """Streaming do ficheiro em chunks: `(generator, tamanho, content_type)`.

        O tamanho vem sempre da **fonte real** (objeto ou ficheiro em cache) e
        não da BD — declarar um `Content-Length` que não bate com os bytes
        enviados faz o browser truncar o download ou ficar pendurado.
        """

        try:
            self._validate_filename(key)

            # 🔹 1. Prioriza cache local (leitura imediata em disco, sem latência de rede
            # e disponível no próprio milissegundo após o upload enquanto sincroniza em background)
            local_path = self._get_local_path(key)
            if os.path.exists(local_path):
                size = os.path.getsize(local_path)

                def stream_local():
                    with open(local_path, "rb") as f:
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk

                log_message(f"Streaming local: {key}", "info")
                return stream_local(), size, None

            # 🔹 2. Fallback / Origem Cloud (MinIO/S3)
            try:
                response = self.s3.get_object(
                    Bucket=self.bucket,
                    Key=key,
                )

                body = response["Body"]
                size = int(response.get("ContentLength") or 0)
                content_type = response.get("ContentType")

                def stream():
                    try:
                        while True:
                            chunk = body.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk
                    finally:
                        body.close()  # 🔥 MUITO IMPORTANTE

                log_message(f"Streaming da cloud: {key}", "info")
                return stream(), size, content_type

            except Exception as e:
                log_message(
                    f"Objeto não encontrado na cloud nem localmente: {key} | {str(e)}",
                    "warning",
                )
                raise FileNotFoundError("Ficheiro não encontrado")

        except (ValueError, FileNotFoundError):
            raise

        except Exception as e:
            log_message(f"Erro no stream: {str(e)}", "error")
            raise RuntimeError(f"Erro ao fazer stream: {str(e)}")
