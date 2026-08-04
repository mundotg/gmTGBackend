from __future__ import annotations

import base64
import hashlib
import os
import secrets

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.config.dotenv import get_env

# ============================================================
#  ⚠️  LEIA ANTES DE MEXER
# ============================================================
#
#  Este módulo tem DUAS camadas com propósitos diferentes.
#  Não as confundas — uma protege, a outra não.
#
#  1) CAMADA DE TRANSPORTE  →  aes_encrypt / aes_decrypt
#     Envelope partilhado com o frontend. A chave viaja DENTRO do
#     próprio texto cifrado (protegida só por cifra de César).
#     Qualquer pessoa que veja o valor consegue decifrá-lo.
#     Isto NÃO é segurança: é ofuscação. Só existe porque o
#     frontend usa o mesmo esquema (ver auth_routes.login) e
#     mudá-lo unilateralmente partiria o cliente.
#     ➜ NUNCA usar para guardar seja o que for na base de dados.
#
#  2) CAMADA EM REPOUSO  →  secret_encrypt / secret_decrypt
#     AES-256-GCM real, com chave-mestra vinda de ENCRYPTION_KEY
#     (variável de ambiente, nunca no código nem na BD).
#     ➜ É esta que deve proteger credenciais guardadas.
#
# ============================================================


# ============================================================
#  Camada 1 — TRANSPORTE (ofuscação, compatível com frontend)
# ============================================================

def cifraCesar(text: str, shift: int) -> str:
    out = []
    for ch in text:
        code = ord(ch)
        # A-Z
        if 65 <= code <= 90:
            out.append(chr(((code - 65 + shift + 26) % 26) + 65))
        # a-z
        elif 97 <= code <= 122:
            out.append(chr(((code - 97 + shift + 26) % 26) + 97))
        else:
            out.append(ch)
    return "".join(out)


def gerarSenha(
    personalizada1: str = "tg",
    personalizada2: str = "EDU",
    size: int = 23
) -> str:
    caracteres = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+"

    def gerarAleatorio(tamanho: int) -> str:
        # criptograficamente seguro
        return "".join(secrets.choice(caracteres) for _ in range(tamanho))

    def removerPalavrasDuplicadas(s: str) -> str:
        palavras = s.split("-")
        # preserva ordem e remove duplicadas
        seen = set()
        uniq = []
        for p in palavras:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return " ".join(uniq)

    def removerPalavras(s: str, palavra: str) -> str:
        return s.replace(palavra, "")

    senhaBase = ""

    def embaralha_cada_palavra(personalizada: str | None) -> str:
        newPalavra = ""
        if personalizada:
            palavras = personalizada.split("-")
            if palavras:
                for _ in range(len(palavras)):
                    idx = secrets.randbelow(len(palavras))
                    newPalavra += palavras[idx] + "-"
            else:
                newPalavra += personalizada + "-"
        return newPalavra

    def processarPersonalizada(personalizada: str | None) -> None:
        nonlocal senhaBase
        if not personalizada:
            return
        palavras = personalizada.split(" ")
        if palavras:
            for _ in range(len(palavras)):
                idx = secrets.randbelow(len(palavras))
                senhaBase += palavras[idx] + "-"
        else:
            senhaBase += personalizada + "-"

    processarPersonalizada(personalizada1)
    processarPersonalizada(personalizada2)

    senhaEmbaralhada = embaralha_cada_palavra(senhaBase)
    randor = secrets.randbelow(30)

    if randor in (2, 3, 25, 26):
        senhaEmbaralhada = removerPalavrasDuplicadas(senhaEmbaralhada)
    if randor > 23:
        senhaEmbaralhada = embaralha_cada_palavra(senhaEmbaralhada)

    restante = size - len(senhaEmbaralhada)
    if restante < 0:
        restante = 0

    senhaAleatoria = senhaEmbaralhada + "-" + gerarAleatorio(restante)

    senhaBase = removerPalavras(senhaAleatoria, "-")
    senhaBase = removerPalavras(senhaBase, " ")

    if len(senhaBase) > size:
        senhaBase = senhaBase[:size]

    restante2 = size - len(senhaBase)
    if restante2 < 0:
        restante2 = 0

    senhaBase += gerarAleatorio(restante2)

    if len(senhaBase) > size:
        senhaBase = senhaBase[:size]

    return senhaBase


def _normalize_key(raw: bytes) -> bytes:
    """Ajusta material de chave para os 32 bytes exigidos pelo AES-256."""
    if len(raw) < 32:
        return raw + b"\x00" * (32 - len(raw))
    return raw[:32]


def aes_encrypt(text: str) -> str:
    """
    ⚠️ OFUSCAÇÃO DE TRANSPORTE — NÃO É SEGURANÇA.

    A chave é gerada aqui e embutida no resultado (cifra de César +3),
    portanto qualquer pessoa que veja o valor consegue decifrá-lo.
    Existe apenas por compatibilidade com o frontend.

    Para guardar segredos usa `secret_encrypt`.
    """
    secretKey = gerarSenha(personalizada1="tg", personalizada2="EDU", size=32)

    key_bytes = _normalize_key(secretKey.encode("utf-8"))

    # IV de 12 bytes (padrão GCM)
    iv = os.urandom(12)

    aesgcm = AESGCM(key_bytes)

    # cryptography retorna: ciphertext || tag
    ct_and_tag = aesgcm.encrypt(iv, text.encode("utf-8"), None)
    ciphertext = ct_and_tag[:-16]
    tag = ct_and_tag[-16:]

    chaveCesar = cifraCesar(secretKey, 3)

    # layout: chave(32 chars) + ivHex(24) + tagHex(32) + cipherHex
    return f"{chaveCesar}{iv.hex()}{tag.hex()}{ciphertext.hex()}"


def aes_decrypt(encryptedText: str) -> str:
    """
    ⚠️ Contraparte de `aes_encrypt` — ver aviso lá.
    """
    # layout fixo: 32 + 24 + 32 + resto
    if not encryptedText:
        return encryptedText

    chaveCesar = encryptedText[:32]
    ivHex = encryptedText[32:32 + 24]
    tagHex = encryptedText[32 + 24:32 + 24 + 32]
    cipherHex = encryptedText[32 + 24 + 32:]

    secretKey = cifraCesar(chaveCesar, -3)
    key_bytes = _normalize_key(secretKey.encode("utf-8"))

    iv = bytes.fromhex(ivHex)
    tag = bytes.fromhex(tagHex)
    ciphertext = bytes.fromhex(cipherHex)

    aesgcm = AESGCM(key_bytes)

    # cryptography espera ciphertext||tag
    return aesgcm.decrypt(iv, ciphertext + tag, None).decode("utf-8")


# ============================================================
#  Camada 2 — EM REPOUSO (AES-256-GCM com chave-mestra)
# ============================================================

# Prefixo que identifica o formato novo. Valores sem este prefixo
# são tratados como legado (camada de transporte) para que a
# migração possa ser gradual e sem downtime.
_V2_PREFIX = "v2."

_master_key_cache: bytes | None = None


def _derive_master_key(raw: str) -> bytes:
    """
    Deriva 32 bytes a partir do valor de ENCRYPTION_KEY.

    Aceita duas formas:
    - base64 de 32 bytes (formato recomendado, gerado por `generate_master_key`)
    - qualquer string — nesse caso passa por SHA-256
    """
    try:
        decoded = base64.urlsafe_b64decode(raw)
        if len(decoded) == 32:
            return decoded
    except Exception:  # noqa: S110 - não é base64; o fallback SHA-256 trata disso
        pass

    return hashlib.sha256(raw.encode("utf-8")).digest()


def get_master_key() -> bytes:
    """
    Devolve a chave-mestra derivada de ENCRYPTION_KEY.

    Levanta RuntimeError se a variável não estiver definida — falhar
    ruidosamente aqui é preferível a guardar credenciais em claro.
    """
    global _master_key_cache

    if _master_key_cache is not None:
        return _master_key_cache

    raw = get_env("ENCRYPTION_KEY")
    if not raw:
        raise RuntimeError(
            "ENCRYPTION_KEY não está definida. Gera uma chave com:\n"
            "  python -c \"from app.services.crypto_utils import generate_master_key; "
            "print(generate_master_key())\"\n"
            "e coloca-a no .env (nunca no código nem no repositório)."
        )

    _master_key_cache = _derive_master_key(raw)
    return _master_key_cache


def generate_master_key() -> str:
    """Gera uma chave-mestra nova, pronta a colar no .env."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")


def is_encrypted_at_rest(value: str | None) -> bool:
    """True se o valor já está no formato novo (chave-mestra)."""
    return bool(value) and value.startswith(_V2_PREFIX)


def secret_encrypt(plaintext: str | None) -> str:
    """
    Cifra um segredo para armazenamento, com AES-256-GCM e chave-mestra.

    A chave NÃO viaja com o resultado — fica em ENCRYPTION_KEY. Sem essa
    variável, quem obtiver um dump da base de dados não decifra nada.

    Formato: "v2." + base64(nonce[12] || ciphertext || tag[16])
    """
    if not plaintext:
        return ""

    aesgcm = AESGCM(get_master_key())
    nonce = os.urandom(12)
    ct_and_tag = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)

    blob = base64.urlsafe_b64encode(nonce + ct_and_tag).decode("utf-8")
    return f"{_V2_PREFIX}{blob}"


def secret_decrypt(value: str | None) -> str:
    """
    Decifra um segredo guardado.

    Aceita ambos os formatos, para permitir migração gradual:
    - "v2.…"  → AES-256-GCM com chave-mestra
    - outro   → formato legado (ofuscação de transporte)

    Assim, linhas ainda não migradas continuam a funcionar; o script
    `scripts/migrate_connection_secrets.py` converte-as para v2.
    """
    if not value:
        return ""

    if not is_encrypted_at_rest(value):
        # Legado: valor gravado com o envelope antigo.
        return aes_decrypt(value)

    raw = base64.urlsafe_b64decode(value[len(_V2_PREFIX):])
    nonce, ct_and_tag = raw[:12], raw[12:]

    try:
        return AESGCM(get_master_key()).decrypt(nonce, ct_and_tag, None).decode("utf-8")
    except InvalidTag as exc:
        raise ValueError(
            "Falha ao decifrar segredo: ENCRYPTION_KEY errada ou dado corrompido."
        ) from exc


def reencrypt_at_rest(value: str | None) -> str:
    """
    Normaliza um valor para o formato v2.

    - Já em v2  → devolve inalterado (idempotente)
    - Legado    → decifra e recifra com a chave-mestra
    - Vazio     → devolve ""

    Usado tanto no boundary de escrita como no script de migração.
    """
    if not value:
        return ""

    if is_encrypted_at_rest(value):
        return value

    return secret_encrypt(aes_decrypt(value))
