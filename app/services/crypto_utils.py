from __future__ import annotations

import os
import secrets
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


# =========================
# Mantido: cifra de César
# =========================
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


# =========================
# Mantido: gerarSenha
# (melhorado: usa secrets)
# =========================
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

    def embaralha_cada_palavra(personalizada: Optional[str]) -> str:
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

    def processarPersonalizada(personalizada: Optional[str]) -> None:
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


# =========================
# AES-256-GCM (compatível)
# =========================
def aes_encrypt(text: str) -> str:
    # Mantendo tua lógica: chave gerada + "César"
    secretKey = gerarSenha(personalizada1="tg", personalizada2="EDU", size=32)

    # chave precisa ser 32 bytes
    key_bytes = secretKey.encode("utf-8")
    if len(key_bytes) < 32:
        key_bytes = key_bytes + b"\x00" * (32 - len(key_bytes))
    elif len(key_bytes) > 32:
        key_bytes = key_bytes[:32]

    # IV de 12 bytes (padrão GCM)
    iv = os.urandom(12)

    aesgcm = AESGCM(key_bytes)

    # cryptography retorna: ciphertext || tag
    ct_and_tag = aesgcm.encrypt(iv, text.encode("utf-8"), None)
    ciphertext = ct_and_tag[:-16]
    tag = ct_and_tag[-16:]

    chaveCesar = cifraCesar(secretKey, 3)

    # layout: chave(32 chars) + ivHex(24) + tagHex(32) + cipherHex
    ivHex = iv.hex()
    tagHex = tag.hex()
    cipherHex = ciphertext.hex()

    return f"{chaveCesar}{ivHex}{tagHex}{cipherHex}"


def aes_decrypt(encryptedText: str) -> str:
    # layout fixo: 32 + 24 + 32 + resto
    if not encryptedText:
        return encryptedText
    chaveCesar = encryptedText[:32]
    ivHex = encryptedText[32:32 + 24]
    tagHex = encryptedText[32 + 24:32 + 24 + 32]
    cipherHex = encryptedText[32 + 24 + 32:]

    secretKey = cifraCesar(chaveCesar, -3)

    key_bytes = secretKey.encode("utf-8")
    if len(key_bytes) < 32:
        key_bytes = key_bytes + b"\x00" * (32 - len(key_bytes))
    elif len(key_bytes) > 32:
        key_bytes = key_bytes[:32]

    iv = bytes.fromhex(ivHex)
    tag = bytes.fromhex(tagHex)
    ciphertext = bytes.fromhex(cipherHex)

    aesgcm = AESGCM(key_bytes)

    # cryptography espera ciphertext||tag
    pt = aesgcm.decrypt(iv, ciphertext + tag, None)
    return pt.decode("utf-8")


# if __name__ == "__main__":
#     ic=  aes_encrypt("testando incript")
#     print(ic)
#     dc = aes_decrypt(ic)
#     print(dc)

    