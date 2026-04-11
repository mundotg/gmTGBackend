from __future__ import annotations

import hashlib
from fastapi import Request

# Idealmente, carregue isso de variáveis de ambiente (ex: pydantic BaseSettings)
TRUST_PROXY_HEADERS = True


def _get_ip_from_headers(request: Request) -> str | None:
    headers = request.headers

    # 🔥 Prioridade 1: Cloudflare (Se você colocar o CF na frente do Render)
    # É o mais seguro pois o CF descarta headers falsos enviados pelo cliente.
    cf_ip = headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()

    # 🔥 Prioridade 2: X-Real-IP
    # O Render/Nginx geralmente força esse header com o IP da conexão real.
    # É mais difícil de sofrer "spoofing" do que o X-Forwarded-For.
    x_real_ip = headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip.strip()

    # 🔥 Prioridade 3: X-Forwarded-For (Padrão do Render)
    x_forwarded_for = headers.get("x-forwarded-for")
    if x_forwarded_for:
        ips = [ip.strip() for ip in x_forwarded_for.split(",")]
        # O Render adiciona o IP real no final da cadeia ou repassa limpo.
        # Pegar o ips[0] é padrão, mas o ideal para evitar IP spoofing
        # é usar o middleware do Uvicorn (veja a dica abaixo).
        return ips[0] if ips else None

    return None


def get_client_ip(request: Request) -> str:
    if TRUST_PROXY_HEADERS:
        ip = _get_ip_from_headers(request)
        if ip:
            return ip

    if request.client and request.client.host:
        return request.client.host

    return "0.0.0.0"


def normalize_user_agent(ua: str | None) -> str:
    # Trata caso o header venha ausente (None) direto da Request
    return " ".join((ua or "").strip().lower().split())


def hash_user_agent(ua: str) -> str:
    """🔐 Não expõe UA bruto no token e padroniza a saída"""
    return hashlib.sha256(ua.encode("utf-8")).hexdigest()


def ip_prefix(ip: str) -> str:
    """Extrai sub-rede para evitar que pequenas mudanças de IP quebrem a sessão"""
    if ":" in ip:  # IPv6 - Retorna o bloco /64
        parts = ip.split(":")
        # Garante que não vai estourar o índice se o IP for malformado
        return ":".join(parts[: min(4, len(parts))])
    else:  # IPv4 - Retorna o bloco /24
        parts = ip.split(".")
        if len(parts) == 4:
            return ".".join(parts[:3])
        return ip


def build_fingerprint(request: Request, salt: str | None = None) -> dict:
    ua_raw = normalize_user_agent(request.headers.get("user-agent"))
    ua_hash = hash_user_agent(ua_raw)

    ip = get_client_ip(request)
    ip_pref = ip_prefix(ip)

    # Concatenação segura garantindo que o salt (mesmo None) vire string vazia
    raw = f"{ua_hash}|{ip_pref}|{salt or ''}".encode("utf-8")
    fp = hashlib.sha256(raw).hexdigest()

    return {
        "user_agent": ua_hash,  # 🔥 agora seguro
        "user_ip_prefix": ip_pref,
        "fp": fp,
    }
