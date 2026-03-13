from __future__ import annotations

import hashlib
from fastapi import Request

# Se estiveres atrás de proxy (Nginx/Traefik/Cloudflare),
# liga isto e garante que confias no proxy.
TRUST_PROXY_HEADERS = True

def _get_ip_from_headers(request: Request) -> str | None:
    # Cloudflare
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()

    # Nginx / proxies comuns
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # pode vir "client, proxy1, proxy2"
        return xff.split(",")[0].strip()

    xrip = request.headers.get("x-real-ip")
    if xrip:
        return xrip.strip()

    return None

def get_client_ip(request: Request) -> str:
    if TRUST_PROXY_HEADERS:
        ip = _get_ip_from_headers(request)
        if ip:
            return ip

    # fallback
    if request.client and request.client.host:
        return request.client.host

    return "0.0.0.0"

def normalize_user_agent(ua: str) -> str:
    return " ".join((ua or "").strip().lower().split())

def ip_prefix(ip: str) -> str:
    # tolerância pra redes móveis:
    # IPv4 => /24 (primeiros 3 octetos)
    # IPv6 => /64 (primeiros 4 blocos)
    if ":" in ip:  # IPv6
        parts = ip.split(":")
        return ":".join(parts[:4])
    else:          # IPv4
        parts = ip.split(".")
        if len(parts) >= 3:
            return ".".join(parts[:3])
        return ip

def build_fingerprint(request: Request, salt: str) -> dict:
    ua = normalize_user_agent(request.headers.get("user-agent", ""))
    ip = get_client_ip(request)
    ip_pref = ip_prefix(ip)

    raw = f"{ua}|{ip_pref}|{salt}".encode("utf-8")
    fp = hashlib.sha256(raw).hexdigest()

    return {
        "user_agent": ua,
        "user_ip_prefix": ip_pref,
        "fp": fp,
    }