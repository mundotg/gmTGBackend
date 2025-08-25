import socket
from app.ultils.logger import log_message

def is_port_open(host: str, port: int, timeout: int = 3) -> bool:
    """
    Testa se a porta de um host está acessível.

    Args:
        host (str): Endereço do servidor.
        port (int): Porta do servidor.
        timeout (int): Tempo máximo em segundos.

    Returns:
        bool: True se a porta está aberta, False caso contrário.
    """
    if not host or not port:
        log_message(f"⚠️ Host ou porta inválidos: host={host}, port={port}", "warning")
        return False

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port)))
            return result == 0
    except Exception as e:
        log_message(
            f"⚠️ Falha ao verificar porta {port} em {host} | Erro: {type(e).__name__}: {str(e)}",
            "warning"
        )
        return False
