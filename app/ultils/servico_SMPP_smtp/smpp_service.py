"""
Serviço de envio de SMS via protocolo SMPP (Short Message Peer-to-Peer v3.4).

Implementação nativa em Python (zero dependências externas) utilizando sockets TCP.
Suporta BIND (Transceiver/Transmitter), SUBMIT_SM, ENQUIRE_LINK, UNBIND,
mensagens longas concatenadas (UDH) e codificação UCS2 (acentuação/emojis).
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import socket
import struct
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from app.ultils.logger import log_message
from app.ultils.servico_SMPP_smtp.config import SMPPConfig, get_smpp_config
from app.ultils.servico_SMPP_smtp.template_service import render_sms_template


# Constantes de Comandos SMPP (Command IDs)
CMD_GENERIC_NACK = 0x80000000
CMD_BIND_RECEIVER = 0x00000001
CMD_BIND_RECEIVER_RESP = 0x80000001
CMD_BIND_TRANSMITTER = 0x00000002
CMD_BIND_TRANSMITTER_RESP = 0x80000002
CMD_QUERY_SM = 0x00000003
CMD_QUERY_SM_RESP = 0x80000003
CMD_SUBMIT_SM = 0x00000004
CMD_SUBMIT_SM_RESP = 0x80000004
CMD_DELIVER_SM = 0x00000005
CMD_DELIVER_SM_RESP = 0x80000005
CMD_UNBIND = 0x00000006
CMD_UNBIND_RESP = 0x80000006
CMD_REPLACE_SM = 0x00000007
CMD_REPLACE_SM_RESP = 0x80000007
CMD_CANCEL_SM = 0x00000008
CMD_CANCEL_SM_RESP = 0x80000008
CMD_BIND_TRANSCEIVER = 0x00000009
CMD_BIND_TRANSCEIVER_RESP = 0x80000009
CMD_ENQUIRE_LINK = 0x00000015
CMD_ENQUIRE_LINK_RESP = 0x80000015

# Tabela de status SMPP comuns
SMPP_STATUS_MESSAGES: Dict[int, str] = {
    0x00000000: "ESME_ROK - Sucesso / Nenhuma falha",
    0x00000001: "ESME_RINVMSGLEN - Tamanho da mensagem inválido",
    0x00000002: "ESME_RINVCMDLEN - Tamanho do comando inválido",
    0x00000003: "ESME_RINVCMDID - Command ID inválido",
    0x00000004: "ESME_RINVBNDSTS - Estado de bind incorreto",
    0x00000005: "ESME_RALYBND - Já em estado de bind",
    0x00000006: "ESME_RINVPRTFLG - Flag de prioridade inválida",
    0x00000007: "ESME_RINVREGDLVFLG - Flag de relatório de entrega inválida",
    0x00000008: "ESME_RSYSERR - Erro de sistema no SMSC",
    0x0000000A: "ESME_RINVSRCADR - Endereço de origem inválido",
    0x0000000B: "ESME_RINVDSTADR - Endereço de destino inválido",
    0x0000000C: "ESME_RINVMSGID - Message ID inválido",
    0x0000000D: "ESME_RBINDFAIL - Falha na autenticação/bind",
    0x0000000E: "ESME_RINVPASWD - Senha inválida no SMSC",
    0x0000000F: "ESME_RINVSYSID - System ID inválido no SMSC",
    0x00000011: "ESME_RCANCELFAIL - Falha ao cancelar envio",
    0x00000013: "ESME_RREPLACEFAIL - Falha ao substituir mensagem",
    0x00000014: "ESME_RMSGQFUL - Fila de mensagens do SMSC cheia",
    0x00000015: "ESME_RINVSERTYP - Service Type inválido",
    0x00000033: "ESME_RINVNUMDESTS - Número de destinatários inválido",
    0x00000034: "ESME_RINVDLNAME - Nome da lista de distribuição inválido",
    0x00000040: "ESME_RINVDESTFLAG - Dest Flag inválida",
    0x00000058: "ESME_RTHROTTLED - Limite de taxa de envio excedido (Throttled)",
}


class SMPPError(Exception):
    """Exceção base para erros de protocolo SMPP."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class SMPPPDU:
    """Estrutura e parser básico de PDU (Protocol Data Unit) SMPP."""

    def __init__(
        self,
        command_id: int,
        command_status: int = 0,
        sequence_number: int = 1,
        body: bytes = b"",
    ):
        self.command_id = command_id
        self.command_status = command_status
        self.sequence_number = sequence_number
        self.body = body

    def pack(self) -> bytes:
        """Serializa o PDU em bytes com cabeçalho de 16 bytes Big-Endian."""
        length = 16 + len(self.body)
        header = struct.pack(">IIII", length, self.command_id, self.command_status, self.sequence_number)
        return header + self.body

    @classmethod
    def unpack_header(cls, header_bytes: bytes) -> Tuple[int, int, int, int]:
        """Lê os 16 bytes de cabeçalho SMPP."""
        if len(header_bytes) < 16:
            raise SMPPError("Cabeçalho PDU incompleto.")
        length, command_id, command_status, sequence_number = struct.unpack(">IIII", header_bytes[:16])
        return length, command_id, command_status, sequence_number


class SMPPService:
    """
    Cliente SMPP 3.4 nativo em Python para envio de SMS.
    """

    def __init__(self, config: Optional[SMPPConfig] = None):
        self.config = config or get_smpp_config()
        self._sequence_counter = random.randint(100, 1000)

    def _next_sequence(self) -> int:
        self._sequence_counter = (self._sequence_counter + 1) & 0x7FFFFFFF
        if self._sequence_counter == 0:
            self._sequence_counter = 1
        return self._sequence_counter

    def _cstring(self, value: str) -> bytes:
        """Converte uma string para C-String terminada em nulo (ASCII/Latin1)."""
        return value.encode("latin-1", errors="replace") + b"\x00"

    def _connect_and_bind(self) -> socket.socket:
        """Abre a conexão TCP e efetua o BIND (Transceiver ou Transmitter)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.config.timeout)

        try:
            sock.connect((self.config.host, self.config.port))
        except (socket.error, OSError) as e:
            sock.close()
            raise SMPPError(f"Falha ao conectar no SMSC {self.config.host}:{self.config.port}: {e}")

        # Determina o comando de bind
        bind_cmd = (
            CMD_BIND_TRANSMITTER
            if self.config.bind_type == "transmitter"
            else CMD_BIND_TRANSCEIVER
        )
        expected_resp_cmd = (
            CMD_BIND_TRANSMITTER_RESP
            if self.config.bind_type == "transmitter"
            else CMD_BIND_TRANSCEIVER_RESP
        )

        seq = self._next_sequence()

        # Monta corpo do PDU de BIND
        body = bytearray()
        body.extend(self._cstring(self.config.system_id))
        body.extend(self._cstring(self.config.password))
        body.extend(self._cstring(self.config.system_type))
        # interface_version: 0x34 para 3.4
        body.append(0x34)
        # addr_ton, addr_npi, address_range
        body.append(self.config.source_addr_ton)
        body.append(self.config.source_addr_npi)
        body.extend(self._cstring(""))

        pdu = SMPPPDU(command_id=bind_cmd, sequence_number=seq, body=bytes(body))
        self._send_pdu(sock, pdu)

        resp = self._read_pdu(sock)
        if resp.command_id != expected_resp_cmd:
            sock.close()
            raise SMPPError(f"Resposta inesperada do SMSC no bind: 0x{resp.command_id:08X}")

        if resp.command_status != 0:
            status_desc = SMPP_STATUS_MESSAGES.get(resp.command_status, "Código desconhecido")
            sock.close()
            raise SMPPError(
                f"SMSC recusou BIND (Status 0x{resp.command_status:08X}): {status_desc}",
                status_code=resp.command_status,
            )

        return sock

    def _send_pdu(self, sock: socket.socket, pdu: SMPPPDU) -> None:
        """Envia um PDU completo através do socket."""
        sock.sendall(pdu.pack())

    def _read_exact(self, sock: socket.socket, num_bytes: int) -> bytes:
        """Lê exatamente `num_bytes` do socket."""
        buffer = bytearray()
        while len(buffer) < num_bytes:
            chunk = sock.recv(num_bytes - len(buffer))
            if not chunk:
                raise SMPPError("Conexão fechada prematuramente pelo SMSC.")
            buffer.extend(chunk)
        return bytes(buffer)

    def _read_pdu(self, sock: socket.socket) -> SMPPPDU:
        """Lê um PDU completo recebido do SMSC."""
        header_data = self._read_exact(sock, 16)
        length, command_id, command_status, sequence_number = SMPPPDU.unpack_header(header_data)

        body_len = length - 16
        body_data = b""
        if body_len > 0:
            body_data = self._read_exact(sock, body_len)

        return SMPPPDU(
            command_id=command_id,
            command_status=command_status,
            sequence_number=sequence_number,
            body=body_data,
        )

    def _unbind_and_close(self, sock: socket.socket) -> None:
        """Envia UNBIND de forma graciosa e fecha o socket."""
        try:
            seq = self._next_sequence()
            unbind_pdu = SMPPPDU(command_id=CMD_UNBIND, sequence_number=seq)
            self._send_pdu(sock, unbind_pdu)
            # Lê UNBIND_RESP com timeout reduzido
            sock.settimeout(3.0)
            self._read_pdu(sock)
        except Exception:
            pass
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def test_connection(self) -> Dict[str, Any]:
        """
        Testa a conectividade e autenticação no servidor SMSC via BIND e ENQUIRE_LINK.
        """
        if not self.config.enabled:
            return {
                "success": False,
                "message": "O serviço SMPP está desabilitado na configuração (SMPP_ENABLED=False).",
                "config": self.config.masked_dict(),
            }

        start_time = time.time()
        sock: Optional[socket.socket] = None
        try:
            sock = self._connect_and_bind()

            # Envia enquire_link para verificar o canal ativo
            enq_seq = self._next_sequence()
            self._send_pdu(sock, SMPPPDU(command_id=CMD_ENQUIRE_LINK, sequence_number=enq_seq))
            resp = self._read_pdu(sock)

            elapsed = round(time.time() - start_time, 3)

            if resp.command_id != CMD_ENQUIRE_LINK_RESP or resp.command_status != 0:
                return {
                    "success": False,
                    "message": f"Enquire link falhou (Status: 0x{resp.command_status:08X})",
                }

            log_message(
                f"Conexão SMPP com {self.config.host}:{self.config.port} testada com sucesso ({elapsed}s)",
                level="info",
                source="SMPPService",
            )
            return {
                "success": True,
                "message": f"Conexão SMPP e autenticação no SMSC estabelecidas com sucesso ({elapsed}s).",
                "details": {
                    "host": self.config.host,
                    "port": self.config.port,
                    "system_id": self.config.system_id,
                    "bind_type": self.config.bind_type,
                    "latency_seconds": elapsed,
                },
            }
        except SMPPError as e:
            msg = f"Falha no teste SMPP: {e}"
            log_message(msg, level="error", source="SMPPService")
            return {"success": False, "message": msg, "error": str(e), "status_code": e.status_code}
        except Exception as e:
            msg = f"Erro inesperado no teste SMPP: {e}"
            log_message(msg, level="error", source="SMPPService")
            return {"success": False, "message": msg, "error": str(e)}
        finally:
            if sock:
                self._unbind_and_close(sock)

    def _normalize_phone_number(self, phone: str) -> str:
        """Limpa e formata o número de telefone em padrão numérico internacional."""
        clean = re.sub(r"[^\d]", "", phone)
        # Se começar com 00, converte para padrão internacional
        if clean.startswith("00"):
            clean = clean[2:]
        return clean

    def _prepare_message_payload(
        self, text: str, forced_data_coding: Optional[int] = None
    ) -> Tuple[List[bytes], int, int]:
        """
        Determina a codificação mais adequada (GSM 7-bit vs UCS2) e divide mensagens
        longas em segmentos com cabeçalho UDH se necessário.
        
        Retorna:
            (lista_de_partes_em_bytes, data_coding, esm_class)
        """
        # Checa se há caracteres fora do ASCII padrão
        is_unicode = any(ord(c) > 127 for c in text)
        data_coding = forced_data_coding if forced_data_coding is not None else (8 if is_unicode else 0)

        if data_coding == 8:
            # Codificação UCS2 / UTF-16-BE
            encoded = text.encode("utf-16-be")
            max_single = 140  # bytes (70 caracteres)
            max_concat = 134  # bytes por segmento (67 caracteres com UDH de 6 bytes)
        else:
            # Codificação SMSC padrão / GSM 7-bit
            encoded = text.encode("latin-1", errors="replace")
            max_single = 160  # bytes
            max_concat = 153  # bytes por segmento com UDH de 6 bytes

        if len(encoded) <= max_single:
            return [encoded], data_coding, 0x00

        # Mensagem longa concatenada
        parts: List[bytes] = []
        chunks = [encoded[i : i + max_concat] for i in range(0, len(encoded), max_concat)]
        total_parts = len(chunks)
        ref_num = random.randint(1, 255)

        for idx, chunk in enumerate(chunks, start=1):
            # UDH de concatenação padrão 8-bit reference:
            # [0x05, 0x00, 0x03, ref_num, total_parts, part_idx]
            udh = struct.pack("BBBBBB", 0x05, 0x00, 0x03, ref_num, total_parts, idx)
            parts.append(udh + chunk)

        # esm_class = 0x40 indica presença de User Data Header (UDH)
        return parts, data_coding, 0x40

    def send_sms(
        self,
        to: Union[str, List[str]],
        text: Optional[str] = None,
        template_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        sender_id: Optional[str] = None,
        data_coding: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Envia uma mensagem SMS para um ou múltiplos destinatários.
        Suporta renderização de templates de texto a partir de template_name.
        """
        if not self.config.enabled:
            return {
                "success": False,
                "message": "Serviço SMPP desabilitado na configuração (SMPP_ENABLED=False).",
            }

        # Se informado template_name, renderiza o texto usando o TemplateService
        if template_name:
            text = render_sms_template(template_name, context or {})

        if not text:
            return {"success": False, "message": "Nenhum texto de mensagem ou template informado para envio do SMS."}

        recipients = [to] if isinstance(to, str) else list(to)
        clean_recipients = [self._normalize_phone_number(p) for p in recipients if self._normalize_phone_number(p)]

        if not clean_recipients:
            return {"success": False, "message": "Nenhum número de destinatário válido informado."}

        source = sender_id or self.config.source_addr
        # Ajusta TON de origem: se for alfanumérico (letras) usa TON=5, se for puramente números usa TON=1
        source_is_alpha = any(c.isalpha() for c in source)
        source_ton = 5 if source_is_alpha else self.config.source_addr_ton

        parts, encoding, esm_class = self._prepare_message_payload(text, data_coding)

        results: List[Dict[str, Any]] = []
        sock: Optional[socket.socket] = None

        try:
            sock = self._connect_and_bind()

            for phone in clean_recipients:
                message_ids: List[str] = []
                for part in parts:
                    seq = self._next_sequence()

                    # Monta corpo de SUBMIT_SM
                    body = bytearray()
                    body.extend(self._cstring(""))  # service_type
                    body.append(source_ton)  # source_addr_ton
                    body.append(self.config.source_addr_npi)  # source_addr_npi
                    body.extend(self._cstring(source))  # source_addr
                    body.append(self.config.dest_addr_ton)  # dest_addr_ton
                    body.append(self.config.dest_addr_npi)  # dest_addr_npi
                    body.extend(self._cstring(phone))  # destination_addr
                    body.append(esm_class)  # esm_class
                    body.append(0x00)  # protocol_id
                    body.append(0x00)  # priority_flag
                    body.extend(self._cstring(""))  # schedule_delivery_time
                    body.extend(self._cstring(""))  # validity_period
                    body.append(0x01)  # registered_delivery (1=solicita confirmação se suportado)
                    body.append(0x00)  # replace_if_present_flag
                    body.append(encoding)  # data_coding
                    body.append(0x00)  # sm_default_msg_id
                    body.append(len(part))  # sm_length
                    body.extend(part)  # short_message bytes

                    submit_pdu = SMPPPDU(command_id=CMD_SUBMIT_SM, sequence_number=seq, body=bytes(body))
                    self._send_pdu(sock, submit_pdu)

                    resp = self._read_pdu(sock)
                    if resp.command_id != CMD_SUBMIT_SM_RESP:
                        raise SMPPError(f"Resposta inválida de submit_sm: 0x{resp.command_id:08X}")

                    if resp.command_status != 0:
                        status_desc = SMPP_STATUS_MESSAGES.get(resp.command_status, "Erro de envio")
                        raise SMPPError(
                            f"Falha no envio para {phone} (Status: 0x{resp.command_status:08X}): {status_desc}",
                            status_code=resp.command_status,
                        )

                    # Lê o message_id do corpo da resposta (C-String)
                    msg_id_str = resp.body.split(b"\x00")[0].decode("ascii", errors="ignore")
                    message_ids.append(msg_id_str)

                results.append({
                    "phone": phone,
                    "success": True,
                    "message_ids": message_ids,
                    "parts_count": len(parts),
                })
                log_message(
                    f"SMS enviado para {phone} ({len(parts)} partes, id={message_ids[0] if message_ids else 'ok'})",
                    level="success",
                    source="SMPPService",
                )

            return {
                "success": True,
                "total_recipients": len(clean_recipients),
                "parts_per_message": len(parts),
                "data_coding": encoding,
                "results": results,
            }

        except Exception as e:
            msg = f"Erro no envio de SMS via SMPP: {e}"
            log_message(msg, level="error", source="SMPPService")
            return {"success": False, "message": msg, "error": str(e), "results": results}
        finally:
            if sock:
                self._unbind_and_close(sock)

    def send_sms_template(
        self,
        to: Union[str, List[str]],
        template_name: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Envia um SMS renderizando o template especificado da pasta de templates.
        """
        return self.send_sms(
            to=to,
            template_name=template_name,
            context=context,
            **kwargs,
        )

    async def async_send_sms(self, *args, **kwargs) -> Dict[str, Any]:
        """Versão assíncrona não bloqueante para FastAPI e tarefas em background."""
        return await asyncio.to_thread(self.send_sms, *args, **kwargs)


# Instância global reutilizável
_default_smpp_service: Optional[SMPPService] = None


def get_smpp_service(config: Optional[SMPPConfig] = None) -> SMPPService:
    """Retorna uma instância de SMPPService."""
    global _default_smpp_service
    if config is not None:
        return SMPPService(config)
    if _default_smpp_service is None:
        _default_smpp_service = SMPPService()
    return _default_smpp_service
