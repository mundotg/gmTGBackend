#!/usr/bin/env bash
# Instalador e Emparelhador do MustaLink Data Connector Agent (Linux / macOS)
# Não contém tokens embutidos: utiliza o código temporário (XXXX-XXXX) gerado no MustaInf.

set -euo pipefail

SERVER_URL="${MUSTA_SERVER_URL:-wss://api.mustainfo.cloud}"
DB_HOST="${MUSTA_DB_HOST:-127.0.0.1:5432}"
AGENT_ID="${MUSTA_AGENT_ID:-luanda-pg-01}"
PAIR_CODE="${MUSTA_PAIR_CODE:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_SRC="${SCRIPT_DIR}/musta-link-agent"

echo "======================================================"
echo "  MustaLink Data Connector Agent — Instalador Unix    "
echo "======================================================"

if [[ ! -f "${BIN_SRC}" ]]; then
  echo "Erro: binário 'musta-link-agent' não encontrado em ${SCRIPT_DIR}." >&2
  exit 1
fi

chmod +x "${BIN_SRC}"

if [[ -z "${PAIR_CODE}" ]]; then
  read -r -p "Introduza o Código de Emparelhamento gerado no MustaInf (ex: ABCD-1234): " PAIR_CODE
fi

read -r -p "Endereço da base de dados local [${DB_HOST}]: " INPUT_DB
if [[ -n "${INPUT_DB}" ]]; then
  DB_HOST="${INPUT_DB}"
fi

INSTALL_DIR="${HOME}/.local/bin"
mkdir -p "${INSTALL_DIR}"
cp "${BIN_SRC}" "${INSTALL_DIR}/musta-link-agent"
chmod 0755 "${INSTALL_DIR}/musta-link-agent"

echo "[+] Binário instalado em: ${INSTALL_DIR}/musta-link-agent"
echo "[+] A iniciar emparelhamento seguro e túnel reverso..."

exec "${INSTALL_DIR}/musta-link-agent" \
  -server "${SERVER_URL}" \
  -db "${DB_HOST}" \
  -agent-id "${AGENT_ID}" \
  -pair "${PAIR_CODE}"

