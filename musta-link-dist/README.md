# MustaLink Data Connector Agent (`musta-link-agent`)

Agente leve e seguro em Go que estabelece um **túnel reverso multiplexado (TCP sobre WebSocket)** entre bases de dados privadas na infraestrutura do cliente (ex.: PostgreSQL, MySQL, SQL Server, Oracle, MongoDB em `127.0.0.1`) e o backend **MustaInf** na nuvem, sem necessidade de abrir portas públicas no firewall da empresa.

---

## Arquitetura e Segurança Implementadas

1. **Autenticação JWT fora da URL e dos Logs:**
   - O token nunca é enviado na query string (`?token=...`) nem impresso nos logs.
   - Viaja exclusivamente no cabeçalho `Authorization: Bearer <token>` durante o handshake WebSocket.
   - Ligações remotas exigem `wss://` (TLS >= 1.2) por omissão (`ws://` só é permitido para `localhost`/`127.0.0.1` ou com `-allow-insecure` explícito em testes).

2. **Multiplexação de Sessões Concorrentes (`ConnID`):**
   - Protocolo binário de cabeçalho fixo de 9 bytes (`[Tipo: 1B][ConnID: 4B][Tamanho: 4B][Payload]`).
   - Suporta múltiplas consultas e sessões TCP simultâneas no mesmo túnel WebSocket sem colisão de pacotes.

3. **Conexões TCP Sob Pedido (*On-Demand*):**
   - O agente **não** abre ligação à base de dados ao arrancar.
   - Cada ligação TCP local é aberta apenas quando o MustaInf envia `FrameOpen(ConnID)` e encerrada imediatamente após o término da sessão (`FrameClose(ConnID)`).

4. **Prevenção de Escritas Parciais (`writeAll`):**
   - Garante que todos os buffers enviados ao socket TCP local são escritos integralmente, respeitando timeouts de escrita (`writeWait`).

5. **Heartbeat (`Ping/Pong`) e Encerramento Imediato (`Ctrl+C`):**
   - Mantém *keepalive* ativo (`Ping` a cada 20s, `ReadDeadline` de 45s) e reconexão automática com *exponential backoff* (2s até 30s).
   - Interrupções (`SIGINT` / `SIGTERM`) cancelam o contexto e encerram o WebSocket e todas as conexões TCP ativas instantaneamente.

---

## Como Executar

### 1. Gerar um Token de Agente no MustaInf
Faça uma chamada autenticada para emitir um token dedicado ao agente:
```http
POST /conn/agent-tunnel/token
Content-Type: application/json

{
  "agent_id": "luanda-pg-01",
  "expires_days": 30
}
```

### 2. Iniciar o Agente na Infraestrutura Local
```bash
go run tunel.go \
  -server wss://api.mustainfo.cloud \
  -db 127.0.0.1:5432 \
  -agent-id luanda-pg-01 \
  -token SEU_TOKEN_JWT
```
*(Em alternativa ao argumento `-token`, pode definir a variável de ambiente `MUSTA_AGENT_TOKEN` para que o token não conste na lista de processos do sistema operativo).*

### 3. Configurar a Conexão no MustaInf
No formulário de nova conexão do MustaInf:
- **Host:** `agent://luanda-pg-01` (ou simplesmente `agent` / `musta-agent` se usar o `agent-id` padrão `default`)
- **Porta:** `5432` (ou qualquer porta; o backend encaminha automaticamente para o túnel ativo do agente)
- **Utilizador / Password / Base de Dados:** Credenciais locais do seu banco de dados

