# 📡 Serviço de Mensageria: SMPP (SMS) & SMTP (E-mail)

Este módulo fornece a infraestrutura de configuração e envio de mensagens para a aplicação **MustaInf**, centralizando o envio de e-mails via **SMTP** e mensagens de texto SMS via protocolo **SMPP 3.4**.

---

## 📁 Estrutura de Arquivos

```
app/ultils/servico_SMPP_smtp/
├── __init__.py         # Exportações públicas e funções de conveniência
├── config.py           # Modelos Pydantic e carregamento de variáveis do .env
├── template_service.py # Motor de renderização de templates Jinja2 (E-mail e SMS)
├── smtp_service.py     # Cliente SMTP nativo (TLS, SSL, HTML, anexos, retentativas)
├── smpp_service.py     # Cliente SMPP 3.4 nativo via sockets (Bind, Submit_SM, UCS2, UDH)
├── manager.py          # MessagingManager unificado (notificações multicanal)
├── templates/          # 🎨 Pasta de templates centralizada
│   ├── emails/         # Modelos de e-mail (HTML e TXT)
│   │   ├── confirmacao_registo.html
│   │   ├── confirmacao_registo.txt
│   │   ├── recuperacao_senha.html
│   │   └── recuperacao_senha.txt
│   └── sms/            # Modelos de SMS (TXT)
│       ├── confirmacao_registo.txt
│       └── codigo_verificacao.txt
└── README.md           # Guia de configuração e uso
```

---

## ⚙️ Variáveis de Ambiente (.env)

Adicione as variáveis abaixo no seu arquivo `.env`:

### ✉️ Configurações SMTP (E-mail)
```env
# -----------------------------
# ✉️ SMTP (E-mail)
# -----------------------------
SMTP_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=seu_email@empresa.com
SMTP_PASSWORD=sua_senha_de_app
SMTP_USE_TLS=true
SMTP_USE_SSL=false
SMTP_FROM_EMAIL=notificacoes@mustainf.com
SMTP_FROM_NAME="MustaInf Notificações"
SMTP_REPLY_TO=suporte@mustainf.com
SMTP_TIMEOUT=30
SMTP_MAX_RETRIES=3
```

### 📱 Configurações SMPP (SMS)
```env
# -----------------------------
# 📱 SMPP (SMS)
# -----------------------------
SMPP_ENABLED=true
SMPP_HOST=smsc.operadora.com
SMPP_PORT=2775
SMPP_SYSTEM_ID=seu_system_id
SMPP_PASSWORD=sua_senha_smsc
SMPP_SYSTEM_TYPE=""
SMPP_BIND_TYPE=transceiver
SMPP_SOURCE_ADDR=MustaInf
SMPP_SOURCE_TON=5
SMPP_SOURCE_NPI=0
SMPP_DEST_TON=1
SMPP_DEST_NPI=1
SMPP_DATA_CODING=0
SMPP_TIMEOUT=30
SMPP_MAX_RETRIES=3
```

---

## 🚀 Como Usar no Código

### 1. Enviar E-mail (Síncrono ou Assíncrono)

```python
from app.ultils.servico_SMPP_smtp import send_email, async_send_email

# Exemplo simples (texto e HTML):
resultado = send_email(
    to="cliente@empresa.com",
    subject="Recuperação de Palavra-passe",
    body_text="O seu código de verificação é: 123456",
    body_html="<h3>MustaInf</h3><p>O seu código de verificação é: <b>123456</b></p>",
)

if resultado.get("success"):
    print("E-mail enviado com ID:", resultado.get("message_id"))
```

Em rotas FastAPI assíncronas:
```python
@router.post("/enviar-alerta")
async def rota_enviar_alerta(dados: AlertaSchema):
    res = await async_send_email(
        to=dados.destinatario,
        subject="Alerta do Sistema",
        body_text=dados.mensagem,
    )
    return res
```

### 2. Enviar E-mail usando Templates (Recomendado)

Os modelos HTML e texto são lidos automaticamente da pasta `templates/emails/`:

```python
from app.ultils.servico_SMPP_smtp import send_email

resultado = send_email(
    to="usuario@empresa.com",
    subject="Confirmação de Registo - MustaInf",
    template_name="confirmacao_registo",  # procura emails/confirmacao_registo.html e .txt
    context={
        "user_name": "Maria Silva",
        "empresa": "Tech Lda",
        "login_url": "https://app.mustainf.com/auth/login",
    },
)
```

---

### 2. Enviar SMS via SMPP

```python
from app.ultils.servico_SMPP_smtp import send_sms, async_send_sms

# Envio de SMS (reconhece caracteres especiais/acentos automaticamente via UCS2):
resultado = send_sms(
    to="+244923456789",
    text="Olá! O seu código de confirmação no MustaInf é 987654.",
)

if resultado.get("success"):
    print("SMS despachado com sucesso!")
```

---

### 3. Usar o `MessagingManager` (Notificação Multicanal)

```python
from app.ultils.servico_SMPP_smtp import get_messaging_manager

manager = get_messaging_manager()

# Envia tanto por e-mail como por SMS simultaneamente:
res = manager.send_notification(
    channel="both",
    message="O seu relatório mensal de dados foi gerado com sucesso.",
    subject="Relatório Mensal Disponível",
    recipient_email="usuario@empresa.com",
    recipient_phone="+244912345678",
)
```

---

### 4. Testar a Conexão dos Serviços

```python
from app.ultils.servico_SMPP_smtp import test_all_connections, test_smtp_connection, test_smpp_connection

# Testar apenas SMTP:
status_smtp = test_smtp_connection()
print(status_smtp)

# Testar apenas SMPP:
status_smpp = test_smpp_connection()
print(status_smpp)

# Testar ambos:
saude = test_all_connections()
print(saude["overall_status"])
```

---

## 🔒 Segurança e Logs

- Todas as senhas são automaticamente **mascaradas** (`********`) em qualquer serialização (`masked_dict()`).
- As tentativas e eventuais falhas são registadas automaticamente através do logger padrão da aplicação (`app.ultils.logger.log_message`).
