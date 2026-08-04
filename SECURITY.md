# Segurança — notas operacionais

Documento de procedimentos. Descreve o que mudou na proteção de credenciais
e o que ainda **exige uma decisão humana** para ficar fechado.

---

## 1. Cifra das credenciais de conexão

### O problema

O esquema anterior (`aes_encrypt`) gerava uma chave AES aleatória e
guardava-a **dentro do próprio texto cifrado**, protegida apenas por uma
cifra de César (deslocamento +3). Qualquer pessoa com acesso de leitura à
tabela `db_connections` — um dump, um backup, uma query de suporte —
decifrava host, utilizador e password de todas as bases de dados dos
clientes sem precisar de segredo nenhum.

### O que mudou

Passaram a existir duas camadas explicitamente separadas em
`app/services/crypto_utils.py`:

| Camada | Funções | Protege? | Onde se usa |
|---|---|---|---|
| Transporte | `aes_encrypt` / `aes_decrypt` | ❌ Não — a chave viaja junto | Envelope partilhado com o frontend (login, payload do utilizador) |
| Em repouso | `secret_encrypt` / `secret_decrypt` | ✅ AES-256-GCM com chave em `ENCRYPTION_KEY` | Credenciais guardadas na BD |

A camada de transporte **não foi alterada** de propósito: o frontend usa o
mesmo esquema e mudá-la unilateralmente partiria o login. Continua a ser
ofuscação, não segurança — está documentado no módulo.

O que mudou foi o **armazenamento**: `app/cruds/connection_cruds.py`
recifra os campos sensíveis com a chave-mestra antes de gravar.

### Configurar

```bash
# Gerar a chave-mestra (uma por ambiente)
python -c "from app.services.crypto_utils import generate_master_key; print(generate_master_key())"
```

Colocar em `ENCRYPTION_KEY` no `.env` **e** num gestor de segredos.

> ⚠️ Perder a `ENCRYPTION_KEY` torna as credenciais guardadas
> irrecuperáveis. Não há recuperação possível — é esse o objetivo.

### Migrar as linhas existentes

A leitura aceita ambos os formatos, por isso a aplicação funciona antes e
depois da migração. Não há downtime.

```bash
# 1. Backup da base de dados (obrigatório)

# 2. Ver o que seria alterado
python -m scripts.migrate_connection_secrets --dry-run

# 3. Aplicar
python -m scripts.migrate_connection_secrets
```

Após a migração, todos os valores ficam com o prefixo `v2.` e deixam de ser
decifráveis sem a chave-mestra.

---

## 2. ⚠️ PENDENTE — segredos no histórico do Git

**Isto ainda não está resolvido e não pode ser resolvido sem decisão vossa.**

O histórico contém commits `Delete .env` e `Delete .env 2`. Apagar um
ficheiro num commit **não o remove do histórico**: o conteúdo continua
acessível em qualquer clone.

```bash
# Confirmar o que está exposto
git log --all --oneline -- .env
git show <commit>:.env
```

Assumir como comprometido tudo o que lá estava: `SECRET_KEY`,
`DATABASE_URL`, credenciais de storage, chaves de API.

### Passo obrigatório: rodar os segredos

Reescrever o histórico **não** desfaz a exposição — quem já clonou tem
cópia. Rodar as credenciais é o que realmente fecha a janela:

- [ ] `SECRET_KEY` — invalida todos os tokens em circulação (obriga a novo login)
- [ ] `ENCRYPTION_KEY` — gerar nova e correr a migração
- [ ] Password da base de dados de produção
- [ ] `STORAGE_ACCESS_KEY` / `STORAGE_SECRET_KEY`
- [ ] `GEMINI_API_KEY`, `DEEPSEEK_API_KEY`
- [ ] `FINGERPRINT_SALT`

### Passo opcional: limpar o histórico

Destrutivo e coordenado — reescreve todos os SHAs, obriga toda a equipa a
re-clonar e quebra referências em PRs e issues. Só depois de rodar os
segredos e com a equipa avisada:

```bash
pip install git-filter-repo
git filter-repo --path .env --path ".env 2" --invert-paths
git push --force --all
```

O `.gitignore` já cobre `.env`, e o hook `bloquear-env` em
`.pre-commit-config.yaml` impede que volte a acontecer via `git add -f`.

---

## 3. Outras alterações

- **CORS** — `allow_origins=["*"]` com `allow_credentials=True` é rejeitado
  pelos browsers e partia o login por cookie. O wildcard passa a desativar
  credenciais em desenvolvimento e a ser recusado com `ENV=production`.
- **Rate limiting** — `/auth/login` limitado a 5 tentativas por 5 minutos,
  contadas por IP **e** por email. Configurável via
  `LOGIN_RATE_LIMIT_ATTEMPTS` / `LOGIN_RATE_LIMIT_WINDOW`.
- **Enumeração de contas** — o login devolvia "E-mail não encontrado" vs.
  "Senha incorreta", permitindo descobrir que contas existem. Agora devolve
  sempre "Credenciais inválidas".
- **Fuga de detalhe em erros** — com `ENV=production`, exceções não tratadas
  deixam de devolver a mensagem interna; erros de SQLAlchemy nunca expõem a
  query. O detalhe fica no log, associado ao `X-Request-ID` devolvido ao cliente.
- **Documentação da API** — `/docs`, `/redoc` e `/openapi.json` ficam
  desativados quando `ENV=production`.
