# 🚀 Plataforma Analítica e Pipeline de Dados para Monitoramento de Bases de Dados

###### MustaInf

> **Resumo:** Projeto end-to-end que integra **engenharia de dados, análise e fundamentos de ciência de dados** para monitorar, auditar e extrair insights acionáveis de bases de dados relacionais. A solução combina  **API em FastAPI** ,  **PostgreSQL** , **dashboards** e um **módulo de ML opcional** para apoiar a tomada de decisão.

---

## 🎯 Problema de Negócio

Equipes técnicas e analíticas precisam **monitorar desempenho, uso e qualidade** de bases de dados para:

* identificar gargalos de consultas,
* priorizar otimizações (índices, modelos),
* reduzir erros operacionais,
* apoiar decisões técnicas e de negócio com métricas confiáveis.

Este projeto centraliza  **metadados, logs e estatísticas** , transforma-os em **KPIs** e disponibiliza **insights** via API, dashboards e relatórios.

---

## 🧱 Arquitetura & Pipeline (End-to-End)

**Fluxo:**

1. **Ingestão** : metadados, logs e resultados de queries (streaming).
2. **Armazenamento** : PostgreSQL com modelos normalizados e índices.
3. **Qualidade** : validações, deduplicação e logs de erro.
4. **Métricas** : agregações e estatísticas de desempenho.
5. **Análise** : EDA e KPIs (SQL/Python).
6. **Consumo** : API FastAPI + Frontend (Next.js) + Relatórios.
7. **ML (opcional)** : detecção de anomalias e previsão simples.

> **Tecnologias:** FastAPI, SQLAlchemy, Alembic, PostgreSQL, Pydantic, React/Next.js.

---

## 📊 Métricas & KPIs

* **Tempo médio de execução** de consultas
* **Volume de consultas** por período/usuário
* **Taxa de erro** por tipo de operação
* **Tabelas mais acessadas**
* **Crescimento de dados** por tabela

> **Decisão apoiada:** priorizar índices, otimizações e ações preventivas.

---

## 🧠 Insights Obtidos (Exemplos)

* Identificação de **consultas de alto custo** recorrentes
* **Padrões de uso** por usuário/projeto
* **Picos anômalos** de latência
* Oportunidades claras de **indexação**

---

## 🛠️ Funcionalidades Avançadas de Gestão de Dados

Além da análise e monitoramento, a plataforma oferece  **operações críticas de gestão de dados** , integradas ao pipeline analítico:

### 🔹 Gestão Estrutural do Banco

* Eliminação controlada de **atributos (colunas)**
* Eliminação segura de **tabelas**
* Validação de impacto antes da execução (schemas, dependências)

### 🔹 Transações de Dados

* Execução de **transações ACID**
* Operações de dados entre **diferentes tipos de bases de dados**
  * PostgreSQL
  * MySQL
  * Outros bancos relacionais via drivers
* Garantia de consistência e rollback automático em caso de falha

### 🔹 Backup e Restore

* Geração de **backups completos ou parciais**
* Restore seguro de bases de dados
* Suporte a versionamento e auditoria de backups

### 🔹 Digitalização Inteligente de Dados (OCR)

* Leitura de **texto a partir de imagens** (OCR)
* Suporte à digitalização de documentos
* Conversão de dados não estruturados em dados estruturados
* Integração com o pipeline para armazenamento e análise

> **Valor de negócio:** reduz riscos operacionais, facilita migrações, garante continuidade do serviço e acelera a digitalização de dados.

---

## 🤖 Módulo de Machine Learning (Opcional, Recomendado)

### Objetivo

Adicionar **valor analítico** ao backend com modelos simples e explicáveis.

### Casos de Uso

* **Detecção de anomalias** no tempo de execução de queries
* **Previsão** de carga/latência (baseline)
* **Clusterização** de padrões de uso

### Modelo Inicial (MVP)

**Detecção de Anomalias com Isolation Forest** (simples e eficaz).

#### 📁 Estrutura Sugerida

```
app/
 ├── ml/
 │   ├── __init__.py
 │   ├── datasets.py        # preparação de dados
 │   ├── features.py        # engenharia de features
 │   ├── train.py           # treino offline
 │   ├── inference.py       # inferência online
 │   └── schemas.py         # contratos (Pydantic)
```

#### 🔧 Features (exemplos)

* tempo_execucao
* tamanho_resultado
* hora_do_dia
* tipo_query

#### 🧪 Treino (exemplo)

```python
from sklearn.ensemble import IsolationForest
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(X_train)
```

#### 🚀 Inferência via API

```python
score = model.decision_function(X)
anomalia = model.predict(X) == -1
```

> **Entrega de valor:** alertar consultas anômalas antes de afetarem o sistema.

---

## 🧪 Qualidade de Dados

* Tratamento de nulos
* Deduplicação
* Validações de esquema
* Logs e auditoria

---

## 🧩 Estrutura do Backend

> Organização em camadas (routes, services, models, schemas) com migrations (Alembic), logs e cache.

---

## ▶️ Execução

```bash
uvicorn app.main:app --reload
```

Docs (Swagger): `http://localhost:8000/docs`

---

## 📈 Dashboards & Relatórios

* Dashboard interativo (KPIs)
* Relatórios PDF/Excel
* Prints e links no repositório

---

## ⚠️ Limitações

* Foco em dados estruturados
* ML introdutório (baseline)
* Dependência da qualidade dos logs

---

## 🧭 Próximos Passos

* Expandir ML (forecasting)
* Alertas em tempo real
* Versionamento de modelos

---

## 🧑‍💻 Autor

**Francemy Eduardo Sebastião** — Full Stack / Data

## 📜 Licença

MIT License
