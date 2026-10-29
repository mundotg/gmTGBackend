# 🛡️ Dry-Run & Avaliação de Risco Pré-Execução (Impact Predictor)

O **Impact Predictor (Dry-Run)** é um subsistema avançado de governança e proteção do **MustaInfo**, inspirado no mecanismo de simulação e previsão de custo do **Google BigQuery** e adaptado para bancos relacionais heterogêneos (PostgreSQL, MySQL, MariaDB, SQLite, Oracle, SQL Server) e NoSQL (MongoDB).

O objetivo é avaliar o impacto, custo, linhas estimadas e tipo de varredura (Index Scan vs. Full Table/Sequential/Collection Scan) **antes** de o usuário executar a consulta ou iniciar uma transferência de dados volumosa, prevenindo quedas e degradação de performance nos bancos de dados dos clientes.

---

## 🌟 Principais Capacidades

- **Previsão em Tempo Real**: Roda o planejador nativo de cada banco sem executar mutações nem carregar milhões de linhas.
- **Detecção de Varredura Completa**: Identifica `Seq Scan` (PostgreSQL), `ALL` (MySQL) e `COLLSCAN` (MongoDB).
- **Classificação de Risco**: Categorização em 4 níveis (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`) com pontuação de 0 a 100%.
- **Sugestões Inteligentes de Otimização**:
  - Geração automática da instrução exata para criação de índices (`CREATE INDEX ...` no SQL ou `db.coll.createIndex(...)` no MongoDB).
  - Recomendação de adição de cláusula `LIMIT`.
  - Alerta de `SELECT *` desnecessário em tabelas largas.
- **Impact Guard (Guarda Pré-Execução)**: Intercepta consultas de alto risco no SQL Editor e solicita confirmação explícita do desenvolvedor antes da execução.
- **Design Fail-Safe**: Se o plano não puder ser gerado ou o banco não suportar comandos específicos, o sistema degrada graciosamente e não interrompe o fluxo normal de trabalho.

---

## 🏗️ Arquitetura do Sistema

```
                      ┌──────────────────────────────────────┐
                      │          Interface Usuário           │
                      │   (SQL Editor / Transações Query)    │
                      └──────────────────┬───────────────────┘
                                         │
                   POST /sql-editor/impact (ou /transfer/impact-query)
                                         │
                                         ▼
                 ┌─────────────────────────────────────────────────┐
                 │       app.services.impact_predictor_service     │
                 │              ImpactPredictorService             │
                 └───────┬─────────────────┬─────────────────┬─────┘
                         │                 │                 │
                         ▼                 ▼                 ▼
                  [ PostgreSQL ]       [ MySQL ]        [ MongoDB ]
                    EXPLAIN JSON     EXPLAIN JSON      queryPlanner
                         │                 │                 │
                         └────────┬────────┴─────────────────┘
                                  ▼
                   ┌───────────────────────────────┐
                   │    Extrator & Classificador   │
                   │    - scan_type                │
                   │    - estimated_rows           │
                   │    - risk_level               │
                   │    - suggestions (CREATE INDEX)│
                   └──────────────┬────────────────┘
                                  ▼
                   ┌───────────────────────────────┐
                   │    ImpactPredictorCard (UI)   │
                   │    - Métricas Visuais         │
                   │    - Alertas de Risco         │
                   │    - Modal de Confirmação     │
                   └───────────────────────────────┘
```

---

## 🔌 Motores Suportados e Mecanismos Nativos

| Motor de Banco | Comando de Dry-Run Utilizado | O que é extraído |
| :--- | :--- | :--- |
| **PostgreSQL** | `EXPLAIN (FORMAT JSON) <query>` | `Node Type` (`Seq Scan`, `Index Scan`, `Bitmap Heap Scan`), `Plan Rows`, `Total Cost`, `Filter` |
| **MySQL / MariaDB** | `EXPLAIN FORMAT=JSON <query>` | `access_type` (`ALL` vs `ref`/`range`), `rows_examined_per_scan`, `query_cost`, `key` utilizado |
| **MongoDB** | `coll.find(filter).explain("queryPlanner")` | `winningPlan.stage` (`COLLSCAN` vs `IXSCAN`), `estimated_document_count()` |
| **SQLite** | `EXPLAIN QUERY PLAN <query>` | Detalhe textual: `SCAN TABLE` vs `SEARCH TABLE USING INDEX` |
| **SQL Server / Oracle / Outros** | `EXPLAIN <query>` + Heurísticas Estáticas | Análise léxica de ausência de `WHERE`, `LIMIT` e comandos DML destrutivos |

> ⚠️ **Segurança Estrita**: O MustaInfo **nunca** executa `EXPLAIN ANALYZE` no dry-run de operações de escrita, pois em PostgreSQL o `ANALYZE` executa a query física. O Impact Predictor utiliza estritamente o `EXPLAIN` puro do otimizador de consultas.

---

## 🚦 Matriz de Níveis de Risco

| Nível | Score | Condições Típicas | Comportamento no Frontend |
| :---: | :---: | :--- | :--- |
| **LOW** 🟢 | 0 – 25% | • Uso de índice (`Index Scan`, `IXSCAN`)<br>• Linhas estimadas < 5.000<br>• Cláusula `LIMIT` explícita pequena | Executa imediatamente sem confirmação. Card informativo opcional. |
| **MEDIUM** 🟡 | 26 – 60% | • Varredura completa em tabelas médias (5.000 a 20.000 linhas)<br>• Consulta sem `LIMIT` em volume moderado | Exibe alerta e sugestões no card de resultados. |
| **HIGH** 🟠 | 61 – 85% | • `Sequential Scan` / `COLLSCAN` entre 20.000 e 100.000 linhas<br>• Ausência de índice em colunas de filtro frequente | **Impact Guard ativo**: Abre modal de aviso pré-execução com opção de confirmação consciente. |
| **CRITICAL** 🔴 | 86 – 100% | • `Seq Scan` em mais de 100.000 linhas<br>• `DELETE` ou `UPDATE` sem cláusula `WHERE`<br>• Instruções `DROP` ou `TRUNCATE` | **Impact Guard obrigatório**: Exige confirmação explícita ("Executar Mesmo Assim") para evitar acidentes. |

---

## 📡 Endpoints da API

### 1. `POST /sql-editor/explain` (ou `POST /sql-editor/impact`)
Avalia uma consulta no contexto da conexão ativa do usuário.

**Request Body:**
```json
{
  "query": "SELECT * FROM pedidos WHERE data_criacao >= '2026-01-01'"
}
```

**Response Payload (`QueryImpact`):**
```json
{
  "success": true,
  "message": "Previsão de impacto concluída",
  "data": {
    "supported": true,
    "dialect": "postgresql",
    "risk_level": "HIGH",
    "risk_score": 75,
    "scan_type": "SEQUENTIAL_SCAN",
    "table_name": "pedidos",
    "estimated_rows": 1450000,
    "estimated_cost": 8420.5,
    "estimated_duration_ms": 4352.0,
    "summary": "PostgreSQL Plan: SEQUENTIAL_SCAN em 'pedidos'. Linhas estimadas: ~1,450,000 (Custo: 8420.5).",
    "warning": "A consulta fará um Sequential Scan em ~1,450,000 linhas na tabela 'pedidos' com custo 8420.5.",
    "suggestions": [
      "Considere criar um índice: CREATE INDEX idx_pedidos_data_criacao ON pedidos(data_criacao);",
      "Adicione LIMIT 1000 para limitar o tráfego de rede e a memória do cliente.",
      "Evite 'SELECT *': especifique apenas as colunas que realmente precisa."
    ],
    "requires_confirmation": true,
    "plan": [ ... ]
  }
}
```

### 2. `POST /transfer/impact-query`
Avalia uma consulta que será usada para extrair dados em uma transferência de dados entre bases.

**Request Body:**
```json
{
  "connection_id": 14,
  "query": "SELECT id, nome, valor FROM faturas WHERE status = 'pendente'",
  "table_name": "faturas"
}
```

---

## 💻 Integração no Frontend

### 1. Componente `ImpactPredictorCard.tsx`
Localização: [app/component/ImpactPredictorCard.tsx](file:///c:/Users/Francemysebasti%C3%A3o/Documents/MustaInfo/gmTGFrontEnd/app/component/ImpactPredictorCard.tsx)
- Suporta dois modos:
  - **Inline**: Renderizado no painel de resultados ao clicar no botão *"🔍 Explain"*.
  - **Modal**: Renderizado como guarda de segurança interceptando o botão *"▶ Executar"* quando o risco for `HIGH` ou `CRITICAL`.
- Inclui botão de cópia com 1 clique para as instruções SQL de indexação sugeridas.
- Gaveta retrátil para inspecionar o JSON bruto do otimizador (`raw_plan`).

### 2. No SQL Editor
Localização: [app/home/editorsql/page.tsx](file:///c:/Users/Francemysebasti%C3%A3o/Documents/MustaInfo/gmTGFrontEnd/app/home/editorsql/page.tsx)
- Botão *"🔍 Explain"* agora apresenta o card de diagnóstico de impacto.
- Ao pressionar `Ctrl+Enter` ou clicar em *"▶ Executar"*, o pré-flight check roda em milissegundos:
  - Se for seguro, a query executa de imediato.
  - Se for crítico, surge o modal preventivo de confirmação.

### 3. Na Transferência por Query
Localização: [app/home/transacoes/components/transacao_query_component/index.tsx](file:///c:/Users/Francemysebasti%C3%A3o/Documents/MustaInfo/gmTGFrontEnd/app/home/transacoes/components/transacao_query_component/index.tsx)
- Botão *"⚡ Avaliar Impacto (Dry-Run)"* adicionado na barra de ferramentas.
- Execução em paralelo durante a amostragem de dados, alertando se a extração causará travamentos na base de origem.
