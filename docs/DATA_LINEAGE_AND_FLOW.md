# 🗺️ Linhagem Visual de Dados (Data Lineage & Flow)

A **Linhagem Visual de Dados (Data Lineage & Flow)** é o módulo de governança, auditoria e rastreabilidade do **MustaInfo**, projetado para responder à pergunta fundamental de Diretores de Dados (CDOs), Auditores e Engenheiros de Dados:  
> *"De onde veio cada dado, quais transformações foram aplicadas e quais tabelas ou relatórios dependem dele?"*

---

## 🌟 Principais Capacidades

- **Grafo Dirigido Acíclico (DAG) Interativo**: Mapeamento visual em nós e arestas com layout hierárquico topológico (Origens ➔ Transformações ➔ Destinos).
- **Suporte a Múltiplos Bancos Heterogêneos**: Mapeia fluxos entre PostgreSQL, MySQL, MongoDB, SQLite e outros drivers suportados.
- **Rastreamento Automático e Não Bloqueante**: Ao concluir qualquer transferência de dados (por tabela ou por query customizada), o evento é persistido automaticamente em segundo plano.
- **Análise de Impacto (Upstream & Downstream)**:
  - **Upstream**: Identifica as tabelas originais e filtros que alimentam a entidade selecionada.
  - **Downstream**: Identifica todos os bancos e tabelas consumidoras, emitindo um score de risco (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`) antes de operações como `DROP TABLE` ou `ALTER TABLE`.
- **Trilha de Auditoria Completa**: Registro detalhado com volume de linhas transferidas, latência em milissegundos, usuário executor, status e query SQL/MQL aplicada.

---

## 🏗️ Arquitetura do Sistema

```
 ┌──────────────────────┐        ┌───────────────────────┐        ┌──────────────────────┐
 │    Banco A (SQL)     │        │  Filtro/Transformação │        │   Banco B (NoSQL)    │
 │   Tabela: pedidos    │───────►│  status = 'pago'      │───────►│ analytics_pedidos    │
 └──────────────────────┘        └───────────────────────┘        └──────────────────────┘
            │                                │                               │
            └────────────────────────────────┼───────────────────────────────┘
                                             ▼
                               ┌───────────────────────────┐
                               │   data_lineage_events     │ (Tabela de Auditoria)
                               └─────────────┬─────────────┘
                                             ▼
                               ┌───────────────────────────┐
                               │      LineageService       │ (Engine NetworkX)
                               └─────────────┬─────────────┘
                                             ▼
                          GET /lineage/graph | GET /lineage/table/{tbl}
                                             ▼
                               ┌───────────────────────────┐
                               │     LineageFlowViewer     │ (Next.js + SVG)
                               └───────────────────────────┘
```

---

## 🔌 Especificação da API REST

### 1. `GET /lineage/graph`
Retorna os nós, arestas e métricas agregadas do ecossistema de linhagem.

#### Parâmetros de Consulta (Query Params):
- `connection_id` *(int, opcional)*: Filtra fluxos vinculados a uma conexão específica.
- `table_name` *(str, opcional)*: Filtra o fluxo de uma tabela de interesse.
- `search` *(str, opcional)*: Pesquisa textual em tabelas, bancos ou regras.

#### Resposta de Exemplo:
```json
{
  "nodes": [
    {
      "id": "table_1_pedidos",
      "type": "table",
      "label": "pedidos",
      "database_name": "PostgreSQL (Produção)",
      "db_type": "postgresql",
      "columns_count": 5,
      "columns": ["id", "cliente_id", "valor_total", "status", "data_criacao"],
      "level": 0
    },
    {
      "id": "tx_1",
      "type": "transformation",
      "label": "Filtro: status = 'pago' AND data...",
      "level": 1,
      "metadata": {
        "rows_transferred": 45230,
        "duration_ms": 3120,
        "status": "success",
        "query": "SELECT id, cliente_id, valor_total, status FROM pedidos WHERE status = 'pago'"
      }
    },
    {
      "id": "table_20_analytics_pedidos",
      "type": "table",
      "label": "analytics_pedidos",
      "database_name": "MongoDB (DataLake Analytics)",
      "db_type": "mongodb",
      "columns_count": 4,
      "columns": ["pedido_id", "cliente_id", "receita_liquida"],
      "level": 2
    }
  ],
  "edges": [
    {
      "id": "edge_table_1_pedidos_to_tx_1",
      "source": "table_1_pedidos",
      "target": "tx_1",
      "label": "Extração",
      "rows": 45230
    },
    {
      "id": "edge_tx_1_to_table_20_analytics_pedidos",
      "source": "tx_1",
      "target": "table_20_analytics_pedidos",
      "label": "Carga (45,230 lins)",
      "rows": 45230
    }
  ],
  "summary": {
    "total_sources": 3,
    "total_targets": 2,
    "total_transformations": 3,
    "total_volume_records": 76530,
    "databases_involved": ["MongoDB (DataLake Analytics)", "MySQL (E-Commerce)", "PostgreSQL (Produção)"],
    "active_flows_count": 3,
    "average_duration_ms": 1973.3
  }
}
```

### 2. `GET /lineage/table/{table_name}`
Realiza a análise de impacto da tabela informada:
- **Upstream**: Lista as tabelas de origem que geram os dados desta tabela.
- **Downstream**: Lista as tabelas dependentes que consomem esta tabela.
- **Risk Assessment**: `LOW`, `MEDIUM`, `HIGH` ou `CRITICAL` com explicação clara para tomada de decisão.

### 3. `GET /lineage/history`
Lista cronológica paginada de todos os eventos de fluxo e transferências registrados.

### 4. `POST /lineage/events`
Permite que scripts ou ferramentas ETL/ELT externas registrem fluxos no grafo de governança do MustaInfo.

---

## 🛡️ Garantias de Segurança & Não-Regressão

1. **Zero Quebra de Código Legado**: O serviço de linhagem roda desacoplado. Falhas na gravação da linhagem são capturadas e logadas sem interromper transferências de dados ativas.
2. **Uso de NetworkX**: Aproveita a biblioteca já instalada `networkx==3.6.1` para ordenação topológica sem dependências adicionais pesadas.
3. **Compatibilidade Total de Tipos**: Frontend construído com React 19, TypeScript estrito e Tailwind CSS, sem conflitos de dependências externas.
