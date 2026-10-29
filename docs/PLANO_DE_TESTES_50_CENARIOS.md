# 📋 Plano Mestre de Testes de Qualidade (QA) - MustaInfoo
**Versão:** 1.0.0  
**Ambiente:** Staging / Local Dev  
**Data:** 18 de Outubro de 2026  
**Módulos Cobertos:** RBAC & Perfil `user`, Dry-Run & Impact Predictor, Editor SQL, Conexões, Tabelas & Esquemas, Histórico & Auditoria, Data Lineage & Flow.

---

## 📌 1. Visão Geral e Matriz de Cobertura

Este documento estabelece o protocolo oficial de homologação e validação da plataforma **MustaInfoo**. Todos os 50 cenários possuem passos reproduzíveis, pré-condições claras e critérios estritos de aprovação/rejeição.

| ID Intervalo | Módulo Funcional | Total de Testes | Prioridade Dominante |
|---|---|---|---|
| **TC-01 a TC-10** | RBAC, Perfil `user` e Governança de Acessos | 10 | 🔴 Crítica |
| **TC-11 a TC-16** | Gestão e Testes de Conexões Multi-Banco | 6 | 🟠 Alta |
| **TC-17 a TC-22** | Estrutura de Tabelas, Metadados e Esquemas | 6 | 🟡 Média |
| **TC-23 a TC-30** | Editor SQL & Motor de Consultas | 8 | 🟠 Alta |
| **TC-31 a TC-40** | Simulador de Impacto & Dry-Run (Impact Predictor) | 10 | 🔴 Crítica |
| **TC-41 a TC-44** | Histórico de Consultas & Auditoria | 4 | 🟡 Média |
| **TC-45 a TC-50** | Linhagem Visual de Dados (Data Lineage & Flow) | 6 | 🟠 Alta |

---

## 🛡️ Módulo 1: RBAC, Perfil `user` & Governança (TC-01 a TC-10)

### [TC-01] Auto-associação ao Perfil `user` no Registo Público
- **Prioridade:** 🔴 Crítica
- **Pré-condição:** Estar na tela pública de registo (`/register`).
- **Passo a Passo:**
  1. Acessar `/register`.
  2. Preencher formulário com nome, e-mail inédito (`teste_user_qa@exemplo.com`) e senha segura.
  3. Clicar em "Criar Conta".
- **Condição Aceitável (Critério de Sucesso):**
  - Conta criada com sucesso (HTTP `201 Created`).
  - No banco de dados, o campo `users.role_id` aponta para o ID da role global `user` (nunca `NULL`).
  - O usuário faz login e já recebe as 18 permissões básicas de consulta e visualização.

### [TC-02] Acesso aos Menus do Sidebar para o Perfil `user`
- **Prioridade:** 🟠 Alta
- **Pré-condição:** Usuário logado com perfil padrão `user`.
- **Passo a Passo:**
  1. Efetuar login e observar o menu lateral (Sidebar).
  2. Verificar a presença dos links: Conexões, Tabelas, Editor SQL, Histórico, Análise de BD e Linhagem.
- **Condição Aceitável:**
  - Todos os itens de operação diária estão visíveis e clicáveis sem tela branca ou redirecionamento forçado para `/login`.
  - Menus restritos de administração de equipe/empresa permanecem ocultos.

### [TC-03] Bloqueio de Acesso a Rotas Administrativas de Empresa
- **Prioridade:** 🔴 Crítica
- **Pré-condição:** Token de autenticação do perfil `user`.
- **Passo a Passo:**
  1. Tentar acessar manualmente no navegador `/admin/empresa` ou `/home/settings/team`.
  2. Enviar requisição `PUT /empresas/atualizar-plano` via cliente HTTP.
- **Condição Aceitável:**
  - Acesso negado com HTTP `403 Forbidden` ou redirecionamento visual seguro com alerta `"Permissão insuficiente"`.

### [TC-04] Bloqueio de Gestão de Membros por Usuário Comum
- **Prioridade:** 🔴 Crítica
- **Pré-condição:** Token do perfil `user`.
- **Passo a Passo:**
  1. Enviar requisição `POST /users/members` tentando convidar um novo colaborador.
  2. Enviar `DELETE /users/{id}` tentando excluir outro usuário.
- **Condição Aceitável:**
  - Ambas as requisições respondem `403 Forbidden` com `{ "detail": "Não autorizado" }`. Nenhuma alteração é persistida.

### [TC-05] Bloqueio de DDL Destrutivo em Massa (`DROP TABLE`)
- **Prioridade:** 🔴 Crítica
- **Pré-condição:** Editor SQL aberto com perfil `user`.
- **Passo a Passo:**
  1. Digitar `DROP TABLE produtos;` no editor.
  2. Clicar em "Executar".
- **Condição Aceitável:**
  - O sistema valida a ausência da permissão `schema:manage` e bloqueia a execução.
  - A tabela continua intacta no banco de dados.

### [TC-06] Execução Normal de Leitura de Dados (`SELECT`)
- **Prioridade:** 🟠 Alta
- **Pré-condição:** Banco de dados ativo selecionado.
- **Passo a Passo:**
  1. Digitar `SELECT * FROM produtos LIMIT 20;` no editor.
  2. Executar consulta (`Ctrl+Enter` ou botão "Executar").
- **Condição Aceitável:**
  - Retorna os dados em grid paginado sem nenhum erro de autorização. Status HTTP `200 OK`.

### [TC-07] Edição de Dados Pessoais do Perfil
- **Prioridade:** 🟡 Média
- **Pré-condição:** Usuário logado em `/perfil`.
- **Passo a Passo:**
  1. Acessar `/perfil`.
  2. Alterar o nome para `"Novo Nome QA"` e salvar.
- **Condição Aceitável:**
  - Resposta HTTP `200 OK`. O novo nome é refletido imediatamente no avatar e header da aplicação.

### [TC-08] Isolamento Multi-inquilino de Conexões
- **Prioridade:** 🔴 Crítica
- **Pré-condição:** Dois usuários pertencentes a empresas distintas (Empresa 1 e Empresa 2).
- **Passo a Passo:**
  1. Logar com usuário da Empresa 1 e anotar o ID de uma conexão exclusiva (`id = 15`).
  2. Logar com usuário da Empresa 2 e requisitar `GET /consu/conexoes/15`.
- **Condição Aceitável:**
  - Retorno HTTP `404 Not Found` ou `403 Forbidden`. O usuário da Empresa 2 nunca tem visibilidade das credenciais ou tabelas da Empresa 1.

### [TC-09] Validação de Token JWT Expirado ou Inválido
- **Prioridade:** 🟠 Alta
- **Pré-condição:** Ter um token expirado ou alterar propositadamente caracteres do token no LocalStorage.
- **Passo a Passo:**
  1. Adulterar o Bearer token no LocalStorage/Cookies.
  2. Recarregar a página `/home/tabelas`.
- **Condição Aceitável:**
  - A requisição falha com `401 Unauthorized`.
  - A aplicação limpa o estado de autenticação e redireciona para a tela de login.

### [TC-10] Listagem Paginada de Registros (`entity:list`)
- **Prioridade:** 🟡 Média
- **Pré-condição:** Tabela com mais de 50 registros cadastrados.
- **Passo a Passo:**
  1. Abrir a listagem de registros de uma tabela.
  2. Alternar entre as páginas 1, 2 e 3 e alterar o limite para 25 por página.
- **Condição Aceitável:**
  - Paginação fluida, sem erros de query no backend, apresentando contador total correto.

---

## 🔌 Módulo 2: Gestão e Testes de Conexões (TC-11 a TC-16)

### [TC-11] Cadastro de Conexão PostgreSQL Válida
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Ir para Conexões ➔ "Nova Conexão".
  2. Preencher Host, Porta (5432), Usuário, Senha e Nome do Banco de testes.
  3. Clicar em "Salvar".
- **Condição Aceitável:**
  - Conexão salva com sucesso. Senha armazenada de forma encriptada. Conexão aparece na lista.

### [TC-12] Teste de Conexão Bem-Sucedido (`Test Connection`)
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. No modal de conexão, preencher dados válidos e clicar em "Testar Conexão".
- **Condição Aceitável:**
  - Toast de sucesso verde: `"Conexão estabelecida com sucesso"` com tempo de latência em milissegundos.

### [TC-13] Teste de Conexão com Host Inexistente (Timeout)
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Preencher host inalcançável (ex: `192.0.2.1`) e porta 5432.
  2. Clicar em "Testar Conexão".
- **Condição Aceitável:**
  - O sistema encerra o socket em no máximo 5 segundos e exibe mensagem amigável sem congelar a UI.

### [TC-14] Teste de Conexão com Credenciais Inválidas
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Preencher host e porta válidos, mas senha errada.
  2. Clicar em "Testar Conexão".
- **Condição Aceitável:**
  - Retorno informativo: `"Falha de autenticação: credenciais inválidas"`.

### [TC-15] Alternância Ativa de Banco de Dados no Seletor Global
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. No topo da interface, mudar o banco ativo no dropdown de conexões.
- **Condição Aceitável:**
  - A árvore lateral de tabelas e o contexto do editor SQL atualizam instantaneamente para o banco selecionado.

### [TC-16] Conexão com Banco NoSQL (MongoDB)
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Cadastrar conexão do tipo MongoDB informando URI `mongodb://...`.
  2. Testar e salvar.
- **Condição Aceitável:**
  - O driver MongoDB conecta com sucesso e expõe as coleções disponíveis.

---

## 📊 Módulo 3: Tabelas, Metadados e Esquemas (TC-17 a TC-22)

### [TC-17] Listagem Completa de Tabelas e Views
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Selecionar conexão ativa e navegar até `/home/tabelas`.
- **Condição Aceitável:**
  - Apresenta todas as tabelas do schema configurado com seus respectivos ícones e volumetria.

### [TC-18] Inspeção de Estrutura de Colunas e Chaves (PK/FK)
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Clicar sobre uma tabela e abrir a aba "Estrutura".
- **Condição Aceitável:**
  - Exibe nome das colunas, tipo de dado SQL exato, indicação de `NULLABLE` e badges de Chave Primária e Chave Estrangeira.

### [TC-19] Visualização de Estatísticas da Tabela
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Abrir a aba "Estatísticas" da tabela selecionada.
- **Condição Aceitável:**
  - Mostra contagem aproximada de linhas, espaço ocupado em disco e lista de índices existentes.

### [TC-20] Busca Rápida de Tabelas por Nome
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. No campo de pesquisa do menu lateral de tabelas, digitar parte do nome da tabela.
- **Condição Aceitável:**
  - Filtro em tempo real (< 100ms) sem recarregar a página.

### [TC-21] Exportação de Dados para CSV
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Na visualização de dados de uma tabela com registros, clicar em "Exportar CSV".
- **Condição Aceitável:**
  - Arquivo `.csv` baixado no navegador, codificado em UTF-8 com todos os registros e colunas correspondentes.

### [TC-22] Comportamento com Tabela Vazia
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Selecionar uma tabela sem registros cadastrados.
- **Condição Aceitável:**
  - Exibe estado visual amigável: `"Nenhum registro encontrado"`, mantendo os cabeçalhos das colunas visíveis.

---

## 💻 Módulo 4: Editor SQL & Consultas (TC-23 a TC-30)

### [TC-23] Execução de Consulta com Retorno Paginado
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. No editor SQL, executar `SELECT * FROM vendas;` em tabela volumosa.
- **Condição Aceitável:**
  - Os primeiros 50 registros são renderizados de forma rápida. O grid permite scroll e paginação.

### [TC-24] Consulta com Múltiplos `JOIN` e Agrupamento
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Executar query com 2 `JOIN`s, `GROUP BY` e `ORDER BY`.
- **Condição Aceitável:**
  - Resultados computados corretamente sem distorção nos tipos numéricos e de data.

### [TC-25] Sugestão Inteligente (Autocomplete)
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. No editor, digitar nome de uma tabela seguido de ponto (ex: `clientes.`).
- **Condição Aceitável:**
  - Abre popup com a lista das colunas pertencentes àquela tabela.

### [TC-26] Interrupção de Consulta Longa / Cancelamento
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Rodar `SELECT pg_sleep(8);` e clicar no botão "Cancelar".
- **Condição Aceitável:**
  - O backend cancela o processo no banco e a UI volta ao estado pronto imediatamente.

### [TC-27] Detecção e Tratamento de Erros de Sintaxe
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Executar query com erro intencional: `SELECT * FROOOM clientes;`.
- **Condição Aceitável:**
  - Card de erro vermelho com a linha, coluna e descrição do erro sem causar crash na interface.

### [TC-28] Navegação e Gerenciamento de Múltiplas Abas
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Criar 3 abas no editor SQL com queries distintas.
  2. Alternar entre as abas e executar queries independentes.
- **Condição Aceitável:**
  - O código e o resultado de cada aba são mantidos de forma independente.

### [TC-29] Exportação de Resultados da Query em JSON
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Executar qualquer `SELECT` com retorno de dados.
  2. Clicar em "Exportar ➔ JSON".
- **Condição Aceitável:**
  - Baixa arquivo JSON sintaticamente válido contendo o array dos objetos retornados.

### [TC-30] Persistência de Rascunho de Query após Recarregar
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Digitar uma consulta complexa no editor.
  2. Recarregar a página (`F5`).
- **Condição Aceitável:**
  - O rascunho da query permanece no editor sem perda de dados.

---

## ⚡ Módulo 5: Simulador de Impacto & Dry-Run (TC-31 a TC-40)

### [TC-31] Simulação de `UPDATE` com `WHERE` Indexado (Baixo Risco)
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Digitar `UPDATE clientes SET telefone = '999999999' WHERE id = 10;`.
  2. Clicar no botão "Simular Impacto (Dry-Run)".
- **Condição Aceitável:**
  - O `ImpactPredictorCard` exibe **Badge Verde (Baixo Risco)**, indicando `1 linha afetada`, sem lock destrutivo.

### [TC-32] Simulação de `UPDATE` SEM `WHERE` (Risco Crítico)
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Digitar `UPDATE clientes SET status = 'inativo';` (sem cláusula `WHERE`).
  2. Clicar em "Simular Impacto (Dry-Run)".
- **Condição Aceitável:**
  - Card exibe **Badge Vermelho / CRÍTICO**, avisando que **todas as linhas da tabela serão alteradas**, bloqueando execução acidental.

### [TC-33] Simulação de `DELETE` sem Filtro (Risco Extremo)
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Digitar `DELETE FROM pedidos;` e simular.
- **Condição Aceitável:**
  - Alerta sonoro/visual de impacto catastrófico com total de linhas a serem excluídas e aviso de perda irreversível.

### [TC-34] Detecção de Bloqueio de Tabela (Exclusive Lock)
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Simular uma operação pesada de alteração em tabela com alto volume.
- **Condição Aceitável:**
  - O preditor sinaliza risco de bloqueio de escrita/leitura concorrente no banco de produção.

### [TC-35] Garantia de Rollback Automático no Dry-Run
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Contar registros da tabela (`SELECT COUNT(*) FROM produtos`).
  2. Executar Dry-Run de `INSERT INTO produtos (nome) VALUES ('Item Fantasma');`.
  3. Contar registros novamente após a simulação.
- **Condição Aceitável:**
  - O número de linhas é exatamente o mesmo antes e depois. O dry-run **nunca** comita dados.

### [TC-36] Análise de Custo via Plano de Execução (`EXPLAIN`)
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Simular query com filtros sobre colunas sem índice.
- **Condição Aceitável:**
  - O preditor identifica `Sequential Scan` e sugere índice para reduzir tempo de resposta.

### [TC-37] Dry-Run em Bancos NoSQL (MongoDB)
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Simular operação `db.pedidos.deleteMany({})` em conexão MongoDB.
- **Condição Aceitável:**
  - O preditor detecta filtro vazio e categoriza a operação como risco crítico de exclusão em massa.

### [TC-38] Renderização do Componente `ImpactPredictorCard`
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Abrir o card de previsão de impacto no frontend.
- **Condição Aceitável:**
  - Renderiza métricas de pontuação de impacto (0 a 100), linhas estimadas, tempo e recomendações com layout sem falhas visuais.

### [TC-39] Modal de Confirmação para Queries de Alto Risco
- **Prioridade:** 🔴 Crítica
- **Passo a Passo:**
  1. Após o Dry-Run classificar uma query como risco Alto/Crítico, tentar clicar em "Executar".
- **Condição Aceitável:**
  - Abre modal de proteção solicitando que o usuário digite explicitamente `CONFIRMAR` antes de liberar o botão de disparo.

### [TC-40] Resiliência a Queries Malformadas no Dry-Run
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Clicar em "Simular Impacto" com query incompleta (ex: `UPDATE clientes SET`).
- **Condição Aceitável:**
  - Retorna mensagem amigável de erro de sintaxe, sem crash da aplicação ou erro 500 não tratado.

---

## 🕒 Módulo 6: Histórico de Consultas & Auditoria (TC-41 a TC-44)

### [TC-41] Registro Automático no Histórico
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Executar 2 consultas no editor SQL e abrir `/home/historico`.
- **Condição Aceitável:**
  - Ambas as consultas constam registradas com query, banco, data/hora e tempo de execução.

### [TC-42] Registro de Falhas no Histórico
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Executar query com erro e consultar o histórico.
- **Condição Aceitável:**
  - Registro exibe badge de falha e armazena o log do erro retornado pelo banco para auditoria.

### [TC-43] Reutilização de Query do Histórico para o Editor
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. No histórico, clicar no botão de copiar/abrir no editor ao lado de uma query.
- **Condição Aceitável:**
  - Redireciona para o Editor SQL com a query pronta para reexecução.

### [TC-44] Exportação do Histórico de Consultas
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Na tela de histórico, clicar em "Exportar Histórico".
- **Condição Aceitável:**
  - Arquivo de auditoria gerado com sucesso para o usuário com perfil `user`.

---

## 🗺️ Módulo 7: Linhagem de Dados (Data Lineage & Flow) (TC-45 a TC-50)

### [TC-45] Renderização do Grafo de Linhagem
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Acessar `/home/lineage`.
- **Condição Aceitável:**
  - Nós (Bancos, Tabelas, Filtros/Transformações e Destinos) renderizados com arestas direcionadas sem nós sobrepostos.

### [TC-46] Interação de Zoom e Panorâmica (Pan)
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Usar a roda do mouse e arrastar o canvas da linhagem.
- **Condição Aceitável:**
  - Movimentação suave a 60 FPS com zoom in/out sem perda de definição dos nós.

### [TC-47] Painel Lateral de Inspeção de Nó
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Clicar em um nó de tabela no grafo.
- **Condição Aceitável:**
  - Abre gaveta/painel lateral com detalhes de colunas, última atualização, banco pertencente e operações vinculadas.

### [TC-48] Rastreamento de Fluxo Multi-Banco
- **Prioridade:** 🟠 Alta
- **Passo a Passo:**
  1. Executar uma transferência de dados da tabela `pedidos` (Banco A) para `analytics_pedidos` (Banco B).
  2. Abrir o mapa de linhagem.
- **Condição Aceitável:**
  - O grafo mostra o fluxo conectado ligando o Banco A ao Banco B através do nó de transferência.

### [TC-49] Destaque de Caminho de Dependência
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Selecionar uma tabela intermediária no grafo.
- **Condição Aceitável:**
  - O sistema destaca visualmente os nós de origem (upstream) e destino (downstream), atenuando os nós não relacionados.

### [TC-50] Responsividade e Teste de Carga do Grafo
- **Prioridade:** 🟡 Média
- **Passo a Passo:**
  1. Carregar grafo com mais de 30 nós e redimensionar a janela do navegador.
- **Condição Aceitável:**
  - O grafo recalcula o viewport sem travar o navegador ou vazar memória.

---

## 📋 Checklist de Homologação Rápida

| Status | Critério |
|:---:|---|
| [ ] | 100% dos testes de prioridade 🔴 Crítica aprovados |
| [ ] | Mínimo de 90% dos testes 🟠 Alta aprovados |
| [ ] | Execução da suíte automatizada (`pytest tests/test_rotas_protegidas.py`) com 30/30 passed |
| [ ] | Execução do typecheck frontend (`npx tsc --noEmit`) com 0 erros |
