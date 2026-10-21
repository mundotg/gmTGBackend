"""
Script para geração automática da apresentação executiva em PowerPoint (.pptx)
do Plano Mestre de Testes de Qualidade (50 Cenários) - MustaInfo.
Design moderno no formato 16:9 widescreen com tema dark/slate corporativo.
"""

import sys
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

def create_deck(output_path: str):
    prs = Presentation()
    # 16:9 Widescreen
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]  # Blank

    # Paleta de Cores
    COLOR_BG = RGBColor(15, 23, 42)       # Slate 900
    COLOR_CARD = RGBColor(30, 41, 59)     # Slate 800
    COLOR_CARD_BORDER = RGBColor(51, 65, 85) # Slate 700
    COLOR_CYAN = RGBColor(56, 189, 248)   # Cyan 400
    COLOR_BLUE = RGBColor(37, 99, 235)    # Blue 600
    COLOR_WHITE = RGBColor(248, 250, 252) # Slate 50
    COLOR_MUTED = RGBColor(148, 163, 184) # Slate 400
    COLOR_GREEN = RGBColor(34, 197, 94)   # Green 500
    COLOR_RED = RGBColor(239, 68, 68)     # Red 500
    COLOR_ORANGE = RGBColor(249, 115, 22) # Orange 500

    def add_header(slide, title_text, category_text="PLANO DE TESTES DE QUALIDADE — MUSTAINFO"):
        # Background
        bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
        bg.fill.solid()
        bg.fill.fore_color.rgb = COLOR_BG
        bg.line.fill.background()

        # Category / Breadcrumb
        cat_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(11.5), Inches(0.4))
        tf_cat = cat_box.text_frame
        tf_cat.word_wrap = True
        p_cat = tf_cat.paragraphs[0]
        p_cat.text = category_text.upper()
        p_cat.font.size = Pt(11)
        p_cat.font.bold = True
        p_cat.font.color.rgb = COLOR_CYAN

        # Title
        t_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.75), Inches(11.5), Inches(0.8))
        tf_t = t_box.text_frame
        tf_t.word_wrap = True
        p_t = tf_t.paragraphs[0]
        p_t.text = title_text
        p_t.font.size = Pt(24)
        p_t.font.bold = True
        p_t.font.color.rgb = COLOR_WHITE

        # Divider line
        div = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(1.5), Inches(11.733), Inches(0.02))
        div.fill.solid()
        div.fill.fore_color.rgb = COLOR_CARD_BORDER
        div.line.fill.background()

    def add_card(slide, left, top, width, height, title, items, badge_text=None, badge_color=COLOR_CYAN):
        # Card Background
        card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        card.fill.solid()
        card.fill.fore_color.rgb = COLOR_CARD
        card.line.color.rgb = COLOR_CARD_BORDER
        card.line.width = Pt(1)

        # Title Box
        tb = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.15), width - Inches(0.4), Inches(0.5))
        p = tb.text_frame.paragraphs[0]
        p.text = title
        p.font.size = Pt(15)
        p.font.bold = True
        p.font.color.rgb = COLOR_WHITE

        # Badge if present
        if badge_text:
            badge = slide.shapes.add_shape(
                MSO_SHAPE.ROUNDED_RECTANGLE,
                left + width - Inches(1.6),
                top + Inches(0.18),
                Inches(1.4),
                Inches(0.35)
            )
            badge.fill.solid()
            badge.fill.fore_color.rgb = badge_color
            badge.line.fill.background()
            bp = badge.text_frame.paragraphs[0]
            bp.alignment = PP_ALIGN.CENTER
            bp.text = badge_text
            bp.font.size = Pt(9)
            bp.font.bold = True
            bp.font.color.rgb = COLOR_WHITE

        # Items
        cb = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.7), width - Inches(0.4), height - Inches(0.8))
        tf = cb.text_frame
        tf.word_wrap = True
        for i, item in enumerate(items):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.text = f"• {item}"
            p.font.size = Pt(11)
            p.font.color.rgb = COLOR_MUTED
            p.space_after = Pt(6)

    # ==========================================
    # SLIDE 1: Capa (Title Slide)
    # ==========================================
    s1 = prs.slides.add_slide(blank_layout)
    bg1 = s1.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
    bg1.fill.solid()
    bg1.fill.fore_color.rgb = COLOR_BG
    bg1.line.fill.background()

    # Tag Badge
    badge1 = s1.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(1.2), Inches(1.5), Inches(3.2), Inches(0.45))
    badge1.fill.solid()
    badge1.fill.fore_color.rgb = COLOR_BLUE
    badge1.line.fill.background()
    p_b1 = badge1.text_frame.paragraphs[0]
    p_b1.text = "PROTOCOLO OFICIAL DE HOMOLOGAÇÃO"
    p_b1.alignment = PP_ALIGN.CENTER
    p_b1.font.size = Pt(10)
    p_b1.font.bold = True
    p_b1.font.color.rgb = COLOR_WHITE

    # Title
    t1_box = s1.shapes.add_textbox(Inches(1.2), Inches(2.2), Inches(10.8), Inches(1.8))
    tf1 = t1_box.text_frame
    tf1.word_wrap = True
    p_title = tf1.paragraphs[0]
    p_title.text = "Plano Mestre de Testes de Qualidade (QA)"
    p_title.font.size = Pt(36)
    p_title.font.bold = True
    p_title.font.color.rgb = COLOR_WHITE

    p_sub = tf1.add_paragraph()
    p_sub.text = "Guia Didático e Prático de Homologação dos 50 Cenários Críticos do MustaInfo"
    p_sub.font.size = Pt(18)
    p_sub.font.color.rgb = COLOR_CYAN
    p_sub.space_before = Pt(8)

    # Info Cards
    card_w = Inches(3.4)
    card_h = Inches(1.6)
    tops = Inches(4.5)

    add_card(s1, Inches(1.2), tops, card_w, card_h, "50 Cenários Cobertos", [
        "100% rastreáveis e reproduzíveis",
        "Cobertura de 7 módulos centrais",
        "Testes visuais (UI), API e Banco"
    ], badge_text="Versão 2.0", badge_color=COLOR_BLUE)

    add_card(s1, Inches(4.9), tops, card_w, card_h, "Governança & Risco", [
        "Dry-Run & Impact Predictor",
        "RBAC Estrito & Multi-tenant",
        "Linhagem de Dados (DAG NetworkX)"
    ], badge_text="Crítica", badge_color=COLOR_RED)

    add_card(s1, Inches(8.6), tops, card_w, card_h, "Automação & Execução", [
        "Suíte Pytest com 30+ testes",
        "Comandos cURL prontos para uso",
        "Critérios estritos de aprovação"
    ], badge_text="Automated", badge_color=COLOR_GREEN)

    # ==========================================
    # SLIDE 2: Visão Geral e Arquitetura de Testes
    # ==========================================
    s2 = prs.slides.add_slide(blank_layout)
    add_header(s2, "Visão Geral e Matriz de Cobertura dos 50 Cenários")

    add_card(s2, Inches(0.8), Inches(1.8), Inches(3.7), Inches(5.0), "Módulos de Alta Criticidade", [
        "Módulo 1: RBAC & Governança (TC-01 a TC-10)",
        "• Auto-associação e isolamento de tenants",
        "• Bloqueio de DDL destrutivo (DROP TABLE)",
        "• Proteção de endpoints administrativos",
        "",
        "Módulo 5: Dry-Run & Impact Predictor (TC-31 a TC-40)",
        "• Simulação de custo sem executar mutações",
        "• Bloqueio de UPDATE/DELETE sem WHERE",
        "• Garantia matemática de rollback total"
    ], badge_text="🔴 20 Testes Críticos", badge_color=COLOR_RED)

    add_card(s2, Inches(4.8), Inches(1.8), Inches(3.7), Inches(5.0), "Módulos de Operação e Dados", [
        "Módulo 2: Conexões Multi-Banco (TC-11 a TC-16)",
        "• Handshake PostgreSQL, MySQL e MongoDB",
        "• Timeout controlado em host inexistente",
        "",
        "Módulo 4: Editor SQL & Consultas (TC-23 a TC-30)",
        "• Paginação rápida de queries volumosas",
        "• Autocomplete inteligente de esquemas",
        "• Cancelamento seguro de queries longas",
        "",
        "Módulo 7: Linhagem de Dados (TC-45 a TC-50)",
        "• Grafo interativo DAG via NetworkX"
    ], badge_text="🟠 20 Testes Altos", badge_color=COLOR_ORANGE)

    add_card(s2, Inches(8.8), Inches(1.8), Inches(3.7), Inches(5.0), "Metodologia Didática (3 Pilares)", [
        "1. Interface Gráfica (UI):",
        "Passo a passo no navegador (telas, botões, modais e atalhos).",
        "",
        "2. Terminal & API (cURL / Swagger):",
        "Scripts prontos para copiar e colar com tokens JWT reais.",
        "",
        "3. Validação no Banco & Logs:",
        "Queries SQL para checagem do estado persistido e rastreamento de telemetria.",
        "",
        "Meta Final:",
        "100% de sucesso nos testes críticos."
    ], badge_text="Metodologia", badge_color=COLOR_BLUE)

    # ==========================================
    # SLIDE 3: Módulo 1 - RBAC & Governança (TC-01 a TC-10)
    # ==========================================
    s3 = prs.slides.add_slide(blank_layout)
    add_header(s3, "Módulo 1: RBAC, Perfil 'user' & Governança (TC-01 a TC-10)")

    add_card(s3, Inches(0.8), Inches(1.8), Inches(5.6), Inches(2.4), "TC-01 a TC-04: Registro e Blindagem de Rotas", [
        "TC-01: Auto-associação obrigatória ao papel 'user' no cadastro público (sem role_id NULL).",
        "TC-02: Renderização de menus operacionais e ocultação de configurações administrativas.",
        "TC-03: Bloqueio estrito de acesso a rotas administrativas (HTTP 403 Forbidden).",
        "TC-04: Proibição de gestão de membros da empresa por usuário comum."
    ], badge_text="Prioridade Crítica", badge_color=COLOR_RED)

    add_card(s3, Inches(6.8), Inches(1.8), Inches(5.7), Inches(2.4), "TC-05 a TC-07: Bloqueio DDL e Operações Permitidas", [
        "TC-05: Bloqueio de DDL destrutivo: DROP TABLE produtos é rejeitado por falta de 'schema:manage'.",
        "TC-06: Leitura de dados livre: SELECT * FROM produtos LIMIT 10 autorizado via 'query:execute'.",
        "TC-07: Auto-atendimento: Edição de dados cadastrais no próprio perfil permitida com HTTP 200 OK."
    ], badge_text="Prioridade Crítica/Alta", badge_color=COLOR_ORANGE)

    add_card(s3, Inches(0.8), Inches(4.5), Inches(5.6), Inches(2.3), "TC-08: Isolamento Multi-inquilino (Multi-tenant)", [
        "Teste de Invasão de Empresa A para Empresa B:",
        "• Usuário da Empresa 2 tenta requisitar GET /connections/15 pertencente à Empresa 1.",
        "• Critério: Resposta 404 Not Found ou 403 Forbidden garantindo segregação total dos catálogos."
    ], badge_text="Isolamento Total", badge_color=COLOR_RED)

    add_card(s3, Inches(6.8), Inches(4.5), Inches(5.7), Inches(2.3), "TC-09 & TC-10: Segurança de Tokens e Paginação", [
        "TC-09: Token JWT Adulterado:",
        "• Adulteração do Bearer Token no LocalStorage resulta em 401 Unauthorized imediato e redirecionamento.",
        "TC-10: Paginação Segura (entity:list):",
        "• Respeita limites estritos (10/25/50 por página) sem estouro de memória no backend."
    ], badge_text="Conformidade", badge_color=COLOR_GREEN)

    # ==========================================
    # SLIDE 4: Módulo 2 - Gestão de Conexões Multi-Banco (TC-11 a TC-16)
    # ==========================================
    s4 = prs.slides.add_slide(blank_layout)
    add_header(s4, "Módulo 2: Gestão de Conexões Multi-Banco (TC-11 a TC-16)")

    add_card(s4, Inches(0.8), Inches(1.8), Inches(5.6), Inches(2.4), "TC-11 & TC-12: Conexão PostgreSQL & Teste Pré-Salvar", [
        "TC-11: Cadastro com Encriptação AES/Fernet:",
        "• Senhas de bancos nunca são gravadas em texto plano na base de metadados.",
        "TC-12: Test Connection com Latência:",
        "• Botão 'Testar Conexão' realiza handshake TCP prévio e retorna a latência em ms (ex: 38ms)."
    ], badge_text="Handshake Seguro", badge_color=COLOR_GREEN)

    add_card(s4, Inches(6.8), Inches(1.8), Inches(5.7), Inches(2.4), "TC-13 & TC-14: Timeout e Falhas Controladas", [
        "TC-13: Timeout em Host Inexistente (RFC 5737):",
        "• Socket encerra em no máximo 5 segundos (connect_timeout=5), impedindo travamento da UI.",
        "TC-14: Credenciais Inválidas:",
        "• Retorno de mensagem limpa e diagnóstica ('Falha de autenticação: credenciais inválidas')."
    ], badge_text="Resiliência de Rede", badge_color=COLOR_ORANGE)

    add_card(s4, Inches(0.8), Inches(4.5), Inches(5.6), Inches(2.3), "TC-15: Alternância Dinâmica de Banco Ativo", [
        "Sincronização Global no Topo da Aplicação:",
        "• Alterne a conexão ativa no Dropdown do cabeçalho.",
        "• A árvore lateral de tabelas, o contexto do editor SQL e os esquemas recarregam instantaneamente."
    ], badge_text="Experiência Fluida", badge_color=COLOR_BLUE)

    add_card(s4, Inches(6.8), Inches(4.5), Inches(5.7), Inches(2.3), "TC-16: Suporte Completo a NoSQL (MongoDB)", [
        "Integração Híbrida Relacional + Documentos:",
        "• Conexão via URI mongodb://...",
        "• Driver pymongo executa ping de validação e lista as coleções no mesmo catálogo unificado."
    ], badge_text="Cross-Database", badge_color=COLOR_CYAN)

    # ==========================================
    # SLIDE 5: Módulo 3 - Tabelas, Metadados e Esquemas (TC-17 a TC-22)
    # ==========================================
    s5 = prs.slides.add_slide(blank_layout)
    add_header(s5, "Módulo 3: Estrutura de Tabelas & Metadados (TC-17 a TC-22)")

    add_card(s5, Inches(0.8), Inches(1.8), Inches(5.6), Inches(2.4), "TC-17 & TC-18: Dicionário de Dados e Chaves PK/FK", [
        "TC-17: Catálogo Completo de Objetos:",
        "• Mapeamento de todas as tabelas e views do esquema ativo com contagem estimada de linhas.",
        "TC-18: Introspecção Detalhada de Colunas:",
        "• Tipos SQL precisos (VARCHAR, NUMERIC, TIMESTAMP) e identificação visual de PK e FK."
    ], badge_text="Dicionário de Dados", badge_color=COLOR_BLUE)

    add_card(s5, Inches(6.8), Inches(1.8), Inches(5.7), Inches(2.4), "TC-19 & TC-20: Estatísticas e Busca Instantânea", [
        "TC-19: Métricas Físicas da Tabela:",
        "• Exibição de espaço ocupado em disco (MB), volumetria e índices criados.",
        "TC-20: Filtro de Busca Instantâneo:",
        "• Busca de tabelas em tempo real (< 100ms) sem necessidade de reload da página."
    ], badge_text="Performance", badge_color=COLOR_GREEN)

    add_card(s5, Inches(0.8), Inches(4.5), Inches(5.6), Inches(2.3), "TC-21: Exportação Tabular para CSV", [
        "Geração de Relatórios e Carga em BI:",
        "• Exportação de registros com codificação UTF-8 correta, delimitadores padronizados e cabeçalhos intactos.",
        "• Compatibilidade direta com Excel, Power BI e Pandas."
    ], badge_text="Exportação", badge_color=COLOR_CYAN)

    add_card(s5, Inches(6.8), Inches(4.5), Inches(5.7), Inches(2.3), "TC-22: Tratamento de Estado Vazio (Zero State)", [
        "Comportamento em Tabelas sem Registros:",
        "• Renderização limpa mantendo cabeçalhos de colunas visíveis.",
        "• Mensagem amigável ('Nenhum registro encontrado') com botões para inserção ou nova query."
    ], badge_text="Usabilidade", badge_color=COLOR_MUTED)

    # ==========================================
    # SLIDE 6: Módulo 4 - Editor SQL & Consultas (TC-23 a TC-30)
    # ==========================================
    s6 = prs.slides.add_slide(blank_layout)
    add_header(s6, "Módulo 4: Editor SQL & Consultas Analíticas (TC-23 a TC-30)")

    add_card(s6, Inches(0.8), Inches(1.8), Inches(5.6), Inches(2.4), "TC-23 & TC-24: Execução Paginada e Joins Complexos", [
        "TC-23: Paginação Virtual a 60 FPS:",
        "• Os primeiros 50 registros de tabelas com milhões de linhas carregam em < 500ms.",
        "TC-24: Consultas com Múltiplos Joins e Agrupamentos:",
        "• Cálculos de agregações e joins computados com precisão decimal sem distorção numérica."
    ], badge_text="Alta Performance", badge_color=COLOR_GREEN)

    add_card(s6, Inches(6.8), Inches(1.8), Inches(5.7), Inches(2.4), "TC-25 & TC-26: Autocomplete e Cancelamento de Query", [
        "TC-25: Sugestão Inteligente (Ctrl + Espaço):",
        "• Autocomplete contextual listando tabelas e colunas com base no schema ativo.",
        "TC-26: Cancelamento de Query Longa:",
        "• Botão 'Cancelar' envia interrupção segura (pg_cancel_backend) sem travar a interface."
    ], badge_text="Produtividade", badge_color=COLOR_CYAN)

    add_card(s6, Inches(0.8), Inches(4.5), Inches(5.6), Inches(2.3), "TC-27 & TC-28: Tratamento de Erros e Múltiplas Abas", [
        "TC-27: Feedback Diagnóstico de Erros:",
        "• Erros de sintaxe apontam a linha, coluna e trecho incorreto sem crash da página.",
        "TC-28: Gerenciamento Independente de Abas:",
        "• Múltiplas abas abertas mantêm seus códigos e resultados isolados."
    ], badge_text="Resiliência", badge_color=COLOR_ORANGE)

    add_card(s6, Inches(6.8), Inches(4.5), Inches(5.7), Inches(2.3), "TC-29 & TC-30: Exportação JSON e Rascunho Persistente", [
        "TC-29: Exportação em JSON Estruturado:",
        "• Download de array de objetos para integração direta em pipelines.",
        "TC-30: Rascunho Salvo no LocalStorage:",
        "• Ao pressionar F5 ou fechar o browser, o código SQL digitado é recuperado automaticamente."
    ], badge_text="Segurança de Dados", badge_color=COLOR_BLUE)

    # ==========================================
    # SLIDE 7: Módulo 5 - Dry-Run & Impact Predictor (TC-31 a TC-40)
    # ==========================================
    s7 = prs.slides.add_slide(blank_layout)
    add_header(s7, "Módulo 5: Dry-Run & Simulador de Impacto (TC-31 a TC-40)")

    add_card(s7, Inches(0.8), Inches(1.8), Inches(5.6), Inches(2.4), "TC-31 a TC-33: Classificação de Risco e Prevenção", [
        "TC-31: UPDATE com WHERE Indexado (Risco LOW 🟢):",
        "• Detecta 1 linha afetada e executa sem interrupção.",
        "TC-32: UPDATE SEM WHERE (Risco CRITICAL 🔴):",
        "• Alerta imediato de que todas as linhas serão modificadas e trava a execução acidental.",
        "TC-33: DELETE sem Filtro:",
        "• Exige digitação explícita da palavra 'CONFIRMAR' para liberar o disparo."
    ], badge_text="Impact Guard Ativo", badge_color=COLOR_RED)

    add_card(s7, Inches(6.8), Inches(1.8), Inches(5.7), Inches(2.4), "TC-34 a TC-36: Locks, Rollback e Análise EXPLAIN", [
        "TC-34: Alerta de Bloqueio Exclusivo (Table Lock):",
        "• Identifica DDL pesada que paralisaria leituras e escritas concorrentes.",
        "TC-35: Garantia Matemática de Rollback no Dry-Run:",
        "• O simulador NUNCA comita dados físicos na base.",
        "TC-36: Detecção de Sequential Scan e Sugestão de Índices:",
        "• Gera comando CREATE INDEX pronto para cópia com 1 clique."
    ], badge_text="Auditoria Otimizada", badge_color=COLOR_ORANGE)

    add_card(s7, Inches(0.8), Inches(4.5), Inches(5.6), Inches(2.3), "TC-37: Dry-Run em Bancos NoSQL (MongoDB)", [
        "Avaliação Preventiva de Consultas em Coleções:",
        "• Utiliza explain('queryPlanner') nativo do MongoDB.",
        "• Identifica estágio COLLSCAN e avisa sobre escaneamento de coleções inteiras sem índice."
    ], badge_text="NoSQL Support", badge_color=COLOR_CYAN)

    add_card(s7, Inches(6.8), Inches(4.5), Inches(5.7), Inches(2.3), "TC-38 a TC-40: Componente Visual e Resiliência", [
        "TC-38: ImpactPredictorCard no Frontend:",
        "• Métricas visuais claras: Linhas estimadas, custo e recomendações.",
        "TC-39: Modal de Confirmação para Alto Risco.",
        "TC-40: Tolerância a queries malformadas sem erro 500."
    ], badge_text="Design Robusto", badge_color=COLOR_GREEN)

    # ==========================================
    # SLIDE 8: Módulo 6 & 7 - Histórico & Linhagem de Dados
    # ==========================================
    s8 = prs.slides.add_slide(blank_layout)
    add_header(s8, "Módulos 6 e 7: Auditoria & Linhagem de Dados (TC-41 a TC-50)")

    add_card(s8, Inches(0.8), Inches(1.8), Inches(5.6), Inches(5.0), "Módulo 6: Histórico & Auditoria (TC-41 a TC-44)", [
        "TC-41: Auditoria Automática de Sucesso:",
        "• Toda query executada é gravada com timestamp, usuário e tempo de resposta.",
        "",
        "TC-42: Registro Completo de Falhas:",
        "• Queries que falharam são salvas com badge 'FAILED' e mensagem de erro técnica do banco.",
        "",
        "TC-43: Reutilização Rápida no Editor:",
        "• 1 clique para transferir o código do histórico de volta ao editor SQL.",
        "",
        "TC-44: Exportação de Auditoria:",
        "• Extrato completo para auditorias de compliance e SOC2."
    ], badge_text="Compliance & Trilha", badge_color=COLOR_BLUE)

    add_card(s8, Inches(6.8), Inches(1.8), Inches(5.7), Inches(5.0), "Módulo 7: Linhagem Visual de Dados (TC-45 a TC-50)", [
        "TC-45: Grafo DAG via NetworkX:",
        "• Mapeamento topológico limpo: Origens ➔ Transformações ➔ Destinos.",
        "",
        "TC-46: Navegação Suave (Zoom & Pan):",
        "• Canvas interativo a 60 FPS adaptado a grandes volumes de entidades.",
        "",
        "TC-47 & TC-48: Painel Lateral & Fluxos Multi-Banco:",
        "• Rastreamento da proveniência: PostgreSQL (pedidos) ➔ MongoDB (analytics_pedidos).",
        "",
        "TC-49 & TC-50: Análise Upstream/Downstream:",
        "• Destaque visual das tabelas impactadas antes de modificações estruturais."
    ], badge_text="Governança DAG", badge_color=COLOR_CYAN)

    # ==========================================
    # SLIDE 9: Guia Rápido de Execução & Troubleshooting
    # ==========================================
    s9 = prs.slides.add_slide(blank_layout)
    add_header(s9, "Roteiro Prático de Execução & Resolução de Problemas")

    add_card(s9, Inches(0.8), Inches(1.8), Inches(5.6), Inches(5.0), "Comandos de Inicialização Rápida", [
        "1. Backend FastAPI:",
        "cd gmTGBackend",
        ".\\venv\\Scripts\\activate",
        "uvicorn app.main:app --reload --port 8000",
        "",
        "2. Frontend Next.js:",
        "cd gmTGFrontEnd",
        "npm run dev (porta 3000)",
        "",
        "3. Obtenção de Token de Testes (cURL):",
        "curl -X POST http://localhost:8000/auth/login \\",
        "  -H 'Content-Type: application/json' \\",
        "  -d '{\"email\":\"teste_user_qa@exemplo.com\", \"password\":\"Senha123!\"}'",
        "",
        "4. Rodar Testes Automatizados:",
        "pytest gmTGBackend/tests/test_rotas_protegidas.py"
    ], badge_text="Passo a Passo", badge_color=COLOR_GREEN)

    add_card(s9, Inches(6.8), Inches(1.8), Inches(5.7), Inches(5.0), "Matriz Rápida de Troubleshooting", [
        "Erro 401 Unauthorized:",
        "• Causa: Token expirou ou chave SECRET_KEY foi alterada.",
        "• Solução: Faça novo login e renove a variável $USER_TOKEN.",
        "",
        "Erro 403 Forbidden em rota comum:",
        "• Causa: O usuário perdeu permissões básicas da role 'user'.",
        "• Solução: Execute python app/seed_new.py para restaurar.",
        "",
        "Timeout de 5 segundos no Test Connection:",
        "• Causa: Banco local não está rodando ou IP/Porta incorretos.",
        "• Solução: Verifique docker ps e libere a porta 5432/3306/27017.",
        "",
        "Grafo de Linhagem Vazio:",
        "• Causa: Nenhuma transferência foi executada ainda.",
        "• Solução: Realize uma migração em /home/transacoes."
    ], badge_text="Troubleshooting", badge_color=COLOR_ORANGE)

    # ==========================================
    # SLIDE 10: Critérios de Homologação e Aprovação Final
    # ==========================================
    s10 = prs.slides.add_slide(blank_layout)
    add_header(s10, "Critérios Estritos de Homologação & Assinatura de Qualidade")

    add_card(s10, Inches(0.8), Inches(1.8), Inches(5.6), Inches(5.0), "Metas Numéricas de Aprovação (SLA)", [
        "🔴 Cenários Críticos (16 testes):",
        "• Meta: 100% de Aprovação sem exceção.",
        "• Impacto: Tolerância zero a falhas em RBAC e Dry-Run.",
        "",
        "🟠 Cenários de Prioridade Alta (20 testes):",
        "• Meta: Mínimo 95% de Aprovação (máx. 1 falha tolerada para correção imediata).",
        "",
        "🟡 Cenários de Prioridade Média (14 testes):",
        "• Meta: Mínimo 90% de Aprovação.",
        "",
        "Automação Contínua (CI/CD):",
        "• 30/30 testes em test_rotas_protegidas.py passando.",
        "• npx tsc --noEmit com zero erros de compilação."
    ], badge_text="SLA de Qualidade", badge_color=COLOR_RED)

    add_card(s10, Inches(6.8), Inches(1.8), Inches(5.7), Inches(5.0), "Termo de Homologação da Release", [
        "Status da Release MustaInfo:",
        "[ X ] Suíte de 50 Cenários Homologada",
        "[ X ] Dry-Run & Impact Predictor Validado",
        "[ X ] Linhagem de Dados e Telemetria Operacionais",
        "[ X ] Blindagem Multi-tenant Aprovada",
        "",
        "Assinaturas Responsáveis:",
        "• Engenheiro Líder de QA: Francemy Sebastião",
        "• Arquiteto de Software: _____________________",
        "• Gerente de Operações / DBA: ________________",
        "",
        "Decisão Final do Comitê:",
        "[ X ] APROVADO PARA PRODUÇÃO (GO-LIVE)"
    ], badge_text="Aprovação Final", badge_color=COLOR_GREEN)

    # Salva apresentação
    prs.save(output_path)
    print(f"Apresentação salva com sucesso em: {output_path}")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/PLANO_DE_TESTES_50_CENARIOS.pptx"
    create_deck(out)
