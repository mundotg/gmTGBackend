import os
import sys
import json
import random
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values, Json

# Adiciona o caminho raiz do backend
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import dotenv
dotenv.load_dotenv(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env")))

from app.database import SessionLocal
from app.models.connection_models import DBConnection, DBConnectionShare
from app.models.user_model import User
from app.services.crypto_utils import secret_encrypt
from app.services.database_inspector import sync_connection_statistics
from app.services.field_info import sincronizar_metadados_da_tabela

DB_CONFIG = {
    "dbname": "vendas_db",
    "user": "postgres",
    "password": "1234",
    "host": "127.0.0.1",
    "port": 5432,
    "sslmode": "disable"
}

DDL_SCRIPT = """
-- Extensões úteis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Limpeza caso já exista
DROP TABLE IF EXISTS movimentacoes_estoque CASCADE;
DROP TABLE IF EXISTS avaliacoes_produtos CASCADE;
DROP TABLE IF EXISTS pagamentos CASCADE;
DROP TABLE IF EXISTS itens_venda CASCADE;
DROP TABLE IF EXISTS vendas CASCADE;
DROP TABLE IF EXISTS cupons CASCADE;
DROP TABLE IF EXISTS clientes CASCADE;
DROP TABLE IF EXISTS vendedores CASCADE;
DROP TABLE IF EXISTS lojas CASCADE;
DROP TABLE IF EXISTS produtos CASCADE;
DROP TABLE IF EXISTS fornecedores CASCADE;
DROP TABLE IF EXISTS categorias CASCADE;

-- 1. Categorias (com autorreferência hierárquica)
CREATE TABLE categorias (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE,
    descricao TEXT,
    categoria_pai_id INT REFERENCES categorias(id) ON DELETE SET NULL,
    icone VARCHAR(50) DEFAULT 'tag',
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Fornecedores
CREATE TABLE fornecedores (
    id SERIAL PRIMARY KEY,
    nome_empresa VARCHAR(150) NOT NULL,
    contato_nome VARCHAR(100),
    email VARCHAR(120) NOT NULL UNIQUE,
    telefone VARCHAR(30),
    nif_cnpj VARCHAR(30) UNIQUE,
    cidade VARCHAR(80),
    pais VARCHAR(60) NOT NULL DEFAULT 'Angola',
    avaliacao NUMERIC(3, 2) DEFAULT 4.50 CHECK (avaliacao >= 0 AND avaliacao <= 5),
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Produtos
CREATE TABLE produtos (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    nome VARCHAR(150) NOT NULL,
    descricao TEXT,
    categoria_id INT NOT NULL REFERENCES categorias(id) ON DELETE RESTRICT,
    fornecedor_id INT NOT NULL REFERENCES fornecedores(id) ON DELETE RESTRICT,
    preco_unitario NUMERIC(10, 2) NOT NULL CHECK (preco_unitario >= 0),
    preco_custo NUMERIC(10, 2) NOT NULL CHECK (preco_custo >= 0),
    estoque_atual INT NOT NULL DEFAULT 0 CHECK (estoque_atual >= 0),
    estoque_minimo INT NOT NULL DEFAULT 5 CHECK (estoque_minimo >= 0),
    unidade_medida VARCHAR(20) NOT NULL DEFAULT 'UNIDADE',
    status VARCHAR(30) NOT NULL DEFAULT 'ativo' CHECK (status IN ('ativo', 'sem_estoque', 'descontinuado')),
    especificacoes JSONB DEFAULT '{}'::jsonb,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Lojas / Filiais
CREATE TABLE lojas (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(20) NOT NULL UNIQUE,
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(30) NOT NULL DEFAULT 'fisica' CHECK (tipo IN ('fisica', 'online', 'quiosque')),
    cidade VARCHAR(80) NOT NULL,
    endereco VARCHAR(200),
    telefone VARCHAR(30),
    ativa BOOLEAN NOT NULL DEFAULT TRUE,
    criada_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Vendedores (com autorreferência para gerência)
CREATE TABLE vendedores (
    id SERIAL PRIMARY KEY,
    codigo_cracha VARCHAR(30) NOT NULL UNIQUE,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(120) NOT NULL UNIQUE,
    telefone VARCHAR(30),
    loja_id INT NOT NULL REFERENCES lojas(id) ON DELETE RESTRICT,
    gerente_id INT REFERENCES vendedores(id) ON DELETE SET NULL,
    comissao_percentual NUMERIC(4, 2) NOT NULL DEFAULT 3.00,
    meta_mensal NUMERIC(12, 2) NOT NULL DEFAULT 500000.00,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    data_admissao DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 6. Clientes
CREATE TABLE clientes (
    id SERIAL PRIMARY KEY,
    codigo_cliente UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    nome_completo VARCHAR(120) NOT NULL,
    email VARCHAR(120) NOT NULL UNIQUE,
    telefone VARCHAR(30),
    nif VARCHAR(30),
    tipo_cliente VARCHAR(30) NOT NULL DEFAULT 'individual' CHECK (tipo_cliente IN ('individual', 'corporativo')),
    segmento VARCHAR(30) NOT NULL DEFAULT 'bronze' CHECK (segmento IN ('bronze', 'prata', 'ouro', 'vip')),
    cidade VARCHAR(80),
    pais VARCHAR(60) NOT NULL DEFAULT 'Angola',
    limite_credito NUMERIC(12, 2) NOT NULL DEFAULT 200000.00,
    total_compras NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. Cupons de Desconto
CREATE TABLE cupons (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(30) NOT NULL UNIQUE,
    descricao VARCHAR(150),
    tipo_desconto VARCHAR(20) NOT NULL DEFAULT 'percentual' CHECK (tipo_desconto IN ('percentual', 'valor_fixo')),
    valor_desconto NUMERIC(10, 2) NOT NULL,
    valor_minimo_pedido NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    valido_de TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valido_ate TIMESTAMPTZ NOT NULL,
    limite_usos INT NOT NULL DEFAULT 100,
    usos_atuais INT NOT NULL DEFAULT 0,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);

-- 8. Vendas (Pedidos de Venda)
CREATE TABLE vendas (
    id SERIAL PRIMARY KEY,
    codigo_venda VARCHAR(40) NOT NULL UNIQUE,
    uuid_rastreio UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    cliente_id INT NOT NULL REFERENCES clientes(id) ON DELETE RESTRICT,
    vendedor_id INT NOT NULL REFERENCES vendedores(id) ON DELETE RESTRICT,
    loja_id INT NOT NULL REFERENCES lojas(id) ON DELETE RESTRICT,
    cupom_id INT REFERENCES cupons(id) ON DELETE SET NULL,
    data_venda TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_venda VARCHAR(30) NOT NULL DEFAULT 'pendente' CHECK (status_venda IN ('pendente', 'pago', 'em_separacao', 'enviado', 'entregue', 'cancelado')),
    valor_subtotal NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    valor_desconto NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    valor_frete NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    valor_total NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    forma_pagamento_predileta VARCHAR(40) NOT NULL DEFAULT 'multicaixa_express',
    observacoes TEXT,
    dados_entrega JSONB DEFAULT '{}'::jsonb
);

-- 9. Itens da Venda
CREATE TABLE itens_venda (
    id SERIAL PRIMARY KEY,
    venda_id INT NOT NULL REFERENCES vendas(id) ON DELETE CASCADE,
    produto_id INT NOT NULL REFERENCES produtos(id) ON DELETE RESTRICT,
    quantidade INT NOT NULL CHECK (quantidade > 0),
    preco_unitario NUMERIC(10, 2) NOT NULL,
    desconto_unitario NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    valor_total_item NUMERIC(12, 2) NOT NULL
);

-- 10. Pagamentos
CREATE TABLE pagamentos (
    id SERIAL PRIMARY KEY,
    transacao_uuid UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    venda_id INT NOT NULL REFERENCES vendas(id) ON DELETE CASCADE,
    metodo VARCHAR(40) NOT NULL CHECK (metodo IN ('multicaixa_express', 'tpa_fisico', 'transferencia_bancaria', 'cartao_credito', 'dinheiro')),
    valor NUMERIC(12, 2) NOT NULL CHECK (valor > 0),
    status VARCHAR(30) NOT NULL DEFAULT 'concluido' CHECK (status IN ('pendente', 'concluido', 'estornado', 'recusado')),
    referencia_bancaria VARCHAR(80),
    data_pagamento TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resposta_gateway JSONB DEFAULT '{}'::jsonb
);

-- 11. Avaliações de Produtos
CREATE TABLE avaliacoes_produtos (
    id SERIAL PRIMARY KEY,
    produto_id INT NOT NULL REFERENCES produtos(id) ON DELETE CASCADE,
    cliente_id INT NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    nota INT NOT NULL CHECK (nota BETWEEN 1 AND 5),
    titulo VARCHAR(100),
    comentario TEXT,
    recomendaria BOOLEAN NOT NULL DEFAULT TRUE,
    data_avaliacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unq_produto_cliente_avaliacao UNIQUE (produto_id, cliente_id)
);

-- 12. Movimentações de Estoque
CREATE TABLE movimentacoes_estoque (
    id SERIAL PRIMARY KEY,
    produto_id INT NOT NULL REFERENCES produtos(id) ON DELETE CASCADE,
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('entrada', 'saida_venda', 'ajuste', 'devolucao')),
    quantidade INT NOT NULL,
    saldo_anterior INT NOT NULL,
    saldo_novo INT NOT NULL,
    referencia_venda_id INT REFERENCES vendas(id) ON DELETE SET NULL,
    motivo VARCHAR(150),
    data_movimento TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices estratégicos
CREATE INDEX idx_produtos_categoria ON produtos(categoria_id);
CREATE INDEX idx_produtos_fornecedor ON produtos(fornecedor_id);
CREATE INDEX idx_vendas_cliente ON vendas(cliente_id);
CREATE INDEX idx_vendas_vendedor ON vendas(vendedor_id);
CREATE INDEX idx_vendas_loja ON vendas(loja_id);
CREATE INDEX idx_vendas_status ON vendas(status_venda);
CREATE INDEX idx_vendas_data ON vendas(data_venda);
CREATE INDEX idx_itens_venda_venda ON itens_venda(venda_id);
CREATE INDEX idx_itens_venda_produto ON itens_venda(produto_id);
CREATE INDEX idx_pagamentos_venda ON pagamentos(venda_id);
"""

def populate_database(conn):
    cur = conn.cursor()
    print("🚀 Inserindo Categorias...")
    # Categorias principais e filhas
    cur.execute("""
        INSERT INTO categorias (id, nome, descricao, categoria_pai_id, icone) VALUES
        (1, 'Eletrônicos & Informática', 'Computadores, smartphones, periféricos e acessórios', NULL, 'laptop'),
        (2, 'Eletrodomésticos', 'Aparelhos para cozinha, climatização e lavandaria', NULL, 'home'),
        (3, 'Moda & Calçado', 'Roupas masculinas, femininas e calçados', NULL, 'shirt'),
        (4, 'Mobiliário & Decoração', 'Móveis para escritório, sala e quarto', NULL, 'armchair'),
        (5, 'Smartphones & Tablets', 'Dispositivos móveis e tablets de última geração', 1, 'smartphone'),
        (6, 'Portáteis & Desktops', 'Computadores portáteis e estações de trabalho', 1, 'computer'),
        (7, 'Acessórios & Cabos', 'Carregadores, capas, cabos e adaptadores', 1, 'cable'),
        (8, 'Frigoríficos & Arcas', 'Refrigeração doméstica e industrial', 2, 'snowflake'),
        (9, 'Climatização', 'Ar-condicionados e ventiladores', 2, 'wind'),
        (10, 'Cadeiras Ergonômicas', 'Cadeiras de escritório de alta performance', 4, 'chair')
        ON CONFLICT (id) DO NOTHING;
    """)
    cur.execute("SELECT setval('categorias_id_seq', (SELECT MAX(id) FROM categorias));")

    print("🚀 Inserindo Fornecedores...")
    fornecedores = [
        ("TechSupply Angola Lda", "Manuel Fernandes", "contato@techsupply.ao", "+244 923 111 222", "5412093842", "Luanda", "Angola", 4.8),
        ("InovaTech Global Corp", "Sarah Jenkins", "sales@inovatech.com", "+1 305 555 0192", "US9823412", "Miami", "Estados Unidos", 4.6),
        ("Móveis & Conforto Indústria", "António Banza", "comercial@moveisconforto.ao", "+244 931 444 555", "5409831201", "Benguela", "Angola", 4.4),
        ("ElectroLeste Importações", "Chen Wei", "export@electroleste.cn", "+86 21 6888 1234", "CN8732190", "Xangai", "China", 4.7),
        ("Kwanza Office Solutions", "Esperança Diniz", "vendas@kwanzaoffice.co.ao", "+244 945 777 888", "5411893420", "Luanda", "Angola", 4.9)
    ]
    execute_values(cur, """
        INSERT INTO fornecedores (nome_empresa, contato_nome, email, telefone, nif_cnpj, cidade, pais, avaliacao)
        VALUES %s;
    """, fornecedores)

    print("🚀 Inserindo Lojas...")
    lojas = [
        ("LJ-LUA-01", "Loja Central Talatona", "fisica", "Luanda", "Av. Pedro de Castro Van-Dúnem Loy, Edifício Sky", "+244 924 100 200"),
        ("LJ-LUA-02", "Loja Marginal Luanda", "fisica", "Luanda", "Avenida 4 de Fevereiro, Marginal", "+244 924 100 300"),
        ("LJ-BEN-01", "Filial Benguela Centro", "fisica", "Benguela", "Rua Aires de Ornelas, 42", "+244 924 200 100"),
        ("LJ-HUB-01", "Hub E-Commerce MustaVendas", "online", "Luanda", "Plataforma Digital Online & App", "+244 900 000 111"),
        ("LJ-LUB-01", "Quiosque Shopping Lubango", "quiosque", "Lubango", "Millennium Shopping Lubango, Piso 1", "+244 924 300 100")
    ]
    execute_values(cur, """
        INSERT INTO lojas (codigo, nome, tipo, cidade, endereco, telefone)
        VALUES %s;
    """, lojas)

    print("🚀 Inserindo Vendedores...")
    cur.execute("""
        INSERT INTO vendedores (id, codigo_cracha, nome, email, telefone, loja_id, gerente_id, comissao_percentual, meta_mensal, data_admissao) VALUES
        (1, 'VND-001', 'Carlos Alberto Silva', 'carlos.silva@mustavendas.ao', '+244 923 001 001', 1, NULL, 5.00, 1500000.00, '2022-01-15'),
        (2, 'VND-002', 'Beatriz Domingos', 'beatriz.domingos@mustavendas.ao', '+244 923 001 002', 1, 1, 3.50, 800000.00, '2023-03-10'),
        (3, 'VND-003', 'Edson Mateus', 'edson.mateus@mustavendas.ao', '+244 923 001 003', 2, 1, 3.50, 750000.00, '2023-06-01'),
        (4, 'VND-004', 'Graça de Fátima', 'graca.fatima@mustavendas.ao', '+244 923 001 004', 3, 1, 4.00, 600000.00, '2023-08-15'),
        (5, 'VND-005', 'João Paulo Baptista', 'joao.baptista@mustavendas.ao', '+244 923 001 005', 4, 1, 2.50, 2000000.00, '2024-01-10')
        ON CONFLICT (id) DO NOTHING;
    """)
    cur.execute("SELECT setval('vendedores_id_seq', (SELECT MAX(id) FROM vendedores));")

    print("🚀 Inserindo Produtos...")
    produtos = [
        ("PROD-IP15P-256", "iPhone 15 Pro Max 256GB Titânio", "Smartphone Apple com Chip A17 Pro, câmara 48MP e bateria de longa duração", 5, 2, 1250000.00, 980000.00, 24, 5, "UNIDADE", "ativo", Json({"marca": "Apple", "cor": "Titânio Natural", "armazenamento": "256GB", "garantia_meses": 12})),
        ("PROD-S24U-512", "Samsung Galaxy S24 Ultra 512GB", "Smartphone com Galaxy AI, tela Dynamic AMOLED 2X e Caneta S Pen integrada", 5, 1, 1180000.00, 910000.00, 18, 4, "UNIDADE", "ativo", Json({"marca": "Samsung", "cor": "Preto Titânio", "armazenamento": "512GB", "garantia_meses": 12})),
        ("PROD-MBA-M3", "MacBook Air 15 M3 16GB 512GB", "Computador portátil ultraleve da Apple com processador M3 e autonomia de 18 horas", 6, 2, 1650000.00, 1300000.00, 12, 3, "UNIDADE", "ativo", Json({"marca": "Apple", "ram": "16GB", "ssd": "512GB", "polegadas": 15.3})),
        ("PROD-DELL-XPS", "Dell XPS 13 Plus Intel Core i7 32GB", "Laptop profissional premium com ecrã táctil OLED 3.5K e chassi de alumínio", 6, 1, 1450000.00, 1150000.00, 9, 2, "UNIDADE", "ativo", Json({"marca": "Dell", "ram": "32GB", "ssd": "1TB", "ecra": "OLED 3.5K"})),
        ("PROD-AC-INV-18", "Ar-Condicionado Inverter 18.000 BTU", "Climatizador split silencioso com economia de energia de até 70% e gás R32", 9, 4, 420000.00, 310000.00, 35, 6, "UNIDADE", "ativo", Json({"btus": 18000, "tecnologia": "Inverter", "voltagem": "220V", "gas": "R32"})),
        ("PROD-FRIG-SBS", "Frigorífico Side by Side 520L Inox", "Frigorífico de duas portas com dispensador de água e gelo e tecnologia Frost Free", 8, 4, 850000.00, 640000.00, 14, 3, "UNIDADE", "ativo", Json({"capacidade_litros": 520, "acabamento": "Inox", "consumo_kwh": "A++"})),
        ("PROD-CAD-ERG-01", "Cadeira Ergonômica Presidente Mesh Pro", "Cadeira com apoio lombar dinâmico 3D, braços ajustáveis e suporte de cabeça", 10, 5, 260000.00, 175000.00, 45, 10, "UNIDADE", "ativo", Json({"material": "Mesh respirável", "peso_max_kg": 150, "ajustes": ["lombar", "bracos 3D", "inclinacao"]})),
        ("PROD-CAD-GAM-02", "Cadeira Gamer Reclinável RGB Master", "Cadeira acolchoada com iluminação LED personalizável e almofadas ortopédicas", 10, 3, 210000.00, 140000.00, 20, 5, "UNIDADE", "ativo", Json({"iluminacao": "RGB", "reclinavel_graus": 180, "espuma": "alta densidade"})),
        ("PROD-CGR-PD-65W", "Carregador Rápido GaN 65W USB-C Triplo", "Carregador universal ultracompacto para portáteis, telemóveis e tablets", 7, 1, 32000.00, 18000.00, 110, 15, "UNIDADE", "ativo", Json({"potencia_watts": 65, "portas": ["USB-C 1", "USB-C 2", "USB-A"]})),
        ("PROD-CB-TYPEC-2M", "Cabo USB-C para Lightning Reforçado 2m", "Cabo trançado de nylon com certificação Apple MFi e transferência rápida", 7, 1, 14500.00, 7200.00, 220, 30, "UNIDADE", "ativo", Json({"comprimento_metros": 2, "revestimento": "Nylon Trançado"}))
    ]
    execute_values(cur, """
        INSERT INTO produtos (sku, nome, descricao, categoria_id, fornecedor_id, preco_unitario, preco_custo, estoque_atual, estoque_minimo, unidade_medida, status, especificacoes)
        VALUES %s;
    """, produtos)

    print("🚀 Inserindo Clientes...")
    clientes = [
        ("António Manuel Gaspar", "antonio.gaspar@gmail.com", "+244 923 888 101", "005912831LA041", "individual", "vip", "Luanda", "Angola", 800000.00, 3450000.00),
        ("Teresa do Rosário Neto", "teresa.neto@hotmail.com", "+244 912 777 202", "004819203LA039", "individual", "ouro", "Luanda", "Angola", 500000.00, 1890000.00),
        ("Global Seguros & Finanças SA", "compras@globalseguros.ao", "+244 222 345 678", "5401928301", "corporativo", "vip", "Luanda", "Angola", 5000000.00, 12600000.00),
        ("Mateus Sebastião Pedro", "mateus.pedro@yahoo.com.br", "+244 931 555 303", "003910294BE021", "individual", "prata", "Benguela", "Angola", 300000.00, 680000.00),
        ("Helena Maria de Carvalho", "helena.carvalho@outlook.com", "+244 945 222 404", "006819201HU011", "individual", "bronze", "Lubango", "Angola", 200000.00, 210000.00),
        ("Orion Tech Soluções Digitais", "suporte@oriontech.ao", "+244 928 666 505", "5419823102", "corporativo", "ouro", "Luanda", "Angola", 2500000.00, 4800000.00),
        ("Francisco Xavier Gelson", "francisco.gelson@gmail.com", "+244 933 111 606", "007819202LA050", "individual", "ouro", "Luanda", "Angola", 400000.00, 1420000.00),
        ("Kátia Regina dos Santos", "katia.santos@sapo.ao", "+244 917 444 707", "002910492LA019", "individual", "bronze", "Luanda", "Angola", 150000.00, 85000.00)
    ]
    execute_values(cur, """
        INSERT INTO clientes (nome_completo, email, telefone, nif, tipo_cliente, segmento, cidade, pais, limite_credito, total_compras)
        VALUES %s;
    """, clientes)

    print("🚀 Inserindo Cupons...")
    cupons = [
        ("MUSTA10", "10% de Desconto em toda a loja", "percentual", 10.00, 50000.00, datetime.now(timezone.utc) - timedelta(days=30), datetime.now(timezone.utc) + timedelta(days=90), 200, 45, True),
        ("BEMVINDO2026", "Desconto Fixo de 25.000 Kz em compras superiores a 150.000 Kz", "valor_fixo", 25000.00, 150000.00, datetime.now(timezone.utc) - timedelta(days=60), datetime.now(timezone.utc) + timedelta(days=60), 500, 112, True),
        ("VIPTECH", "15% Exclusivo em Informática", "percentual", 15.00, 300000.00, datetime.now(timezone.utc) - timedelta(days=15), datetime.now(timezone.utc) + timedelta(days=120), 50, 18, True),
        ("BLACKFRIDAY", "20% Super Desconto Promocional", "percentual", 20.00, 100000.00, datetime.now(timezone.utc) - timedelta(days=120), datetime.now(timezone.utc) - timedelta(days=90), 1000, 1000, False)
    ]
    execute_values(cur, """
        INSERT INTO cupons (codigo, descricao, tipo_desconto, valor_desconto, valor_minimo_pedido, valido_de, valido_ate, limite_usos, usos_atuais, ativo)
        VALUES %s;
    """, cupons)

    print("🚀 Gerando e Inserindo Vendas, Itens, Pagamentos e Avaliações...")
    cur.execute("SELECT id, preco_unitario, estoque_atual FROM produtos;")
    produtos_db = cur.fetchall()

    cur.execute("SELECT id FROM clientes;")
    clientes_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM vendedores;")
    vendedores_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM lojas;")
    lojas_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id, codigo, tipo_desconto, valor_desconto FROM cupons WHERE ativo = TRUE;")
    cupons_db = cur.fetchall()

    formas_pag = ['multicaixa_express', 'tpa_fisico', 'transferencia_bancaria', 'cartao_credito', 'dinheiro']
    status_list = ['entregue', 'entregue', 'entregue', 'pago', 'em_separacao', 'enviado', 'pendente']

    now = datetime.now(timezone.utc)

    for i in range(1, 41):
        codigo_venda = f"VND-2026-{i:04d}"
        cliente_id = random.choice(clientes_ids)
        vendedor_id = random.choice(vendedores_ids)
        loja_id = random.choice(lojas_ids)
        usar_cupom = random.random() < 0.35
        cupom = random.choice(cupons_db) if usar_cupom else None
        cupom_id = cupom[0] if cupom else None

        dias_atras = random.randint(1, 90)
        data_venda = now - timedelta(days=dias_atras, hours=random.randint(0, 12), minutes=random.randint(0, 50))
        status_venda = random.choice(status_list)

        # Escolhe de 1 a 4 produtos aleatórios para a venda
        itens_escolhidos = random.sample(produtos_db, k=random.randint(1, 3))
        subtotal = 0.0
        itens_dados = []

        for p_id, p_preco, p_est in itens_escolhidos:
            qtd = random.randint(1, 3)
            p_preco_float = float(p_preco)
            desc_item = 0.0
            tot_item = (p_preco_float - desc_item) * qtd
            subtotal += tot_item
            itens_dados.append((p_id, qtd, p_preco_float, desc_item, tot_item))

        # Aplica desconto do cupom
        desconto = 0.0
        if cupom:
            if cupom[2] == 'percentual':
                desconto = round(subtotal * (float(cupom[3]) / 100.0), 2)
            else:
                desconto = min(float(cupom[3]), subtotal)

        frete = 5000.00 if loja_id == 4 else 0.00 # Loja 4 é online
        total = round(subtotal - desconto + frete, 2)
        forma_pag = random.choice(formas_pag)

        dados_entrega = Json({
            "metodo": "Expresso Luanda" if loja_id == 4 else "Retirada em Loja",
            "previsao_dias": 2,
            "transportadora": "MustaExpress Logística",
            "codigo_rastreio": f"TRK-{i:06d}-AO"
        })

        cur.execute("""
            INSERT INTO vendas (codigo_venda, cliente_id, vendedor_id, loja_id, cupom_id, data_venda, status_venda, valor_subtotal, valor_desconto, valor_frete, valor_total, forma_pagamento_predileta, observacoes, dados_entrega)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """, (
            codigo_venda, cliente_id, vendedor_id, loja_id, cupom_id, data_venda, status_venda,
            subtotal, desconto, frete, total, forma_pag,
            f"Venda registrada com sucesso no sistema PDV. Operador crachá VND-{vendedor_id:03d}.",
            dados_entrega
        ))
        venda_id = cur.fetchone()[0]

        # Inserir Itens
        for p_id, qtd, preco, desc, tot in itens_dados:
            cur.execute("""
                INSERT INTO itens_venda (venda_id, produto_id, quantidade, preco_unitario, desconto_unitario, valor_total_item)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (venda_id, p_id, qtd, preco, desc, tot))

            # Movimentação de estoque correspondente
            if status_venda in ('entregue', 'pago', 'enviado'):
                cur.execute("""
                    INSERT INTO movimentacoes_estoque (produto_id, tipo, quantidade, saldo_anterior, saldo_novo, referencia_venda_id, motivo, data_movimento)
                    VALUES (%s, 'saida_venda', %s, %s, %s, %s, %s, %s);
                """, (p_id, qtd, 50, 50 - qtd, venda_id, f"Saída por Venda {codigo_venda}", data_venda))

        # Pagamentos
        if status_venda != 'cancelado':
            pag_status = 'concluido' if status_venda in ('pago', 'em_separacao', 'enviado', 'entregue') else 'pendente'
            cur.execute("""
                INSERT INTO pagamentos (venda_id, metodo, valor, status, referencia_bancaria, data_pagamento, resposta_gateway)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                venda_id, forma_pag, total, pag_status,
                f"REF-{random.randint(100000000, 999999999)}",
                data_venda + timedelta(minutes=5),
                Json({"gateway": "EMIS Multicaixa", "autorizacao": f"AUTH{i:05d}", "codigo_resposta": "00"})
            ))

    print("🚀 Inserindo Avaliações de Produtos...")
    avaliacoes = [
        (1, 1, 5, "Excepcional performance!", "O iPhone 15 Pro Max é incrível, fotos perfeitas e bateria que dura o dia todo.", True),
        (2, 2, 5, "A tela e a inteligência artificial surpreendem", "Melhor smartphone Android do mercado. O zoom de 100x é surreal.", True),
        (3, 3, 5, "Portabilidade absoluta", "O MacBook Air M3 é super rápido, silencioso e muito leve para transportar.", True),
        (7, 4, 4, "Excelente cadeira para Home Office", "Muito confortável para quem passa 8 horas sentado. Fácil de montar.", True),
        (9, 6, 5, "Carrega tudo ao mesmo tempo", "Uso para carregar meu notebook e celular com um único carregador compacto.", True)
    ]
    execute_values(cur, """
        INSERT INTO avaliacoes_produtos (produto_id, cliente_id, nota, titulo, comentario, recomendaria)
        VALUES %s ON CONFLICT DO NOTHING;
    """, avaliacoes)

    conn.commit()
    print("✅ Banco de dados 'vendas_db' estruturado e populado com sucesso!")

def register_in_mustainfo():
    print("🔗 Registrando conexão 'vendas_db' no MustaInfo...")
    db = SessionLocal()
    try:
        # Usuários
        user2 = db.query(User).filter(User.id == 2).first()
        user1 = db.query(User).filter(User.id == 1).first()

        owner_id = user2.id if user2 else (user1.id if user1 else 1)

        # Verifica se já existe
        conn_existente = db.query(DBConnection).filter(
            DBConnection.database_name == "vendas_db"
        ).first()

        if not conn_existente:
            nova_conn = DBConnection(
                user_id=owner_id,
                name="Vendas & E-Commerce Demo",
                type="postgresql",
                host=secret_encrypt(DB_CONFIG["host"]),
                port=DB_CONFIG["port"],
                username=secret_encrypt(DB_CONFIG["user"]),
                password=secret_encrypt(DB_CONFIG["password"]),
                database_name=DB_CONFIG["dbname"],
                sslmode="disable",
                status="available",
                is_encrypted=True
            )
            db.add(nova_conn)
            db.commit()
            db.refresh(nova_conn)
            conn_id = nova_conn.id
            print(f"🎉 Conexão cadastrada com sucesso! ID={conn_id}, Nome='{nova_conn.name}'")
        else:
            conn_id = conn_existente.id
            conn_existente.name = "Vendas & E-Commerce Demo"
            conn_existente.status = "available"
            db.commit()
            print(f"ℹ️ Conexão existente encontrada: ID={conn_id}, Nome='{conn_existente.name}'")

        # Se houver outro usuário (ex: User 1), garante partilha direta ou acesso
        outro_user_id = 1 if owner_id == 2 else 2
        outro_user = db.query(User).filter(User.id == outro_user_id).first()
        if outro_user:
            share_existente = db.query(DBConnectionShare).filter(
                DBConnectionShare.connection_id == conn_id,
                DBConnectionShare.user_id == outro_user_id
            ).first()
            if not share_existente:
                novo_share = DBConnectionShare(
                    connection_id=conn_id,
                    user_id=outro_user_id,
                    access_level="manage"
                )
                db.add(novo_share)
                db.commit()
                print(f"🤝 Conexão partilhada com o usuário {outro_user_id} com nível 'manage'")

        # Sincroniza estatísticas e metadados no MustaInfo
        print("📊 Sincronizando estatísticas e estruturas com MustaInfo...")
        try:
            sync_connection_statistics(conn_id, db)
            print("✅ Estatísticas de tabelas sincronizadas!")
        except Exception as e:
            print(f"⚠️ Aviso ao sincronizar estatísticas: {e}")

        tabelas = [
            "categorias", "fornecedores", "produtos", "lojas", "vendedores",
            "clientes", "cupons", "vendas", "itens_venda", "pagamentos",
            "avaliacoes_produtos", "movimentacoes_estoque"
        ]
        print("📋 Sincronizando metadados de cada tabela com MustaInfo...")
        for tbl in tabelas:
            try:
                sincronizar_metadados_da_tabela(db, tbl, owner_id, connection_id=conn_id)
                print(f"  ✓ Metadados da tabela '{tbl}' sincronizados!")
            except Exception as e:
                print(f"  ⚠️ Aviso ao sincronizar '{tbl}': {e}")

    finally:
        db.close()

def main():
    print("🔌 Conectando ao banco 'vendas_db'...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        print("🛠️ Executando DDL para criar as 12 tabelas relacionais...")
        cur.execute(DDL_SCRIPT)
        conn.commit()

        populate_database(conn)
    finally:
        conn.close()

    register_in_mustainfo()
    print("\n✨ Base de Dados de Vendas para Demonstração pronta para uso no MustaInfo! ✨")

if __name__ == "__main__":
    main()
