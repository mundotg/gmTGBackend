import os
import sys
import json
import random
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values, Json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import dotenv
dotenv.load_dotenv(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env")))

from app.database import SessionLocal
from app.services.database_inspector import sync_connection_statistics

DB_CONFIG = {
    "dbname": "vendas_db",
    "user": "postgres",
    "password": "1234",
    "host": "127.0.0.1",
    "port": 5432,
    "sslmode": "disable"
}

def generate_large_dataset():
    print("🔌 Conectando ao PostgreSQL (vendas_db)...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("🧹 Limpando dados anteriores para repovoamento massivo...")
    cur.execute("""
        TRUNCATE TABLE 
            movimentacoes_estoque,
            avaliacoes_produtos,
            pagamentos,
            itens_venda,
            vendas,
            cupons,
            clientes,
            vendedores,
            lojas,
            produtos,
            fornecedores,
            categorias
        RESTART IDENTITY CASCADE;
    """)

    # ==========================================
    # 1. CATEGORIAS (24 categorias)
    # ==========================================
    print("📁 Inserindo 24 Categorias...")
    cur.execute("""
        INSERT INTO categorias (id, nome, descricao, categoria_pai_id, icone) VALUES
        (1, 'Eletrônicos & Tecnologia', 'Computadores, celulares e dispositivos inteligentes', NULL, 'cpu'),
        (2, 'Eletrodomésticos & Clima', 'Equipamentos para o lar, cozinha e climatização', NULL, 'home'),
        (3, 'Mobiliário & Escritório', 'Móveis corporativos, cadeiras e estações de trabalho', NULL, 'briefcase'),
        (4, 'Áudio & Vídeo', 'Fones de ouvido, colunas de som e televisores', NULL, 'headphones'),
        (5, 'Acessórios & Cabos', 'Adaptadores, cabos, suportes e periféricos essenciais', NULL, 'cable'),
        (6, 'Smartphones', 'Celulares topo de linha e intermediários', 1, 'smartphone'),
        (7, 'Portáteis & Laptops', 'Laptops ultrafinos e estações móveis', 1, 'laptop'),
        (8, 'Desktops & Servidores', 'PCs gamer e servidores empresariais', 1, 'server'),
        (9, 'Tablets & E-Readers', 'Tablets para produtividade e leitura digital', 1, 'tablet'),
        (10, 'Frigoríficos & Arcas', 'Refrigeração Frost Free e arcas congeladoras', 2, 'snowflake'),
        (11, 'Ar-Condicionado & Clima', 'Splits Inverter e climatizadores portáteis', 2, 'wind'),
        (12, 'Cafeteiras & Máquinas de Café', 'Cafeteiras elétricas e de cápsulas premium', 2, 'coffee'),
        (13, 'Micro-ondas & Fornos', 'Fornos embutidos e micro-ondas digitais', 2, 'flame'),
        (14, 'Cadeiras Ergonômicas', 'Cadeiras com suporte lombar 3D e braços ajustáveis', 3, 'chair'),
        (15, 'Mesas com Altura Ajustável', 'Mesas elétricas para trabalhar em pé ou sentado', 3, 'table'),
        (16, 'Estantes & Gaveteiros', 'Armazenamento e organização de escritório', 3, 'archive'),
        (17, 'Fones com Cancelamento de Ruído', 'Headphones Bluetooth over-ear e intra-auriculares', 4, 'headphones'),
        (18, 'Colunas de Som Bluetooth', 'Caixas de som portáteis à prova d água', 4, 'speaker'),
        (19, 'Smart TVs 4K & 8K', 'Televisores OLED e QLED de alta resolução', 4, 'tv'),
        (20, 'Carregadores Rápidos GaN', 'Carregadores multiporta ultracompactos', 5, 'zap'),
        (21, 'Cabos USB-C e Thunderbolt', 'Cabos reforçados de alta velocidade', 5, 'link'),
        (22, 'Hubs & Docks USB-C', 'Estações de expansão para múltiplas telas', 5, 'share-2'),
        (23, 'Teclados Mecânicos', 'Teclados para digitação rápida e gaming', 5, 'keyboard'),
        (24, 'Ratos Ergonômicos & Trackballs', 'Ratos sem fio com sensor de alta precisão', 5, 'mouse');
    """)
    cur.execute("SELECT setval('categorias_id_seq', 24);")

    # ==========================================
    # 2. FORNECEDORES (20 empresas)
    # ==========================================
    print("🏢 Inserindo 20 Fornecedores...")
    fornecedores = [
        ("TechSupply Angola Importações Lda", "Manuel Fernandes", "contato@techsupply.ao", "+244 923 111 201", "5412093842", "Luanda", "Angola", 4.8),
        ("InovaTech Global Miami Corp", "Sarah Jenkins", "sales@inovatech.com", "+1 305 555 0192", "US9823412", "Miami", "Estados Unidos", 4.7),
        ("Móveis & Conforto Indústria SA", "António Banza", "comercial@moveisconforto.ao", "+244 931 444 202", "5409831201", "Benguela", "Angola", 4.5),
        ("ElectroLeste Shanghai Export", "Chen Wei", "export@electroleste.cn", "+86 21 6888 1234", "CN8732190", "Xangai", "China", 4.6),
        ("Kwanza Office Solutions Lda", "Esperança Diniz", "vendas@kwanzaoffice.co.ao", "+244 945 777 203", "5411893420", "Luanda", "Angola", 4.9),
        ("Lusitana Componentes Portugal", "Rui Pedro Morais", "geral@lusitanacomponentes.pt", "+351 21 987 6543", "PT509123849", "Lisboa", "Portugal", 4.8),
        ("Shenzhen AudioTech Electronics", "Li Qiang", "b2b@audiotech-sz.com", "+86 755 8332 9988", "CN9123847", "Shenzhen", "China", 4.7),
        ("Sul Climatização Huambo Lda", "Domingos Kapango", "sulclima@angola.ao", "+244 912 333 404", "5408712390", "Huambo", "Angola", 4.4),
        ("BrasTech Distribuição Brasil", "Camila Albuquerque", "exportacao@brastech.com.br", "+55 11 3890 2211", "BR129384710001", "São Paulo", "Brasil", 4.6),
        ("EuroNordic Ergonomics Sweden", "Lars Svensson", "orders@euronordic.se", "+46 8 123 4567", "SE5561234567", "Estocolmo", "Suécia", 4.9),
        ("Angola Cables & Power Ltd", "José Chivinda", "pedidos@angolacablespower.ao", "+244 928 999 505", "5418293012", "Luanda", "Angola", 4.5),
        ("Nippon Digital Displays Japan", "Kenji Sato", "overseas@nippondigital.co.jp", "+81 3 5555 7890", "JP98127391", "Tóquio", "Japão", 4.9),
        ("Deltabox Embalagens & Logística", "Cláudia Simões", "atendimento@deltabox.ao", "+244 934 666 707", "5403928172", "Lobito", "Angola", 4.3),
        ("MegaChip Semiconductor Korea", "Min-jun Park", "chipsales@megachip.kr", "+82 2 3456 7890", "KR8829102", "Seul", "Coreia do Sul", 4.8),
        ("Atlântico Home Appliances Lda", "Sebastião Lourenço", "comercial@atlanticohome.ao", "+244 946 555 808", "5410928391", "Luanda", "Angola", 4.4),
        ("Apex Gaming Gears Taiwan", "David Chen", "sales@apexgaming.tw", "+886 2 2345 6789", "TW7728192", "Taipé", "Taiwan", 4.7),
        ("EcoLuz Iluminação Sustentável", "Maria Teresa Vaz", "info@ecoluz.ao", "+244 917 888 909", "5409182736", "Lubango", "Angola", 4.5),
        ("SoundWave Instruments Germany", "Hans Gruber", "export@soundwave-berlin.de", "+49 30 9876 5432", "DE812345678", "Berlim", "Alemanha", 4.8),
        ("Kalandula Tech Imports Lda", "Bernardo Afonso", "geral@kalandulatech.ao", "+244 925 111 010", "5419827364", "Malanje", "Angola", 4.3),
        ("Zenza Distribuição e Comércio", "Paula Cassule", "vendas@zenzadist.ao", "+244 938 222 121", "5401928374", "Caxito", "Angola", 4.6)
    ]
    execute_values(cur, """
        INSERT INTO fornecedores (nome_empresa, contato_nome, email, telefone, nif_cnpj, cidade, pais, avaliacao)
        VALUES %s;
    """, fornecedores)

    # ==========================================
    # 3. LOJAS (10 filiais)
    # ==========================================
    print("🏬 Inserindo 10 Lojas...")
    lojas = [
        ("LJ-LUA-01", "Loja Flagship Talatona", "fisica", "Luanda", "Av. Pedro de Castro Van-Dúnem Loy, Edifício Sky", "+244 924 100 200"),
        ("LJ-LUA-02", "Loja Marginal 4 de Fevereiro", "fisica", "Luanda", "Avenida 4 de Fevereiro, Marginal de Luanda", "+244 924 100 300"),
        ("LJ-LUA-03", "Loja Belas Shopping", "fisica", "Luanda", "Belas Shopping, Piso 1, Talatona", "+244 924 100 400"),
        ("LJ-BEN-01", "Filial Benguela Centro", "fisica", "Benguela", "Rua Aires de Ornelas, Edifício Comercial Benguela", "+244 924 200 100"),
        ("LJ-LOB-01", "Filial Porto do Lobito", "fisica", "Lobito", "Avenida da Independência, 115", "+244 924 200 200"),
        ("LJ-HUA-01", "Filial Huambo Cidade Alta", "fisica", "Huambo", "Rua do Comércio, 88", "+244 924 300 100"),
        ("LJ-LUB-01", "Loja Millennium Lubango", "fisica", "Lubango", "Millennium Shopping Lubango, Loja 24", "+244 924 400 100"),
        ("LJ-CAB-01", "Filial Cabinda Litoral", "fisica", "Cabinda", "Rua Duque de Chiappas, 40", "+244 924 500 100"),
        ("LJ-HUB-01", "Hub Central E-Commerce MustaVendas", "online", "Luanda", "Plataforma Digital Online & App Mobile", "+244 900 000 111"),
        ("LJ-QSK-01", "Quiosque Aeroporto 4 de Fevereiro", "quiosque", "Luanda", "Aeroporto Internacional 4 de Fevereiro, Terminal 1", "+244 924 600 100")
    ]
    execute_values(cur, """
        INSERT INTO lojas (codigo, nome, tipo, cidade, endereco, telefone)
        VALUES %s;
    """, lojas)

    # ==========================================
    # 4. VENDEDORES (30 vendedores)
    # ==========================================
    print("👔 Inserindo 30 Vendedores...")
    cur.execute("""
        INSERT INTO vendedores (id, codigo_cracha, nome, email, telefone, loja_id, gerente_id, comissao_percentual, meta_mensal, data_admissao) VALUES
        (1, 'DIR-001', 'Carlos Alberto Silva', 'carlos.silva@mustavendas.ao', '+244 923 001 001', 1, NULL, 6.00, 3000000.00, '2021-02-01'),
        (2, 'DIR-002', 'Helena Maria Quaresma', 'helena.quaresma@mustavendas.ao', '+244 923 001 002', 4, NULL, 5.50, 2500000.00, '2021-06-15'),
        (3, 'DIR-003', 'António Joaquim Banza', 'antonio.banza@mustavendas.ao', '+244 923 001 003', 6, NULL, 5.50, 2000000.00, '2022-01-10'),
        (4, 'DIR-004', 'Margarida dos Santos', 'margarida.santos@mustavendas.ao', '+244 923 001 004', 9, NULL, 5.00, 4500000.00, '2022-03-20')
        ON CONFLICT (id) DO NOTHING;
    """)
    cur.execute("SELECT setval('vendedores_id_seq', 4);")

    vendedores_dados = [
        ("VND-005", "Beatriz Domingos", "beatriz.domingos@mustavendas.ao", "+244 923 001 005", 1, 1, 3.50, 950000.00, "2023-01-15"),
        ("VND-006", "Edson Mateus", "edson.mateus@mustavendas.ao", "+244 923 001 006", 1, 1, 3.50, 900000.00, "2023-02-01"),
        ("VND-007", "Graça de Fátima", "graca.fatima@mustavendas.ao", "+244 923 001 007", 2, 1, 3.80, 850000.00, "2023-03-10"),
        ("VND-008", "João Paulo Baptista", "joao.baptista@mustavendas.ao", "+244 923 001 008", 2, 1, 3.50, 800000.00, "2023-04-05"),
        ("VND-009", "Teresa Cristina Neto", "teresa.neto@mustavendas.ao", "+244 923 001 009", 3, 1, 4.00, 1100000.00, "2022-08-15"),
        ("VND-010", "Mauro Sérgio Costa", "mauro.costa@mustavendas.ao", "+244 923 001 010", 3, 1, 3.50, 850000.00, "2023-05-20"),
        ("VND-011", "Cláudia Manuela Diniz", "claudia.diniz@mustavendas.ao", "+244 923 001 011", 4, 2, 3.50, 750000.00, "2023-06-12"),
        ("VND-012", "Fernando Gouveia", "fernando.gouveia@mustavendas.ao", "+244 923 001 012", 4, 2, 3.50, 700000.00, "2023-07-01"),
        ("VND-013", "Nádia dos Remédios", "nadia.remedios@mustavendas.ao", "+244 923 001 013", 5, 2, 3.80, 720000.00, "2023-08-10"),
        ("VND-014", "Valdemar Kapango", "valdemar.kapango@mustavendas.ao", "+244 923 001 014", 6, 3, 3.50, 680000.00, "2023-09-01"),
        ("VND-015", "Rosa Maria Pedro", "rosa.pedro@mustavendas.ao", "+244 923 001 015", 6, 3, 3.50, 650000.00, "2023-09-15"),
        ("VND-016", "Sérgio Luvualu", "sergio.luvualu@mustavendas.ao", "+244 923 001 016", 7, 3, 3.80, 750000.00, "2023-10-01"),
        ("VND-017", "Gisela Cassoma", "gisela.cassoma@mustavendas.ao", "+244 923 001 017", 7, 3, 3.50, 700000.00, "2023-11-05"),
        ("VND-018", "Armindo Tchissola", "armindo.tchissola@mustavendas.ao", "+244 923 001 018", 8, 3, 4.00, 800000.00, "2023-12-01"),
        ("VND-019", "Patrícia Van-Dúnem", "patricia.vandunem@mustavendas.ao", "+244 923 001 019", 9, 4, 3.00, 1800000.00, "2022-09-10"),
        ("VND-020", "Daniel Kwanza", "daniel.kwanza@mustavendas.ao", "+244 923 001 020", 9, 4, 3.00, 1600000.00, "2023-02-15"),
        ("VND-021", "Luísa Muachissengue", "luisa.muachi@mustavendas.ao", "+244 923 001 021", 9, 4, 3.00, 1500000.00, "2023-05-01"),
        ("VND-022", "Bernardo Caxito", "bernardo.caxito@mustavendas.ao", "+244 923 001 022", 9, 4, 3.00, 1400000.00, "2023-07-20"),
        ("VND-023", "Elizeth Sambongo", "elizeth.sambongo@mustavendas.ao", "+244 923 001 023", 10, 1, 4.00, 600000.00, "2023-10-15"),
        ("VND-024", "Júlio César Gelson", "julio.gelson@mustavendas.ao", "+244 923 001 024", 1, 1, 3.50, 880000.00, "2024-01-08"),
        ("VND-025", "Yara de Fátima", "yara.fatima@mustavendas.ao", "+244 923 001 025", 2, 1, 3.50, 820000.00, "2024-02-01"),
        ("VND-026", "Afonso Vunge", "afonso.vunge@mustavendas.ao", "+244 923 001 026", 3, 1, 3.80, 890000.00, "2024-02-15"),
        ("VND-027", "Catarina Malanje", "catarina.malanje@mustavendas.ao", "+244 923 001 027", 4, 2, 3.50, 710000.00, "2024-03-01"),
        ("VND-028", "Ismael Chimuco", "ismael.chimuco@mustavendas.ao", "+244 923 001 028", 5, 2, 3.50, 700000.00, "2024-03-15"),
        ("VND-029", "Soraya Cangombe", "soraya.cangombe@mustavendas.ao", "+244 923 001 029", 7, 3, 3.50, 720000.00, "2024-04-01"),
        ("VND-030", "Gelson Quaresma", "gelson.quaresma@mustavendas.ao", "+244 923 001 030", 9, 4, 3.20, 1550000.00, "2024-04-10")
    ]
    execute_values(cur, """
        INSERT INTO vendedores (codigo_cracha, nome, email, telefone, loja_id, gerente_id, comissao_percentual, meta_mensal, data_admissao)
        VALUES %s;
    """, vendedores_dados)
    cur.execute("SELECT setval('vendedores_id_seq', 30);")

    # ==========================================
    # 5. PRODUTOS (80 produtos catalogados)
    # ==========================================
    print("📦 Inserindo 80 Produtos com JSONB...")
    marcas = ["Apple", "Samsung", "Xiaomi", "Dell", "Lenovo", "HP", "Sony", "LG", "JBL", "Logitech", "HPE", "Asus", "DeLonghi"]
    produtos_base = [
        ("PROD-IP15P-256", "iPhone 15 Pro Max 256GB Titânio", "Smartphone Apple com Chip A17 Pro, câmara 48MP", 6, 2, 1250000.00, 980000.00, 80, 5, "UNIDADE", "ativo", {"marca": "Apple", "cor": "Titânio Natural", "armazenamento": "256GB"}),
        ("PROD-IP15-128", "iPhone 15 128GB Azul Claro", "Smartphone Apple com Dynamic Island e câmara avançada", 6, 2, 850000.00, 680000.00, 95, 6, "UNIDADE", "ativo", {"marca": "Apple", "cor": "Azul", "armazenamento": "128GB"}),
        ("PROD-S24U-512", "Samsung Galaxy S24 Ultra 512GB", "Smartphone com Galaxy AI, tela Dynamic AMOLED 2X e Caneta S Pen", 6, 1, 1180000.00, 910000.00, 70, 4, "UNIDADE", "ativo", {"marca": "Samsung", "cor": "Preto Titânio", "armazenamento": "512GB"}),
        ("PROD-S24-256", "Samsung Galaxy S24 Plus 256GB", "Smartphone premium com processador Snapdragon 8 Gen 3", 6, 1, 920000.00, 720000.00, 60, 5, "UNIDADE", "ativo", {"marca": "Samsung", "cor": "Violeta", "armazenamento": "256GB"}),
        ("PROD-MI14-512", "Xiaomi 14 Ultra 512GB Leica", "Câmara quádrupla Leica de 50MP com sensor de 1 polegada", 6, 4, 980000.00, 760000.00, 45, 3, "UNIDADE", "ativo", {"marca": "Xiaomi", "camera": "Leica 50MP", "armazenamento": "512GB"}),
        ("PROD-RED-NOTE13", "Xiaomi Redmi Note 13 Pro 256GB", "Melhor custo-benefício com câmara de 200MP e carga de 67W", 6, 4, 280000.00, 210000.00, 180, 10, "UNIDADE", "ativo", {"marca": "Xiaomi", "camera": "200MP", "ram": "8GB"}),
        
        ("PROD-MBA-M3-15", "MacBook Air 15 M3 16GB 512GB", "Portátil Apple com chip M3, design ultrafino e 18h de bateria", 7, 2, 1650000.00, 1300000.00, 40, 3, "UNIDADE", "ativo", {"marca": "Apple", "ram": "16GB", "ssd": "512GB", "polegadas": 15.3}),
        ("PROD-MBP-M3PRO", "MacBook Pro 16 M3 Pro 36GB 1TB", "Estação de trabalho móvel com ecrã Liquid Retina XDR", 7, 2, 2890000.00, 2300000.00, 25, 2, "UNIDADE", "ativo", {"marca": "Apple", "ram": "36GB", "ssd": "1TB", "polegadas": 16.2}),
        ("PROD-DELL-XPS13", "Dell XPS 13 Plus Intel Core i7 32GB", "Portátil ultra premium com ecrã táctil OLED 3.5K", 7, 1, 1450000.00, 1150000.00, 35, 2, "UNIDADE", "ativo", {"marca": "Dell", "ram": "32GB", "ssd": "1TB"}),
        ("PROD-LEN-X1", "Lenovo ThinkPad X1 Carbon Gen 11", "Clássico corporativo ultraleve de fibra de carbono", 7, 1, 1550000.00, 1220000.00, 30, 3, "UNIDADE", "ativo", {"marca": "Lenovo", "peso_kg": 1.12, "ram": "16GB"}),
        ("PROD-ASUS-G14", "Asus ROG Zephyrus G14 RTX 4070", "Portátil gamer de alta performance com ecrã Nebula 165Hz", 7, 16, 1780000.00, 1420000.00, 28, 2, "UNIDADE", "ativo", {"marca": "Asus", "gpu": "RTX 4070 8GB"}),
        
        ("PROD-SRV-DL380", "HPE ProLiant DL380 Gen10 Xeon", "Servidor rack 2U de alta disponibilidade para data centers", 8, 1, 4800000.00, 3900000.00, 12, 1, "UNIDADE", "ativo", {"marca": "HPE", "formato": "Rack 2U"}),
        ("PROD-PC-WK-DELL", "Dell Precision 3660 Tower i9 64GB", "Workstation profissional para modelagem 3D e renderização", 8, 1, 2450000.00, 1950000.00, 18, 2, "UNIDADE", "ativo", {"marca": "Dell", "ram": "64GB"}),
        
        ("PROD-IPAD-PRO13", "iPad Pro 13 Chip M4 OLED 256GB", "Tablet mais avançado do mundo com ecrã Ultra Retina XDR", 9, 2, 1380000.00, 1080000.00, 50, 4, "UNIDADE", "ativo", {"marca": "Apple", "chip": "M4", "tela": "OLED 13"}),
        ("PROD-TAB-S9U", "Samsung Galaxy Tab S9 Ultra 512GB", "Tablet com tela gigante de 14.6 polegadas e resistência IP68", 9, 1, 1050000.00, 820000.00, 40, 3, "UNIDADE", "ativo", {"marca": "Samsung", "tela": "14.6"}),

        ("PROD-FRIG-SBS", "Frigorífico Side by Side 520L Inox", "Frigorífico duas portas com dispensador de água e gelo", 10, 15, 850000.00, 640000.00, 30, 3, "UNIDADE", "ativo", {"capacidade": "520 Litros", "frost_free": True}),
        ("PROD-FRIG-DUP-420", "Frigorífico Duplex Frost Free 420L", "Alta eficiência energética e congelamento rápido", 10, 15, 520000.00, 390000.00, 45, 4, "UNIDADE", "ativo", {"capacidade": "420 Litros"}),
        ("PROD-ARCA-HOR-300", "Arca Frigorífica Horizontal 300L", "Excelente retenção de frio mesmo em quedas de energia", 10, 4, 380000.00, 280000.00, 55, 5, "UNIDADE", "ativo", {"capacidade": "300 Litros"}),
        ("PROD-AC-INV-12", "Ar-Condicionado Split Inverter 12.000 BTU", "Super silencioso com tecnologia de íons e economia A+++", 11, 8, 320000.00, 230000.00, 90, 8, "UNIDADE", "ativo", {"btus": 12000, "inverter": True}),
        ("PROD-AC-INV-18", "Ar-Condicionado Split Inverter 18.000 BTU", "Ideal para salas amplas e escritórios com refrigeração turbo", 11, 8, 420000.00, 310000.00, 80, 6, "UNIDADE", "ativo", {"btus": 18000, "inverter": True}),
        ("PROD-AC-INV-24", "Ar-Condicionado Split Inverter 24.000 BTU", "Alta capacidade para ambientes comerciais de médio porte", 11, 8, 590000.00, 440000.00, 45, 4, "UNIDADE", "ativo", {"btus": 24000, "inverter": True}),
        
        ("PROD-CAF-NES-PRO", "Máquina de Café Nespresso Zenius Pro", "Ideal para recepções e salas de reunião empresariais", 12, 6, 240000.00, 170000.00, 60, 5, "UNIDADE", "ativo", {"pressao": "19 bar"}),
        ("PROD-CAF-EXP-DEL", "Cafeteira Expresso DeLonghi Dedica", "Cafeteira manual de aço inoxidável para café moído e sachês", 12, 6, 195000.00, 135000.00, 75, 6, "UNIDADE", "ativo", {"pressao": "15 bar"}),
        ("PROD-MIC-DIG-32L", "Micro-ondas Digital Inox 32 Litros", "Com receitas pré-programadas e função descongelamento inteligente", 13, 15, 115000.00, 78000.00, 85, 8, "UNIDADE", "ativo", {"capacidade": "32L"}),
        
        ("PROD-CAD-ERG-01", "Cadeira Ergonômica Presidente Mesh Pro", "Apoio lombar 3D dinâmico, braços ajustáveis e encosto reclinável", 14, 10, 260000.00, 175000.00, 110, 10, "UNIDADE", "ativo", {"material": "Mesh", "peso_max": "150kg"}),
        ("PROD-CAD-DIR-COURO", "Cadeira Diretor em Couro Legítimo", "Conforto máximo com acabamento clássico em couro acolchoado", 14, 3, 310000.00, 210000.00, 50, 4, "UNIDADE", "ativo", {"material": "Couro"}),
        ("PROD-CAD-GAM-RGB", "Cadeira Gamer Reclinável 180° RGB Master", "Com almofadas ortopédicas de memória e iluminação LED", 14, 16, 210000.00, 140000.00, 65, 5, "UNIDADE", "ativo", {"reclinavel": "180°"}),
        ("PROD-MES-ELET-160", "Mesa Elevatória Elétrica Standing Desk 160cm", "Dois motores potentes e painel digital com 4 memórias", 15, 10, 450000.00, 310000.00, 35, 3, "UNIDADE", "ativo", {"largura": "160cm"}),
        ("PROD-GAV-MET-03", "Gaveteiro Volante de Metal 3 Gavetas", "Gaveteiro com chave e rodízios reforçados de silicone", 16, 5, 85000.00, 52000.00, 120, 10, "UNIDADE", "ativo", {"gavetas": 3}),
        
        ("PROD-FONE-SONY-XM5", "Headphone Sony WH-1000XM5 ANC", "Referência mundial em cancelamento de ruído e áudio Hi-Res", 17, 7, 340000.00, 250000.00, 80, 5, "UNIDADE", "ativo", {"anc": True, "bateria": "30h"}),
        ("PROD-FONE-AIRPODS-P2", "Apple AirPods Pro 2ª Geração USB-C", "Áudio espacial adaptativo e cancelamento de ruído ativo 2x mais forte", 17, 2, 260000.00, 195000.00, 120, 8, "UNIDADE", "ativo", {"porta": "USB-C"}),
        ("PROD-COL-JBL-BOOM", "Coluna Bluetooth JBL Boombox 3 Wi-Fi", "Graves profundos, bateria de 24 horas e resistência à água IP67", 18, 7, 395000.00, 285000.00, 45, 3, "UNIDADE", "ativo", {"ip67": True, "potencia": "180W"}),
        ("PROD-COL-JBL-FLIP6", "Coluna Portátil JBL Flip 6 Bluetooth", "Compacta, resistente a quedas e som nítido de duas vias", 18, 7, 115000.00, 75000.00, 190, 15, "UNIDADE", "ativo", {"potencia": "30W"}),
        ("PROD-TV-LG-OLED65", "Smart TV 65 4K LG OLED evo C3", "Preto absoluto, Dolby Vision, 120Hz e som imersivo Dolby Atmos", 19, 12, 1450000.00, 1120000.00, 25, 2, "UNIDADE", "ativo", {"tela": "OLED 65"}),
        ("PROD-TV-SAM-QLED55", "Smart TV 55 4K Samsung QLED Q60D", "Cores vibrantes com tecnologia de Pontos Quânticos e design fino", 19, 1, 680000.00, 510000.00, 48, 4, "UNIDADE", "ativo", {"tela": "QLED 55"}),
        
        ("PROD-CGR-GAN-65W", "Carregador GaN 65W Rápido 3 Portas", "Carrega MacBook, iPhone e iPad simultaneamente em alta velocidade", 20, 1, 32000.00, 18000.00, 350, 20, "UNIDADE", "ativo", {"potencia": "65W"}),
        ("PROD-CGR-GAN-140W", "Carregador GaN Pro 140W Ultra Rápido", "Potência máxima para portáteis de alto desempenho e estações", 20, 1, 65000.00, 38000.00, 160, 10, "UNIDADE", "ativo", {"potencia": "140W"}),
        ("PROD-CAB-TB4-2M", "Cabo Thunderbolt 4 Pro 2 Metros 40Gbps", "Transferência ultra-rápida de 40Gbps e saída de vídeo 8K", 21, 11, 42000.00, 22000.00, 210, 15, "UNIDADE", "ativo", {"taxa": "40Gbps"}),
        ("PROD-CAB-USBC-NYL", "Cabo USB-C para USB-C 100W Trançado 2m", "Revestido em nylon balístico de ultra resistência", 21, 11, 12500.00, 6000.00, 650, 50, "UNIDADE", "ativo", {"potencia": "100W"}),
        ("PROD-DOCK-12IN1", "Dock Station USB-C 12 em 1 Triplo Display", "Expansão para 2x HDMI 4K, DisplayPort, Ethernet 1Gbps, USB 3.0", 22, 1, 89000.00, 52000.00, 130, 8, "UNIDADE", "ativo", {"portas": 12}),
        ("PROD-TEC-LOGI-MX", "Teclado Mecânico Logitech MX Mechanical", "Switches silenciosos, retroiluminação inteligente e digitação fluida", 23, 5, 145000.00, 98000.00, 95, 6, "UNIDADE", "ativo", {"switches": "Tactile Quiet"}),
        ("PROD-MOU-LOGI-MX3S", "Rato Sem Fio Logitech MX Master 3S", "Sensor 8000 DPI que funciona em vidro e rolagem eletromagnética MagSpeed", 24, 5, 95000.00, 62000.00, 140, 10, "UNIDADE", "ativo", {"dpi": 8000}),
        ("PROD-MOU-VERT-ERG", "Rato Ergonômico Vertical Sem Fio 57°", "Previne lesões por esforço repetitivo (LER) e dores no pulso", 24, 10, 48000.00, 28000.00, 180, 12, "UNIDADE", "ativo", {"angulo": "57°"})
    ]

    produtos_insert = []
    for p in produtos_base:
        produtos_insert.append((
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], Json(p[11])
        ))

    execute_values(cur, """
        INSERT INTO produtos (sku, nome, descricao, categoria_id, fornecedor_id, preco_unitario, preco_custo, estoque_atual, estoque_minimo, unidade_medida, status, especificacoes)
        VALUES %s;
    """, produtos_insert)

    # ==========================================
    # 6. CLIENTES (2.500 CLIENTES)
    # ==========================================
    print("👥 Inserindo 2.500 Clientes...")
    nomes = ["Manuel", "António", "João", "José", "Francisco", "Mateus", "Sebastião", "Domingos", "Carlos", "Paulo", 
             "Maria", "Ana", "Teresa", "Helena", "Beatriz", "Esperança", "Graça", "Fátima", "Isabel", "Cláudia", 
             "Nádia", "Rosa", "Catarina", "Elizeth", "Yara", "Kátia", "Gisela", "Soraya", "Patrícia", "Afonso",
             "Rui", "Jorge", "Pedro", "Luís", "Fernando", "Nelson", "Bernardo", "Simão", "Edson", "Valdemar"]
    sobrenomes = ["dos Santos", "Silva", "Fernandes", "Neto", "Gaspar", "Pedro", "Carvalho", "Banza", "Diniz", "Gelson",
                  "Costa", "Baptista", "Quaresma", "Kapango", "Muachissengue", "Caxito", "Sambongo", "Vunge", "Malanje", "Chimuco",
                  "Lourenço", "Afonso", "Gouveia", "Tchissola", "Luvualu", "Cangombe", "Albuquerque", "Morais"]
    cidades = ["Luanda", "Luanda", "Luanda", "Luanda", "Benguela", "Lobito", "Huambo", "Lubango", "Cabinda", "Soyo", "Malanje", "Namibe", "Uíge"]
    segmentos = ["bronze", "bronze", "bronze", "prata", "prata", "ouro", "ouro", "vip"]
    provedores = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "sapo.ao", "ao.telecom.net", "empresa.ao"]

    clientes_dados = []
    # Clientes Corporativos
    empresas_corp = [
        ("Sonangol E.P. - Direcção de TI", "ti.compras@sonangol.co.ao", "+244 222 690 000", "5401002341", "corporativo", "vip", "Luanda", 15000000.00),
        ("Banco Angolano de Investimentos (BAI)", "aquisicoes@bancobai.ao", "+244 222 693 000", "5401092831", "corporativo", "vip", "Luanda", 12000000.00),
        ("Unitel SA - Operações de Rede", "suprimentos@unitel.ao", "+244 923 100 000", "5403918274", "corporativo", "vip", "Luanda", 20000000.00),
        ("ENSA Seguros de Angola", "compras@ensa.co.ao", "+244 222 334 455", "5401928340", "corporativo", "ouro", "Luanda", 8000000.00),
        ("Universidade Agostinho Neto", "reitoria.compras@uan.ao", "+244 222 310 000", "5400192831", "corporativo", "ouro", "Luanda", 5000000.00),
        ("Clínica Girassol Luanda", "patrimonio@clinicagirassol.co.ao", "+244 222 670 000", "5402918239", "corporativo", "ouro", "Luanda", 6500000.00),
        ("Porto de Luanda E.P.", "aprov@portoluanda.co.ao", "+244 222 311 223", "5403019284", "corporativo", "vip", "Luanda", 10000000.00),
        ("BFA - Banco de Fomento Angola", "logistica@bfa.ao", "+244 222 699 000", "5401129384", "corporativo", "vip", "Luanda", 14000000.00),
        ("TAAG Linhas Aéreas de Angola", "suprimentos@taag.com", "+244 222 695 000", "5401920391", "corporativo", "vip", "Luanda", 18000000.00),
        ("Endiama E.P. Mineração", "comercial@endiama.co.ao", "+244 222 692 000", "5402019283", "corporativo", "vip", "Luanda", 16000000.00)
    ]
    for emp in empresas_corp:
        clientes_dados.append((emp[0], emp[1], emp[2], emp[3], emp[4], emp[5], emp[6], "Angola", emp[7], 0.00))

    # 2.490 Clientes Individuais
    for idx in range(1, 2491):
        n = random.choice(nomes)
        s1 = random.choice(sobrenomes)
        s2 = random.choice(sobrenomes)
        nome_comp = f"{n} {s1} {s2}"
        email = f"{n.lower()}.{s1.lower().replace(' ', '')}{idx}@{random.choice(provedores)}"
        tel = f"+244 9{random.randint(1,4)}{random.randint(1,9)} {random.randint(100,999)} {random.randint(100,999)}"
        nif = f"00{random.randint(100000,999999)}LA0{random.randint(10,99)}"
        cid = random.choice(cidades)
        seg = random.choice(segmentos)
        limite = 150000.00 if seg == 'bronze' else (350000.00 if seg == 'prata' else (750000.00 if seg == 'ouro' else 1500000.00))
        clientes_dados.append((nome_comp, email, tel, nif, "individual", seg, cid, "Angola", limite, 0.00))

    execute_values(cur, """
        INSERT INTO clientes (nome_completo, email, telefone, nif, tipo_cliente, segmento, cidade, pais, limite_credito, total_compras)
        VALUES %s;
    """, clientes_dados, page_size=1000)

    # ==========================================
    # 7. CUPONS (12 cupons)
    # ==========================================
    print("🎟️ Inserindo 12 Cupons...")
    now = datetime.now(timezone.utc)
    cupons = [
        ("MUSTA10", "10% de Desconto em toda a loja", "percentual", 10.00, 50000.00, now - timedelta(days=180), now + timedelta(days=180), 5000, 1312, True),
        ("BEMVINDO2026", "25.000 Kz de desconto fixo em compras acima de 150.000 Kz", "valor_fixo", 25000.00, 150000.00, now - timedelta(days=120), now + timedelta(days=120), 8000, 3840, True),
        ("VIPTECH15", "15% de Desconto em Tecnologia & Laptops", "percentual", 15.00, 300000.00, now - timedelta(days=60), now + timedelta(days=120), 2000, 989, True),
        ("ANGOLA50K", "50.000 Kz de desconto em compras superiores a 500.000 Kz", "valor_fixo", 50000.00, 500000.00, now - timedelta(days=90), now + timedelta(days=90), 1000, 442, True),
        ("CLIMA2026", "8% de desconto em equipamentos de climatização", "percentual", 8.00, 100000.00, now - timedelta(days=45), now + timedelta(days=90), 1500, 388, True),
        ("ESCRITORIO5", "5% em todo o mobiliário e cadeiras ergonômicas", "percentual", 5.00, 80000.00, now - timedelta(days=30), now + timedelta(days=180), 3000, 756, True),
        ("FRETEGRATIS", "Desconto fixo de 5.000 Kz no valor do frete", "valor_fixo", 5000.00, 30000.00, now - timedelta(days=60), now + timedelta(days=60), 6000, 2610, True),
        ("BLACKFRIDAY", "20% Super Promoção Black Friday", "percentual", 20.00, 100000.00, now - timedelta(days=360), now - timedelta(days=330), 5000, 5000, False),
        ("NATAL2025", "12% de Boas Festas", "percentual", 12.00, 60000.00, now - timedelta(days=330), now - timedelta(days=300), 5000, 5000, False),
        ("ANIVERSARIO", "15.000 Kz de bônus de aniversário MustaInfo", "valor_fixo", 15000.00, 100000.00, now - timedelta(days=200), now + timedelta(days=200), 2000, 840, True),
        ("VERAO2026", "10% em climatização e bebidas", "percentual", 10.00, 75000.00, now - timedelta(days=40), now + timedelta(days=100), 2000, 695, True),
        ("CORPORATIVO", "18% Exclusivo para Clientes Corporativos", "percentual", 18.00, 1000000.00, now - timedelta(days=150), now + timedelta(days=200), 1000, 378, True)
    ]
    execute_values(cur, """
        INSERT INTO cupons (codigo, descricao, tipo_desconto, valor_desconto, valor_minimo_pedido, valido_de, valido_ate, limite_usos, usos_atuais, ativo)
        VALUES %s;
    """, cupons)

    # ==========================================
    # 8. VENDAS, ITENS E PAGAMENTOS (10.000 VENDAS EM LOTES)
    # ==========================================
    TOTAL_VENDAS = 10000
    print(f"🛒 Gerando e inserindo {TOTAL_VENDAS:,} Vendas com ~25.000 Itens e ~9.600 Pagamentos...")
    
    cur.execute("SELECT id, preco_unitario, estoque_atual, nome FROM produtos;")
    produtos_db = cur.fetchall()

    cur.execute("SELECT id FROM clientes;")
    clientes_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM vendedores;")
    vendedores_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM lojas;")
    lojas_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id, codigo, tipo_desconto, valor_desconto, valor_minimo_pedido FROM cupons WHERE ativo = TRUE;")
    cupons_db = cur.fetchall()

    formas_pag = ['multicaixa_express', 'multicaixa_express', 'tpa_fisico', 'tpa_fisico', 'transferencia_bancaria', 'cartao_credito', 'dinheiro']
    status_weights = [
        ('entregue', 68),
        ('pago', 14),
        ('enviado', 8),
        ('em_separacao', 5),
        ('pendente', 3),
        ('cancelado', 2)
    ]
    status_choices = []
    for s, w in status_weights:
        status_choices.extend([s] * w)

    BATCH_SIZE = 1000
    total_itens_inseridos = 0
    total_pagamentos_inseridos = 0

    for batch_start in range(1, TOTAL_VENDAS + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE - 1, TOTAL_VENDAS)
        batch_vendas_data = []
        batch_itens_map = [] # lista de tuples (index_in_batch, [itens])

        for i in range(batch_start, batch_end + 1):
            cod_venda = f"VND-2025-26-{i:06d}"
            c_id = random.choice(clientes_ids)
            v_id = random.choice(vendedores_ids)
            l_id = random.choice(lojas_ids)
            
            usar_cupom = random.random() < 0.32
            cupom = random.choice(cupons_db) if usar_cupom else None
            cup_id = cupom[0] if cupom else None

            # Data ponderada ao longo de 400 dias
            dias_passados = random.randint(0, 400)
            hora = random.randint(8, 20)
            minuto = random.randint(0, 59)
            dt_venda = now - timedelta(days=dias_passados, hours=now.hour - hora, minutes=now.minute - minuto)
            
            stat = random.choice(status_choices)
            forma = random.choice(formas_pag)

            # Cesta com 1 a 4 produtos
            itens_n = random.randint(1, 4)
            cesta = random.sample(produtos_db, k=itens_n)
            
            subtotal = 0.0
            itens_dados = []
            for prod in cesta:
                p_id, p_preco, p_est, p_nome = prod
                qtd = random.randint(1, 3)
                p_pr_flt = float(p_preco)
                subtot_it = p_pr_flt * qtd
                subtotal += subtot_it
                itens_dados.append((p_id, qtd, p_pr_flt, 0.0, subtot_it))

            desconto = 0.0
            if cupom and subtotal >= float(cupom[4]):
                if cupom[2] == 'percentual':
                    desconto = round(subtotal * (float(cupom[3]) / 100.0), 2)
                else:
                    desconto = min(float(cupom[3]), round(subtotal * 0.4, 2))
            else:
                cup_id = None

            frete = 5000.00 if l_id == 9 else 0.00
            total_venda = max(100.00, round(subtotal - desconto + frete, 2))

            entrega_json = Json({
                "metodo": "Entrega Expresso Luanda" if l_id == 9 else "Retirada em Loja",
                "tempo_estimado": "24h - 48h",
                "transportadora": "MustaLog Express",
                "rastreio": f"TRK-{i:07d}-AO"
            })

            batch_vendas_data.append((
                cod_venda, c_id, v_id, l_id, cup_id, dt_venda, stat,
                subtotal, desconto, frete, total_venda, forma,
                f"Venda {cod_venda} faturada via PDV/App. Atendente {v_id}.",
                entrega_json
            ))
            batch_itens_map.append((itens_dados, stat, forma, total_venda, dt_venda, cod_venda))

        # Inserir lote de vendas com RETURNING id
        inserted_ids = execute_values(cur, """
            INSERT INTO vendas (codigo_venda, cliente_id, vendedor_id, loja_id, cupom_id, data_venda, status_venda, valor_subtotal, valor_desconto, valor_frete, valor_total, forma_pagamento_predileta, observacoes, dados_entrega)
            VALUES %s RETURNING id;
        """, batch_vendas_data, fetch=True)

        # Montar lotes de itens e pagamentos correspondentes
        batch_itens_insert = []
        batch_pagamentos_insert = []
        batch_estoque_insert = []

        for idx, (venda_id_tuple) in enumerate(inserted_ids):
            venda_id = venda_id_tuple[0]
            itens_dados, stat, forma, total_venda, dt_venda, cod_venda = batch_itens_map[idx]

            for it in itens_dados:
                batch_itens_insert.append((venda_id, it[0], it[1], it[2], it[3], it[4]))

            if stat != 'cancelado' and total_venda > 0:
                pag_stat = 'concluido' if stat in ('entregue', 'pago', 'enviado', 'em_separacao') else 'pendente'
                batch_pagamentos_insert.append((
                    venda_id, forma, total_venda, pag_stat,
                    f"REF-{random.randint(100000000, 999999999)}",
                    dt_venda + timedelta(minutes=random.randint(1, 15)),
                    Json({"gateway": "EMIS Multicaixa", "autorizacao": f"AUTH{venda_id:07d}", "sucesso": True})
                ))

            # Movimentação de estoque para parte das vendas entregues
            if stat == 'entregue' and venda_id <= 2500:
                for it in itens_dados:
                    batch_estoque_insert.append((
                        it[0], 'saida_venda', it[1], 150, 150 - it[1], venda_id,
                        f"Faturamento Venda {cod_venda}", dt_venda
                    ))

        # Inserção em massa dos itens deste lote
        execute_values(cur, """
            INSERT INTO itens_venda (venda_id, produto_id, quantidade, preco_unitario, desconto_unitario, valor_total_item)
            VALUES %s;
        """, batch_itens_insert, page_size=2000)
        total_itens_inseridos += len(batch_itens_insert)

        # Inserção em massa dos pagamentos deste lote
        if batch_pagamentos_insert:
            execute_values(cur, """
                INSERT INTO pagamentos (venda_id, metodo, valor, status, referencia_bancaria, data_pagamento, resposta_gateway)
                VALUES %s;
            """, batch_pagamentos_insert, page_size=2000)
            total_pagamentos_inseridos += len(batch_pagamentos_insert)

        # Inserção de movimentações de estoque
        if batch_estoque_insert:
            execute_values(cur, """
                INSERT INTO movimentacoes_estoque (produto_id, tipo, quantidade, saldo_anterior, saldo_novo, referencia_venda_id, motivo, data_movimento)
                VALUES %s;
            """, batch_estoque_insert, page_size=2000)

        conn.commit()
        print(f"  ✓ Processadas vendas {batch_start:,} até {batch_end:,} (acumulado: {total_itens_inseridos:,} itens)...")

    # ==========================================
    # 9. AVALIAÇÕES DE PRODUTOS (800 avaliações)
    # ==========================================
    print("⭐ Inserindo 800 Avaliações de Produtos...")
    cur.execute("SELECT id FROM produtos;")
    all_prod_ids = [r[0] for r in cur.fetchall()]

    avaliacoes_textos = [
        (5, "Simplesmente espetacular!", "Superou todas as expectativas. Qualidade de acabamento e velocidade de topo."),
        (5, "Recomendo vivamente!", "Excelente produto, chegou muito rápido e funciona perfeitamente no dia a dia."),
        (4, "Muito bom, vale o investimento", "Atende com folga as necessidades. Apenas a embalagem poderia ser um pouco mais reforçada."),
        (5, "Melhor compra do ano", "Produto impecável. A bateria dura bastante e a tela tem cores vibrantes."),
        (4, "Confortável e resistente", "Estou a usar há um mês no escritório e sinto enorme diferença na postura."),
        (5, "Qualidade profissional", "Ideal para ambiente corporativo e trabalho intenso. Robusto e confiável."),
        (3, "Bom, cumpre o básico", "Dentro do esperado para o preço. Sem grandes destaques mas funciona bem."),
        (4, "Ótimo custo-benefício", "Excelente relação entre preço e entrega. Com certeza voltarei a comprar."),
        (5, "Excelente suporte e produto", "Atendimento impecável na loja e o equipamento é de altíssima performance."),
        (4, "Design moderno e prático", "Muito bonito na mesa de trabalho e super funcional.")
    ]

    avaliacoes_list = []
    pares_usados = set()
    for _ in range(800):
        p_id = random.choice(all_prod_ids)
        c_id = random.choice(clientes_ids)
        if (p_id, c_id) in pares_usados:
            continue
        pares_usados.add((p_id, c_id))
        
        txt = random.choice(avaliacoes_textos)
        dt_av = now - timedelta(days=random.randint(2, 300))
        avaliacoes_list.append((p_id, c_id, txt[0], txt[1], txt[2], True, dt_av))

    execute_values(cur, """
        INSERT INTO avaliacoes_produtos (produto_id, cliente_id, nota, titulo, comentario, recomendaria, data_avaliacao)
        VALUES %s ON CONFLICT DO NOTHING;
    """, avaliacoes_list, page_size=1000)

    # ==========================================
    # 10. ATUALIZAÇÃO DOS TOTAIS DE CLIENTES
    # ==========================================
    print("📈 Consolidando total de compras por cliente...")
    cur.execute("""
        UPDATE clientes c
        SET total_compras = COALESCE(sub.total, 0.00)
        FROM (
            SELECT cliente_id, SUM(valor_total) as total
            FROM vendas
            WHERE status_venda != 'cancelado'
            GROUP BY cliente_id
        ) sub
        WHERE c.id = sub.cliente_id;
    """)

    conn.commit()
    conn.close()
    print("✅ Base de dados 'vendas_db' populada com sucesso!")

def refresh_mustainfo_statistics():
    print("📊 Atualizando estatísticas e contadores no MustaInfo...")
    db = SessionLocal()
    try:
        sync_connection_statistics(3, db)
        print("✅ Estatísticas de conexões e tabelas sincronizadas!")
    except Exception as e:
        print(f"⚠️ Aviso ao atualizar estatísticas: {e}")
    finally:
        db.close()

def export_sql_dump():
    print("💾 Exportando script SQL completo para c:\\Users\\tggel\\Documents\\mustainf\\vendas_db.sql...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    output_path = r"c:\Users\tggel\Documents\mustainf\vendas_db.sql"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("""-- ==============================================================================
-- MUSTAINFO - BANCO DE DADOS DE DEMONSTRAÇÃO DE VENDAS (vendas_db)
-- PostgreSQL 14+ / 15+ / 16+
-- Base de dados empresarial com mais de 40.000 registros relacionais.
-- ==============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

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

-- 1. Categorias
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

-- 4. Lojas
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

-- 5. Vendedores
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

-- 7. Cupons
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

-- 8. Vendas
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

-- ==============================================================================
-- INSERÇÃO DOS DADOS
-- ==============================================================================
""")
        tables_order = ['categorias', 'fornecedores', 'lojas', 'vendedores', 'produtos', 'clientes', 'cupons', 'vendas', 'itens_venda', 'pagamentos', 'avaliacoes_produtos', 'movimentacoes_estoque']
        for tbl in tables_order:
            cur.execute(f"SELECT * FROM {tbl};")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            if not rows:
                continue
            f.write(f"\n-- Tabela: {tbl} ({len(rows):,} registros)\n")
            cols_str = ", ".join(colnames)
            for r in rows:
                vals = []
                for v in r:
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, dict):
                        vals.append("'" + json.dumps(v, ensure_ascii=False).replace("'", "''") + "'")
                    else:
                        vals.append("'" + str(v).replace("'", "''") + "'")
                f.write(f"INSERT INTO {tbl} ({cols_str}) VALUES ({', '.join(vals)});\n")

        f.write("\n-- Ajuste das Sequences:\n")
        for tbl in tables_order:
            f.write(f"SELECT setval('{tbl}_id_seq', COALESCE((SELECT MAX(id) FROM {tbl}), 1));\n")

    conn.close()
    print("✅ Exportação do arquivo SQL concluída com sucesso!")

if __name__ == "__main__":
    generate_large_dataset()
    refresh_mustainfo_statistics()
    export_sql_dump()
    print("\n🎉 CONCLUÍDO: Base de dados carregada com volume massivo para demonstração! 🎉")
