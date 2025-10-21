import random
import re
from typing import Any, Optional
from app.config.dotenv import get_faker_config, get_generator_config
from app.schemas.dbstructure_schema import CampoDetalhado
from faker import Faker
from hypothesis import strategies as st
import logging
from functools import lru_cache
from threading import Lock

# Configuração de logging
logger = logging.getLogger(__name__)

# Múltiplos locales para dados mais diversos
faker_pt = Faker("pt_PT")
faker_br = Faker("pt_BR")
faker_en = Faker("en_US")


class GeradorDadosInteligente:
    """Gerador de dados mais inteligente com análise contextual de nomes de colunas"""

    def __init__(self, locale: str = "pt_PT"):
        gen_config = get_generator_config()
        faker_config = get_faker_config()
        
        self.locale = gen_config['default_locale']
        self.max_retries = gen_config['max_retries']
        self.null_probability = gen_config['null_probability']
        
        # Configura Faker com seed se especificado
        if faker_config['seed'] is not None:
            self.faker.seed(faker_config['seed'])
        self._cache_lock = Lock()
        self._cache = {}
        self._relational_cache = {}
        self._consistent_data = {}
        self._unique_values_cache = {}
        
        try:
            self.faker = Faker( gen_config['default_locale'])
            self._build_patterns()
            self._build_relational_data()
        except Exception as e:
            logger.error(f"Erro ao inicializar gerador com locale {locale}: {e}")
            # Fallback para locale padrão
            self.faker = Faker("pt_PT")

    def _build_patterns(self):
        """Constrói padrões regex para identificação inteligente de campos"""
        base_flags = re.IGNORECASE
        self.patterns = {
            # Identificadores
            "id": re.compile(r"(^id$|.*_id$|pk|primary.*key)", base_flags),
            "uuid": re.compile(r"\b(uuid|guid)\b", base_flags),
            # Datas
            "created_at": re.compile(
                r"(created|criado|dt.?cria|data.?cria)", base_flags
            ),
            "updated_at": re.compile(
                r"(updated|modificado|alterado|dt.?alt|data.?alt)", base_flags
            ),
            "deleted_at": re.compile(
                r"(deleted|removido|excluido|dt.?del)", base_flags
            ),
            "birth_date": re.compile(
                r"(nascimento|birth|aniversario|dt.?nasc)", base_flags
            ),
            # Dados pessoais
            "email": re.compile(r"(email|mail|correio)", base_flags),
            "first_name": re.compile(
                r"(primeiro.?nome|first.?name|nome.?proprio)", base_flags
            ),
            "last_name": re.compile(
                r"(ultimo.?nome|last.?name|sobrenome|apelido)", base_flags
            ),
            "full_name": re.compile(
                r"(^nome$|^name$|nome.?completo|full.?name)", base_flags
            ),
            "generic_name": re.compile(r"(nome|name)", base_flags),  # <-- NOVO
            "username": re.compile(r"(username|usuario|user|login)", base_flags),
            # Contato
            "phone": re.compile(
                r"(telefone|phone|telemovel|celular|mobile)", base_flags
            ),
            "whatsapp": re.compile(r"(whatsapp|wpp|zap)", base_flags),
            # Endereço
            "address": re.compile(r"(endereco|address|morada|rua)", base_flags),
            "city": re.compile(r"(cidade|city|localidade)", base_flags),
            "state": re.compile(r"(estado|state|provincia|distrito)", base_flags),
            "postal_code": re.compile(r"(cep|zip|postal|codigo.?postal)", base_flags),
            "country": re.compile(r"(pais|country|nacao)", base_flags),
            # Financeiro
            "price": re.compile(r"(preco|price|valor|custo|cost)", base_flags),
            "salary": re.compile(
                r"(salario|salary|vencimento|remuneracao)", base_flags
            ),
            "tax": re.compile(r"(imposto|tax|taxa)", base_flags),
            "discount": re.compile(r"(desconto|discount|reducao)", base_flags),
            # Texto e conteúdo
            "description": re.compile(
                r"(descricao|description|detalhes|observacoes|obs)", base_flags
            ),
            "title": re.compile(r"(titulo|title|assunto)", base_flags),
            "comment": re.compile(r"(comentario|comment|nota|observacao)", base_flags),
            # Status e controle
            "status": re.compile(r"(status|estado|situacao)", base_flags),
            "active": re.compile(r"(ativo|active|habilitado|enabled)", base_flags),
            "confirmed": re.compile(
                r"(confirmado|confirmed|verificado|verified)", base_flags
            ),
            # Links e imagens
            "url": re.compile(r"(url|link|website|site)", base_flags),
            "image": re.compile(r"(imagem|image|foto|picture|avatar)", base_flags),
            # Códigos
            "code": re.compile(r"(codigo|code|referencia|ref)", base_flags),
            "barcode": re.compile(r"(codigo.*barras|barcode|ean)", base_flags),
        }

    def _build_relational_data(self):
        """Dados relacionais para manter consistência"""
        self.relational_data = {
            "countries": ["Portugal", "Brasil", "Espanha", "França", "Alemanha"],
            "cities_pt": ["Lisboa", "Porto", "Coimbra", "Braga", "Aveiro"],
            "cities_br": [
                "São Paulo",
                "Rio de Janeiro",
                "Brasília",
                "Salvador",
                "Fortaleza",
            ],
            "status_options": [
                "ativo",
                "inativo",
                "pendente",
                "cancelado",
                "concluído",
            ],
            "priority_options": ["baixa", "média", "alta", "crítica"],
            "departments": ["TI", "RH", "Financeiro", "Marketing", "Vendas"],
            "product_categories": ["Eletrônicos", "Vestuário", "Alimentação", "Casa", "Esportes"],
            "order_status": ["pendente", "processando", "enviado", "entregue", "cancelado"],
        }

    @lru_cache(maxsize=1000)
    def _detect_field_type_cached(self, nome: str) -> Optional[str]:
        """Versão em cache da detecção de tipo de campo"""
        return self._detect_field_type(nome)

    def _detect_field_type(self, nome: str) -> Optional[str]:
        """Detecta o tipo de campo baseado no nome usando regex"""
        nome = nome.strip().lower()
        for field_type, pattern in self.patterns.items():
            if pattern.search(nome):
                return field_type
        return None

    def _get_smart_range_for_numeric(self, nome: str, tipo: str) -> tuple:
        """Retorna ranges inteligentes baseados no contexto do campo"""
        nome_lower = nome.lower()

        # IDs: sempre positivos, geralmente pequenos para testes
        if "id" in nome_lower or nome_lower in ["pk"]:
            return (1, 1000)

        # Idades
        if any(word in nome_lower for word in ["idade", "age", "anos"]):
            return (18, 80)

        # Preços e valores monetários
        if any(
            word in nome_lower
            for word in ["preco", "price", "valor", "custo", "salario"]
        ):
            return (10, 50000)

        # Quantidades
        if any(
            word in nome_lower for word in ["quantidade", "qty", "stock", "estoque"]
        ):
            return (0, 1000)

        # Percentagens
        if any(word in nome_lower for word in ["percent", "taxa", "desconto"]):
            return (0, 100)

        # Default
        if tipo in ["smallint"]:
            return (1, 32767)
        elif tipo in ["int", "integer"]:
            return (1, 100000)
        else:
            return (1, 999999)

    def _get_smart_text_length(self, nome: str, length: Optional[int]) -> int:
        """Determina tamanho inteligente para campos de texto"""
        nome_lower = nome.lower()

        # Se length é especificado, usa ele como máximo
        max_length = length if length else 255

        # Campos curtos
        if any(word in nome_lower for word in ["codigo", "code", "sigla", "abrev"]):
            return min(10, max_length)

        # Nomes
        if any(word in nome_lower for word in ["nome", "name", "titulo", "title"]):
            return min(50, max_length)

        # Descrições
        if any(
            word in nome_lower
            for word in ["descricao", "description", "obs", "comment"]
        ):
            return min(200, max_length)

        # URLs
        if any(word in nome_lower for word in ["url", "link", "website"]):
            return min(100, max_length)

        return min(30, max_length)

    def _gerar_nome_generico(self, tabela_name: str, coluna_name: str | None = None):
        """
        Gera nomes genéricos inteligentes com base no contexto semântico da tabela e/ou da coluna.
        """
        tabela_lower = (tabela_name or "").lower()
        coluna_lower = (coluna_name or "").lower()

        # 🔹 Subcategorias de contexto
        subcategorias = {
            "cliente": lambda: self.faker.name(),
            "user": lambda: self.faker.user_name(),
            "usuario": lambda: self.faker.user_name(),
            "empresa": lambda: self.faker.company(),
            "compania": lambda: self.faker.company(),
            "fornecedor": lambda: self.faker.company(),
            "produto": lambda: self.faker.word(),
            "item": lambda: self.faker.word(),
            "servico": lambda: self.faker.catch_phrase(),
            "projeto": lambda: self.faker.bs().title(),
            "categoria": lambda: random.choice(
                ["Eletrônicos", "Roupas", "Comida", "Serviços", "Outros"]
            ),
            "curso": lambda: f"Curso de {self.faker.job()}",
            "filme": lambda: self.faker.sentence(nb_words=3).rstrip("."),
            "faculdade": lambda: f"Universidade de {self.faker.city()}",
            "escola": lambda: f"Escola {self.faker.last_name()}",
            "profissao": lambda: self.faker.job(),
            "cargo": lambda: self.faker.job(),
            "funcionario": lambda: self.faker.name(),
            "colaborador": lambda: self.faker.name(),
            "membro": lambda: self.faker.name(),
            "participante": lambda: self.faker.name(),
            "aluno": lambda: self.faker.name(),
            "vendedor": lambda: self.faker.name(),
            "representante": lambda: self.faker.name(),
            "marca": lambda: self.faker.company(),
            "pessoa": lambda: self.faker.name(),
            "departamento": lambda: self.faker.job(),
            "funcao": lambda: self.faker.job(),
            "setor": lambda: self.faker.job(),
            "modelo": lambda: f"Modelo {random.randint(100, 999)}",
            "pais": lambda: self.faker.country(),
            "cidade": lambda: self.faker.city(),
            "regiao": lambda: self.faker.state(),
            "area": lambda: random.choice(
                ["Administração", "Engenharia", "Marketing", "TI", "Vendas"]
            ),
        }

        # 🔹 Verifica primeiro pelo nome da tabela
        for chave, gerador in subcategorias.items():
            if chave in tabela_lower:
                return gerador()

        # 🔹 Se nada encontrado, tenta pelo nome da coluna
        if coluna_name:
            for chave, gerador in subcategorias.items():
                if chave in coluna_lower:
                    return gerador()

        # 🔹 Fallback: gera um nome genérico
        return self.faker.word().title()

    def _aplicar_constraints(self, coluna: CampoDetalhado, valor: Any) -> Any:
        """Aplica constraints como unique, not null, etc."""
        # Campo não nulo
        if getattr(coluna, 'not_null', True) and valor is None:
            return self._gerar_valor_nao_nulo(coluna)
        
        # Validar tamanho
        valor = self._validar_tamanho(coluna, valor)
        
        return valor

    def _validar_tamanho(self, coluna: CampoDetalhado, valor: Any) -> Any:
        """Valida e ajusta tamanho do valor gerado"""
        if not valor or not hasattr(valor, '__len__'):
            return valor
        
        max_length = getattr(coluna, 'length', None)
        if max_length and len(str(valor)) > max_length:
            return str(valor)[:max_length]
        
        return valor

    def _gerar_valor_nao_nulo(self, coluna: CampoDetalhado) -> Any:
        """Gera um valor não nulo para a coluna"""
        # Tenta gerar novamente com fallback seguro
        for _ in range(3):  # Tenta 3 vezes
            valor = self._gerar_valor_seguro(coluna)
            if valor is not None:
                return valor
        return self._valor_fallback(coluna)

    def _gerar_valor_seguro(self, coluna: CampoDetalhado, **kwargs) -> Any:
        """Geração segura com tratamento de erro"""
        try:
            return self.gerar_valor_inteligente(coluna, **kwargs)
        except Exception as e:
            logger.warning(f"Erro ao gerar valor para {coluna.nome}: {e}")
            return self._valor_fallback(coluna)

    def _valor_fallback(self, coluna: CampoDetalhado) -> Any:
        """Valor fallback quando a geração falha"""
        tipo = coluna.tipo.lower()
        
        if tipo in ['varchar', 'text', 'char']:
            return f"fallback_{self.faker.word()}"
        elif tipo in ['int', 'integer', 'bigint']:
            return 1
        elif tipo in ['bool', 'boolean']:
            return True
        elif 'date' in tipo or 'time' in tipo:
            return "2023-01-01"
        else:
            return "fallback_value"

    def gerar_dados_relacionais(self, tabela: str, coluna: str, valor_chave: Any = None):
        """Gera dados que mantêm consistência relacional"""
        cache_key = f"{tabela}.{coluna}"
        
        if cache_key not in self._relational_cache:
            self._relational_cache[cache_key] = []
        
        # Reutiliza valores existentes para manter consistência
        if (valor_chave and valor_chave in self._consistent_data.get(cache_key, {}) and
            random.random() < 0.7):  # 70% de chance de reusar
            return self._consistent_data[cache_key][valor_chave]
        
        novo_valor = self._gerar_novo_valor_relacional(tabela, coluna)
        self._relational_cache[cache_key].append(novo_valor)
        
        if valor_chave:
            if cache_key not in self._consistent_data:
                self._consistent_data[cache_key] = {}
            self._consistent_data[cache_key][valor_chave] = novo_valor
        
        return novo_valor

    def _gerar_novo_valor_relacional(self, tabela: str, coluna: str) -> Any:
        """Gera novo valor para dados relacionais"""
        # Lógica específica para geração relacional
        if "cidade" in coluna.lower():
            return random.choice(self.relational_data.get("cities_pt", []))
        elif "departamento" in coluna.lower():
            return random.choice(self.relational_data.get("departments", []))
        else:
            return self.faker.word()

    def gerar_lote_dados(self, colunas: list, quantidade: int) -> list[dict]:
        """Gera um lote de dados consistentes"""
        return [self.gerar_linha_dados(colunas) for _ in range(quantidade)]

    def gerar_linha_dados(self, colunas: list) -> dict:
        """Gera uma linha completa de dados mantendo consistência"""
        linha = {}
        for coluna in colunas:
            try:
                linha[coluna.nome] = self.gerar_valor_inteligente(coluna)
            except Exception as e:
                logger.warning(f"Erro ao gerar valor para {coluna.nome}: {e}")
                linha[coluna.nome] = self._valor_fallback(coluna)
        return linha

    def gerar_valor_inteligente(
        self, coluna: CampoDetalhado, como_strategy: bool = False, tabela_name: str = ""
    ) -> Any:
        """
        Gera valores inteligentes com base no contexto semântico do nome e tipo da coluna.
        Utiliza faker + heurísticas inteligentes de domínio.
        """
        # Verificação de segurança
        if not coluna or not coluna.nome:
            return self._valor_fallback(coluna)

        try:
            nome = coluna.nome.lower().strip()
            tipo = coluna.tipo.lower().strip()
            field_type = self._detect_field_type_cached(nome)

            def faker_strategy(func):
                """Permite compatibilidade entre execução direta e modo strategy"""
                return st.builds(func) if como_strategy else func()

            # 🔹 IDs e chaves
            if field_type == "id":
                if "uuid" in nome:
                    return faker_strategy(lambda: str(self.faker.uuid4()))
                else:
                    min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
                    return (
                        st.integers(min_value=min_val, max_value=max_val)
                        if como_strategy
                        else random.randint(min_val, max_val)
                    )

            elif field_type == "uuid":
                return faker_strategy(lambda: str(self.faker.uuid4()))

            # 🔹 Datas
            elif field_type in ["created_at", "updated_at", "deleted_at"]:
                date_ranges = {
                    "created_at": "-3y",
                    "updated_at": "-60d",
                    "deleted_at": "-1y",
                }
                start_date = date_ranges.get(field_type, "-1y")
                return faker_strategy(
                    lambda: self.faker.date_time_between(
                        start_date=start_date, end_date="now"
                    )
                )

            elif field_type == "birth_date":
                return faker_strategy(
                    lambda: self.faker.date_of_birth(minimum_age=18, maximum_age=80)
                )

            # 🔹 Identificação e contato
            elif field_type == "email":
                return faker_strategy(lambda: self.faker.unique.email())

            elif field_type == "first_name":
                return faker_strategy(lambda: self.faker.first_name())

            elif field_type == "last_name":
                return faker_strategy(lambda: self.faker.last_name())

            elif field_type == "full_name":
                return faker_strategy(lambda: self.faker.name())

            elif field_type == "username":
                return faker_strategy(lambda: self.faker.user_name())

            elif field_type in ["phone", "mobile", "whatsapp"]:
                return faker_strategy(lambda: self.faker.phone_number())
            elif field_type == "generic_name":
                return faker_strategy(lambda: self._gerar_nome_generico(tabela_name, coluna.nome))

            # 🔹 Endereço
            elif field_type == "address":
                return faker_strategy(lambda: self.faker.address().replace("\n", ", "))

            elif field_type == "city":
                cidades = self.relational_data.get(
                    "cities_pt", []
                ) + self.relational_data.get("cities_br", [])
                return faker_strategy(lambda: random.choice(cidades or [self.faker.city()]))

            elif field_type == "state":
                return faker_strategy(lambda: self.faker.state())

            elif field_type == "postal_code":
                return faker_strategy(lambda: self.faker.postcode())

            elif field_type == "country":
                return faker_strategy(
                    lambda: random.choice(
                        self.relational_data.get("countries", [self.faker.country()])
                    )
                )

            # 🔹 Financeiro
            elif field_type in ["price", "salary", "tax", "discount"]:
                min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
                if tipo in ["decimal", "numeric", "float", "double precision", "real"]:
                    return (
                        st.floats(
                            min_value=min_val,
                            max_value=max_val,
                            allow_nan=False,
                            allow_infinity=False,
                        )
                        if como_strategy
                        else round(random.uniform(min_val, max_val), 2)
                    )
                return (
                    st.integers(min_value=min_val, max_value=max_val)
                    if como_strategy
                    else random.randint(min_val, max_val)
                )

            # 🔹 Texto e descrição
            elif field_type in ["description", "comment"]:
                length = self._get_smart_text_length(nome, coluna.length)
                return faker_strategy(
                    lambda: self.faker.paragraph(nb_sentences=random.randint(2, 5))[:length]
                )

            elif field_type == "title":
                return faker_strategy(lambda: self.faker.sentence(nb_words=3).rstrip("."))

            # 🔹 Status e flags
            elif field_type == "status":
                opcoes = self.relational_data.get(
                    "status_options", ["active", "inactive", "pending"]
                )
                return faker_strategy(lambda: random.choice(opcoes))

            elif field_type in ["active", "confirmed"]:
                # Chance mais alta de True
                if como_strategy:
                    return st.booleans()
                return random.choices([True, False], weights=[0.8, 0.2])[0]

            # 🔹 URLs e mídias
            elif field_type == "url":
                return faker_strategy(lambda: self.faker.url())

            elif field_type == "image":
                return faker_strategy(
                    lambda: f"https://picsum.photos/{random.randint(200, 800)}/{random.randint(200, 600)}"
                )

            # 🔹 Códigos
            elif field_type == "code":
                return faker_strategy(lambda: self.faker.bothify(text="??###").upper())

            elif field_type == "barcode":
                return faker_strategy(lambda: self.faker.ean13())

            # 🔹 Fallback inteligente
            valor = self._generate_by_data_type(coluna, como_strategy)
            return self._aplicar_constraints(coluna, valor)

        except Exception as e:
            logger.warning(f"Erro na geração inteligente para {coluna.nome}: {e}")
            return self._valor_fallback(coluna)

    def _generate_by_data_type(
        self, coluna: CampoDetalhado, como_strategy: bool = False
    ) -> Any:
        """Geração baseada no tipo de dados ou no nome da coluna quando não há padrão específico detectado"""
        nome = coluna.nome.lower()
        tipo = coluna.tipo.lower()

        # Detecta se o nome indica campo numérico
        nome_indica_numerico = any(
            palavra in nome for palavra in ["num", "numero", "number", "quantidade"]
        )

        def faker_strategy(func):
            return st.builds(func) if como_strategy else func()

        # Booleanos
        if tipo in ["bool", "boolean"]:
            return st.booleans() if como_strategy else random.choice([True, False])

        # Inteiros e campos detectados como numéricos
        if tipo in ["int", "integer", "bigint", "smallint"] or nome_indica_numerico:
            min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
            return (
                st.integers(min_value=min_val, max_value=max_val)
                if como_strategy
                else random.randint(min_val, max_val)
            )

        # Decimais e numéricos de ponto flutuante
        if tipo in ["decimal", "numeric", "float", "double precision", "real"]:
            min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
            return (
                st.floats(
                    min_value=min_val,
                    max_value=max_val,
                    allow_nan=False,
                    allow_infinity=False,
                )
                if como_strategy
                else round(random.uniform(min_val, max_val), 2)
            )

        # Texto
        if tipo in ["varchar", "character varying", "text", "char"]:
            tamanho = self._get_smart_text_length(nome, coluna.length)
            return faker_strategy(
                lambda: self.faker.text(max_nb_chars=tamanho)[:tamanho]
            )

        # Datas simples
        if tipo == "date":
            return faker_strategy(
                lambda: self.faker.date_between(start_date="-5y", end_date="today")
            )

        # Datas com hora
        if tipo in ["timestamp", "timestamptz", "datetime"]:
            return faker_strategy(
                lambda: self.faker.date_time_between(start_date="-2y", end_date="now")
            )

        # Fallback final
        return faker_strategy(lambda: f"auto_{self.faker.word()}")


# Função principal para manter compatibilidade com código existente
def gerar_valor_pelo_tipo_de_dados_na_bd(
    coluna: CampoDetalhado, como_strategy: bool = False, tabela_name: str = ""
):
    """
    Versão melhorada da função original com geração inteligente de dados
    """
    gerador = GeradorDadosInteligente()
    return gerador.gerar_valor_inteligente(coluna, como_strategy, tabela_name)