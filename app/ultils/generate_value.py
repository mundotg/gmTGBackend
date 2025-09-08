
import random
import re
from typing import Any, Optional
from app.schemas.dbstructure_schema import CampoDetalhado
from faker import Faker
from hypothesis import strategies as st


# Múltiplos locales para dados mais diversos
faker_pt = Faker("pt_PT")
faker_br = Faker("pt_BR")
faker_en = Faker("en_US")


class GeradorDadosInteligente:
    """Gerador de dados mais inteligente com análise contextual de nomes de colunas"""
    
    def __init__(self, locale: str = "pt_PT"):
        self.faker = Faker(locale)
        self._build_patterns()
        self._build_relational_data()
    
    def _build_patterns(self):
        """Constrói padrões regex para identificação inteligente de campos"""
        self.patterns = {
            # IDs e chaves
            'id': re.compile(r'(^id$|.*_id$|pk|primary.*key)', re.IGNORECASE),
            'uuid': re.compile(r'(uuid|guid)', re.IGNORECASE),
            
            # Datas
            'created_at': re.compile(r'(created|criado|dt_cria|data_cria)', re.IGNORECASE),
            'updated_at': re.compile(r'(updated|modificado|alterado|dt_alt|data_alt)', re.IGNORECASE),
            'deleted_at': re.compile(r'(deleted|removido|excluido|dt_del)', re.IGNORECASE),
            'birth_date': re.compile(r'(nascimento|birth|aniversario|dt_nasc)', re.IGNORECASE),
            
            # Dados pessoais
            'email': re.compile(r'(email|mail|correio)', re.IGNORECASE),
            'first_name': re.compile(r'(primeiro.*nome|first.*name|nome_proprio)', re.IGNORECASE),
            'last_name': re.compile(r'(ultimo.*nome|last.*name|sobrenome|apelido)', re.IGNORECASE),
            'full_name': re.compile(r'(^nome$|name$|nome_completo|full.*name)', re.IGNORECASE),
            'username': re.compile(r'(username|usuario|user|login)', re.IGNORECASE),
            
            # Contatos
            'phone': re.compile(r'(telefone|phone|telemovel|celular|mobile)', re.IGNORECASE),
            'mobile': re.compile(r'(celular|mobile|telemovel)', re.IGNORECASE),
            'whatsapp': re.compile(r'(whatsapp|wpp|zap)', re.IGNORECASE),
            
            # Endereços
            'address': re.compile(r'(endereco|address|morada|rua)', re.IGNORECASE),
            'city': re.compile(r'(cidade|city|localidade)', re.IGNORECASE),
            'state': re.compile(r'(estado|state|provincia|distrito)', re.IGNORECASE),
            'postal_code': re.compile(r'(cep|zip|postal|codigo_postal)', re.IGNORECASE),
            'country': re.compile(r'(pais|country|nacao)', re.IGNORECASE),
            
            # Financeiro
            'price': re.compile(r'(preco|price|valor|custo|cost)', re.IGNORECASE),
            'salary': re.compile(r'(salario|salary|vencimento|remuneracao)', re.IGNORECASE),
            'tax': re.compile(r'(imposto|tax|taxa)', re.IGNORECASE),
            'discount': re.compile(r'(desconto|discount|reducao)', re.IGNORECASE),
            
            # Descrições e textos
            'description': re.compile(r'(descricao|description|detalhes|observacoes|obs)', re.IGNORECASE),
            'title': re.compile(r'(titulo|title|nome|assunto)', re.IGNORECASE),
            'comment': re.compile(r'(comentario|comment|nota|observacao)', re.IGNORECASE),
            
            # Status e flags
            'status': re.compile(r'(status|estado|situacao)', re.IGNORECASE),
            'active': re.compile(r'(ativo|active|habilitado|enabled)', re.IGNORECASE),
            'confirmed': re.compile(r'(confirmado|confirmed|verificado|verified)', re.IGNORECASE),
            
            # URLs e links
            'url': re.compile(r'(url|link|website|site)', re.IGNORECASE),
            'image': re.compile(r'(imagem|image|foto|picture|avatar)', re.IGNORECASE),
            
            # Códigos
            'code': re.compile(r'(codigo|code|referencia|ref)', re.IGNORECASE),
            'barcode': re.compile(r'(codigo.*barras|barcode|ean)', re.IGNORECASE),
        }
    
    def _build_relational_data(self):
        """Dados relacionais para manter consistência"""
        self.relational_data = {
            'countries': ['Portugal', 'Brasil', 'Espanha', 'França', 'Alemanha'],
            'cities_pt': ['Lisboa', 'Porto', 'Coimbra', 'Braga', 'Aveiro'],
            'cities_br': ['São Paulo', 'Rio de Janeiro', 'Brasília', 'Salvador', 'Fortaleza'],
            'status_options': ['ativo', 'inativo', 'pendente', 'cancelado', 'concluído'],
            'priority_options': ['baixa', 'média', 'alta', 'crítica'],
        }
    
    def _detect_field_type(self, nome: str) -> Optional[str]:
        """Detecta o tipo de campo baseado no nome usando regex"""
        for field_type, pattern in self.patterns.items():
            if pattern.search(nome):
                return field_type
        return None
    
    def _get_smart_range_for_numeric(self, nome: str, tipo: str) -> tuple:
        """Retorna ranges inteligentes baseados no contexto do campo"""
        nome_lower = nome.lower()
        
        # IDs: sempre positivos, geralmente pequenos para testes
        if 'id' in nome_lower or nome_lower in ['pk']:
            return (1, 1000)
        
        # Idades
        if any(word in nome_lower for word in ['idade', 'age', 'anos']):
            return (18, 80)
        
        # Preços e valores monetários
        if any(word in nome_lower for word in ['preco', 'price', 'valor', 'custo', 'salario']):
            return (10, 50000)
        
        # Quantidades
        if any(word in nome_lower for word in ['quantidade', 'qty', 'stock', 'estoque']):
            return (0, 1000)
        
        # Percentagens
        if any(word in nome_lower for word in ['percent', 'taxa', 'desconto']):
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
        if any(word in nome_lower for word in ['codigo', 'code', 'sigla', 'abrev']):
            return min(10, max_length)
        
        # Nomes
        if any(word in nome_lower for word in ['nome', 'name', 'titulo', 'title']):
            return min(50, max_length)
        
        # Descrições
        if any(word in nome_lower for word in ['descricao', 'description', 'obs', 'comment']):
            return min(200, max_length)
        
        # URLs
        if any(word in nome_lower for word in ['url', 'link', 'website']):
            return min(100, max_length)
        
        return min(30, max_length)

    def gerar_valor_inteligente(self, coluna: CampoDetalhado, como_strategy: bool = False) -> Any:
        """
        Gera valores inteligentes baseados em análise contextual do nome da coluna
        """
        nome = coluna.nome.lower()
        tipo = coluna.tipo.lower()
        
        def faker_strategy(func):
            return st.builds(func) if como_strategy else func()
        
        # Detecta o tipo de campo baseado no nome
        field_type = self._detect_field_type(nome)
        
        # Geração baseada no tipo detectado
        if field_type == 'id':
            if 'uuid' in nome:
                return faker_strategy(lambda: str(self.faker.uuid4()))
            else:
                min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
                return st.integers(min_value=min_val, max_value=max_val) if como_strategy else random.randint(min_val, max_val)
        
        elif field_type == 'uuid':
            return faker_strategy(lambda: str(self.faker.uuid4()))
        
        elif field_type in ['created_at', 'updated_at', 'deleted_at']:
            if field_type == 'updated_at':
                # Updated_at geralmente é mais recente
                start_date = "-30d"
            elif field_type == 'created_at':
                start_date = "-2y"
            else:  # deleted_at
                start_date = "-1y"
            
            return faker_strategy(lambda: self.faker.date_time_between(start_date=start_date, end_date="now"))
        
        elif field_type == 'birth_date':
            return faker_strategy(lambda: self.faker.date_of_birth(minimum_age=18, maximum_age=80))
        
        elif field_type == 'email':
            return faker_strategy(lambda: self.faker.email())
        
        elif field_type == 'first_name':
            return faker_strategy(lambda: self.faker.first_name())
        
        elif field_type == 'last_name':
            return faker_strategy(lambda: self.faker.last_name())
        
        elif field_type == 'full_name':
            return faker_strategy(lambda: self.faker.name())
        
        elif field_type == 'username':
            return faker_strategy(lambda: self.faker.user_name())
        
        elif field_type in ['phone', 'mobile', 'whatsapp']:
            return faker_strategy(lambda: self.faker.phone_number())
        
        elif field_type == 'address':
            return faker_strategy(lambda: self.faker.address().replace('\n', ', '))
        
        elif field_type == 'city':
            cities = self.relational_data['cities_pt'] + self.relational_data['cities_br']
            return faker_strategy(lambda: random.choice(cities))
        
        elif field_type == 'state':
            return faker_strategy(lambda: self.faker.state())
        
        elif field_type == 'postal_code':
            return faker_strategy(lambda: self.faker.postcode())
        
        elif field_type == 'country':
            return faker_strategy(lambda: random.choice(self.relational_data['countries']))
        
        elif field_type in ['price', 'salary', 'tax']:
            min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
            if tipo in ["decimal", "numeric", "float", "double precision", "real"]:
                return st.floats(min_value=min_val, max_value=max_val, allow_nan=False, allow_infinity=False) if como_strategy \
                    else round(random.uniform(min_val, max_val), 2)
            else:
                return st.integers(min_value=min_val, max_value=max_val) if como_strategy else random.randint(min_val, max_val)
        
        elif field_type == 'description':
            length = self._get_smart_text_length(nome, coluna.length)
            return faker_strategy(lambda: self.faker.text(max_nb_chars=length))
        
        elif field_type == 'title':
            return faker_strategy(lambda: self.faker.sentence(nb_words=4).rstrip('.'))
        
        elif field_type == 'comment':
            return faker_strategy(lambda: self.faker.sentence(nb_words=random.randint(5, 15)))
        
        elif field_type == 'status':
            return faker_strategy(lambda: random.choice(self.relational_data['status_options']))
        
        elif field_type in ['active', 'confirmed']:
            return st.booleans() if como_strategy else random.choice([True, False])
        
        elif field_type == 'url':
            return faker_strategy(lambda: self.faker.url())
        
        elif field_type == 'image':
            return faker_strategy(lambda: f"https://picsum.photos/{random.randint(200, 800)}/{random.randint(200, 600)}")
        
        elif field_type == 'code':
            return faker_strategy(lambda: self.faker.bothify(text='??###'))
        
        elif field_type == 'barcode':
            return faker_strategy(lambda: self.faker.ean13())
        
        # Fallback para tipos de dados padrão
        return self._generate_by_data_type(coluna, como_strategy)
    
    def _generate_by_data_type(self, coluna: CampoDetalhado, como_strategy: bool = False) -> Any:
        """Geração baseada no tipo de dados quando não há padrão específico detectado"""
        nome = coluna.nome.lower()
        tipo = coluna.tipo.lower()
        
        def faker_strategy(func):
            return st.builds(func) if como_strategy else func()
        
        if tipo in ["bool", "boolean"]:
            return st.booleans() if como_strategy else random.choice([True, False])
        
        if tipo in ["int", "integer", "bigint", "smallint"]:
            min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
            return st.integers(min_value=min_val, max_value=max_val) if como_strategy else random.randint(min_val, max_val)
        
        if tipo in ["decimal", "numeric", "float", "double precision", "real"]:
            min_val, max_val = self._get_smart_range_for_numeric(nome, tipo)
            return st.floats(min_value=min_val, max_value=max_val, allow_nan=False, allow_infinity=False) if como_strategy \
                else round(random.uniform(min_val, max_val), 2)
        
        if tipo in ["varchar", "character varying", "text", "char"]:
            tamanho = self._get_smart_text_length(nome, coluna.length)
            return faker_strategy(lambda: self.faker.text(max_nb_chars=tamanho)[:tamanho])
        
        if tipo in ["date"]:
            return faker_strategy(lambda: self.faker.date_between(start_date="-5y", end_date="today"))
        
        if tipo in ["timestamp", "timestamptz", "datetime"]:
            return faker_strategy(lambda: self.faker.date_time_between(start_date="-2y", end_date="now"))
        
        # Fallback final
        return faker_strategy(lambda: f"auto_{self.faker.word()}")


# Função principal para manter compatibilidade com código existente
def gerar_valor_pelo_tipo_de_dados_na_bd(coluna: CampoDetalhado, como_strategy: bool = False):
    """
    Versão melhorada da função original com geração inteligente de dados
    """
    gerador = GeradorDadosInteligente()
    return gerador.gerar_valor_inteligente(coluna, como_strategy)


# # Exemplo de uso
# if __name__ == "__main__":
#     # Exemplo de como usar
#     class ExampleColumn:
#         def __init__(self, nome, tipo, length=None):
#             self.nome = nome
#             self.tipo = tipo
#             self.length = length
    
#     gerador = GeradorDadosInteligente()
    
#     # Exemplos de colunas
#     colunas_exemplo = [
#         ExampleColumn("user_id", "integer"),
#         ExampleColumn("email_usuario", "varchar", 100),
#         ExampleColumn("data_nascimento", "date"),
#         ExampleColumn("preco_produto", "decimal"),
#         ExampleColumn("descricao_item", "text", 500),
#         ExampleColumn("status_pedido", "varchar", 20),
#         ExampleColumn("telefone_contato", "varchar", 20),
#     ]
    
#     print("🔹 Exemplos de dados gerados:")
#     for coluna in colunas_exemplo:
#         valor = gerador.gerar_valor_inteligente(coluna)
#         print(f"{coluna.nome} ({coluna.tipo}): {valor}")