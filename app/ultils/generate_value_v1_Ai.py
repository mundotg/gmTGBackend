import random
import re
from typing import Any, Optional
from app.schemas.dbstructure_schema import CampoDetalhado
from transformers import pipeline
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class GeradorDadosInteligente:
    """
    Versão com Transformer (NLP) para gerar dados mais realistas
    baseado no contexto do nome da coluna
    """

    def __init__(self):
        try:
            # Modelo leve de geração de texto
            self.generator = pipeline("text-generation", model="gpt2")
            self._build_patterns()
        except Exception as e:
            logger.error(f"Erro ao carregar modelo Transformer: {e}")
            self.generator = None

    def _build_patterns(self):
        base_flags = re.IGNORECASE
        self.patterns = {
            "email": re.compile(r"(email|mail)", base_flags),
            "name": re.compile(r"(nome|name)", base_flags),
            "address": re.compile(r"(endereco|address)", base_flags),
            "company": re.compile(r"(empresa|company)", base_flags),
            "text": re.compile(r"(descricao|description|obs)", base_flags),
        }

    @lru_cache(maxsize=500)
    def _detect_field_type_cached(self, nome: str) -> Optional[str]:
        return self._detect_field_type(nome)

    def _detect_field_type(self, nome: str) -> Optional[str]:
        nome = nome.lower()
        for field_type, pattern in self.patterns.items():
            if pattern.search(nome):
                return field_type
        return None

    def _gerar_com_transformer(self, prompt: str, max_length: int = 30) -> str:
        if not self.generator:
            return f"fallback_{random.randint(1, 999)}"

        try:
            result = self.generator(
                prompt,
                max_length=max_length,
                num_return_sequences=1,
                truncation=True
            )
            texto = result[0]["generated_text"]
            return texto.replace(prompt, "").strip()
        except Exception as e:
            logger.warning(f"Erro no transformer: {e}")
            return f"fallback_{random.randint(1, 999)}"

    def gerar_valor_inteligente(
        self,
        coluna: CampoDetalhado,
        como_strategy: bool = False,
        tabela_name: str = ""
    ) -> Any:

        if not coluna or not coluna.nome:
            return self._valor_fallback(coluna)

        nome = coluna.nome.lower()
        tipo = coluna.tipo.lower()

        field_type = self._detect_field_type_cached(nome)

        # 🔹 EMAIL
        if field_type == "email":
            return self._gerar_com_transformer(
                "Generate a realistic email:"
            )

        # 🔹 NOME
        elif field_type == "name":
            return self._gerar_com_transformer(
                "Generate a full name:"
            )

        # 🔹 EMPRESA
        elif field_type == "company":
            return self._gerar_com_transformer(
                "Generate a company name:"
            )

        # 🔹 ENDEREÇO
        elif field_type == "address":
            return self._gerar_com_transformer(
                "Generate an address:"
            )

        # 🔹 TEXTO
        elif field_type == "text":
            return self._gerar_com_transformer(
                f"Write a short description about {tabela_name}:",
                max_length=60
            )

        # 🔹 NUMÉRICOS
        elif tipo in ["int", "integer"]:
            return random.randint(1, 1000)

        elif tipo in ["float", "decimal"]:
            return round(random.uniform(1, 1000), 2)

        # 🔹 BOOLEAN
        elif tipo in ["bool", "boolean"]:
            return random.choice([True, False])

        # 🔹 DATA
        elif "date" in tipo:
            return "2024-01-01"

        # 🔹 TEXTO GENÉRICO COM IA
        return self._gerar_com_transformer(
            f"Generate a value for column {coluna.nome} in table {tabela_name}:"
        )

    def _valor_fallback(self, coluna: CampoDetalhado) -> Any:
        return "fallback_value"

    def gerar_linha_dados(self, colunas: list) -> dict:
        linha = {}
        for coluna in colunas:
            linha[coluna.nome] = self.gerar_valor_inteligente(coluna)
        return linha

    def gerar_lote_dados(self, colunas: list, quantidade: int) -> list:
        return [self.gerar_linha_dados(colunas) for _ in range(quantidade)]


# 🔹 Mantém compatibilidade com teu código atual
def gerar_valor_pelo_tipo_de_dados_na_bd(
    coluna: CampoDetalhado,
    como_strategy: bool = False,
    tabela_name: str = ""
):
    gerador = GeradorDadosInteligente()
    return gerador.gerar_valor_inteligente(coluna, como_strategy, tabela_name)