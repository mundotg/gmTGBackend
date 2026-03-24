import re
from typing import Optional, List, Dict, Any


def extract_angolan_id_data(
    ocr_text: str, formulario: Optional[List[Dict[str, Any]]] = None
) -> dict:
    """
    Analisa o texto do OCR e tenta extrair os dados específicos
    de um Bilhete de Identidade de Angola usando Regex e Heurísticas.
    Melhorado com tolerância a falhas comuns de OCR.
    """
    dados_brutos = {}

    # 1. Número do BI (Tolera espaços acidentais ex: 006308665 UE 047)
    bi_match = re.search(r"(\d{9}\s*[A-Za-z]{2}\s*\d{3})", ocr_text, re.IGNORECASE)
    if bi_match:
        # Removemos eventuais espaços e passamos para maiúsculas
        val = bi_match.group(1).replace(" ", "").upper()
        dados_brutos.update({"numero_bi": val, "bi": val, "n_bi": val})

    # 2. Data de Nascimento (Tolera - em vez de / e falha de caracteres após Nascimento)
    nascimento_match = re.search(
        r"Nascimento[^\d]*(\d{2}[-/]\d{2}[-/]\d{4})", ocr_text, re.IGNORECASE
    )
    if nascimento_match:
        val = nascimento_match.group(1).replace("-", "/")
        dados_brutos.update({"data_nascimento": val, "data": val, "nascimento": val})

    # 3. Sexo (Tolera se o OCR ler apenas M ou F)
    sexo_match = re.search(
        r"Sexo[^\w]*(MASCULINO|FEMININO|M|F)", ocr_text, re.IGNORECASE
    )
    if sexo_match:
        val = sexo_match.group(1).upper()
        val = "MASCULINO" if val == "M" else "FEMININO" if val == "F" else val
        dados_brutos.update({"sexo": val, "genero": val})

    # 4. Estado Civil
    estado_civil_match = re.search(
        r"Estado Civil[^\w]*([A-Za-zÀ-ÿ]+)", ocr_text, re.IGNORECASE
    )
    if estado_civil_match:
        val = estado_civil_match.group(1).upper()
        dados_brutos.update({"estado_civil": val, "estado civil": val})

    # 5. Validade
    emissao_match = re.search(
        r"Emitido em[^\d]*(\d{2}[-/]\d{2}[-/]\d{4})", ocr_text, re.IGNORECASE
    )
    if emissao_match:
        dados_brutos["data_emissao"] = emissao_match.group(1).replace("-", "/")

    # Tolera a falta de acentos (Valido ate)
    validade_match = re.search(
        r"V[áa]lido at[ée][^\d]*(\d{2}[-/]\d{2}[-/]\d{4})", ocr_text, re.IGNORECASE
    )
    if validade_match:
        dados_brutos["data_validade"] = validade_match.group(1).replace("-", "/")

    # 6. Nome Completo (Tolera "Filiacao" sem cedilha ou til)
    nome_match = re.search(
        r"Nome Completo[^\w]*([\s\S]*?)Filia[çc][ãaä]o", ocr_text, re.IGNORECASE
    )
    if nome_match:
        nome_limpo = " ".join(nome_match.group(1).strip().split())
        val = nome_limpo.upper()
        dados_brutos.update({"nome_completo": val, "nome completo": val, "nome": val})

    # ========================================================
    # FILTRAGEM INTELIGENTE (Otimizada para Busca Rápida O(1))
    # ========================================================
    if not formulario:
        return dados_brutos

    def normalizar(k: str) -> str:
        return str(k).lower().replace(" ", "_").strip()

    # Cria um mapa de chaves normalizadas para evitar loops aninhados
    brutos_normalizados = {normalizar(k): v for k, v in dados_brutos.items()}

    dados_filtrados = {}

    for campo in formulario:
        if not isinstance(campo, dict):
            continue

        chave_original = str(campo.get("nome", ""))
        if not chave_original:
            continue

        chave_norm = normalizar(chave_original)

        # O Python procura instantaneamente sem precisar percorrer listas
        if chave_norm in brutos_normalizados:
            dados_filtrados[chave_original] = brutos_normalizados[chave_norm]

    return dados_filtrados
