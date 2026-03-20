import os
import google.generativeai as genai

from app.config.dotenv import get_env

class GeminiService:
    _instance = None

    def __new__(cls):
        # Garante que a configuração é feita apenas uma vez
        if cls._instance is None:
            cls._instance = super(GeminiService, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        api_key = get_env("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("Erro: A variável de ambiente GEMINI_API_KEY não foi encontrada.")
        
        # Configura a API globalmente
        genai.configure(api_key=api_key)
        
        # Inicializa o modelo (podes usar 'gemini-1.5-pro' para tarefas complexas ou '1.5-flash' para velocidade)
        self.model = genai.GenerativeModel("gemini-2.5-flash" )

    def gerar_resposta(self, prompt: str) -> str:
        """Envia um prompt simples e devolve o texto gerado."""
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Erro ao processar o pedido: {str(e)}"

# Instância pronta a ser importada noutras partes do projecto
gemini_client = GeminiService()