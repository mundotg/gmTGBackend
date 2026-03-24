# app/config/ai_config.py

import time
from google import genai
from google.genai import types
from app.config.dotenv import get_env, get_env_float, get_env_int
from app.ultils.logger import log_message

default_prompt = (
    "És um especialista em:\n"
    "- Base de dados\n"
    "- Ciência de dados\n"
    "- Engenharia de software\n\n"
    "Responde de forma clara, prática e didática.\n"
    "Se não souberes, diz que não sabes."
)


class GeminiService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        api_key = get_env("GEMINI_API_KEY")

        if not api_key:
            raise ValueError("GEMINI_API_KEY não configurada")

        self.client = genai.Client(api_key=api_key)

        # 🔥 Lista de modelos (com variável de ambiente e fallbacks)
        self.models_priority = [
            get_env("GEMINI_MODEL_NAME", "gemini-2.5-flash"),
            "gemini-2.5-flash",
            "gemini-1.5-flash",
        ]

        # 🔥 Lendo as configurações do .env com conversão de tipo e fallbacks seguros
        self.temperature = get_env_float("GEMINI_TEMPERATURE", 0.7)
        self.max_tokens = get_env_int("GEMINI_MAX_TOKENS", 8192)
        self.max_retries = get_env_int("GEMINI_MAX_RETRIES", 3)

        # Prompt de sistema padrão caso o ficheiro .env não o tenha

        self.system_prompt = get_env("GEMINI_SYSTEM_PROMPT", default_prompt)

    # ============================================================
    # 🧠 GERAR BASE (SDK NOVA)
    # ============================================================
    def _generate(self, model: str, prompt: str):
        try:
            response = self.client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=self.temperature,  # 👈 Vem do .env
                    max_output_tokens=self.max_tokens,  # 👈 Vem do .env
                    system_instruction=self.system_prompt,  # 👈 Vem do .env
                ),
            )

            text = getattr(response, "text", None)
            if not response or not text:
                raise ValueError("Resposta vazia do modelo")

            return text.strip()

        except Exception as e:
            raise RuntimeError(f"Falha na geração: {str(e)}")

    # ============================================================
    # 🔁 RETRY + FALLBACK
    # ============================================================
    def gerar_resposta(self, prompt: str) -> str:
        last_error = None

        for model in self.models_priority:
            for attempt in range(self.max_retries or 3):  # 👈 Retries vem do .env
                try:
                    return self._generate(model, prompt)

                except Exception as e:
                    last_error = e
                    error_str = str(e)

                    log_message(
                        f"[Gemini] tentativa {attempt+1} | modelo={model} | erro={error_str}"
                    )

                    # 🔥 rate limit
                    if "429" in error_str:
                        time.sleep(min(2**attempt, 10))
                        continue

                    # 🔥 modelo não existe
                    if "404" in error_str:
                        break

                    time.sleep(1)

        log_message(f"[Gemini] Falha total: {str(last_error)}", level="error")

        return "⚠️ O sistema está temporariamente sobrecarregado. Tenta novamente."

    # ============================================================
    # 🛡️ SANITIZE
    # ============================================================
    def _sanitize_input(self, text: str) -> str:
        blacklist = [
            "ignore previous instructions",
            "act as",
            "system:",
            "assistant:",
        ]

        for word in blacklist:
            text = text.replace(word, "")

        return text.strip()

    # ============================================================
    # 💬 CHAT COM CONTEXTO REAL
    # ============================================================
    def gerar_com_contexto(self, mensagens: list[dict]) -> str:
        try:
            if not mensagens:
                return "⚠️ Nenhum contexto fornecido."

            mensagens = mensagens[-10:]

            mensagens = [
                {
                    "role": m.get("role", "user"),
                    "content": self._sanitize_input(m.get("content", "")),
                }
                for m in mensagens
            ]

            conversa = ""
            for m in mensagens:
                if m["role"] == "user":
                    conversa += f"Utilizador: {m['content']}\n"
                else:
                    conversa += f"Assistente: {m['content']}\n"

            prompt = f"Conversa:\n{conversa}\n\nResposta:\n"

            # print(f"Prompt para IA:\n{prompt}\n{'-'*50}")
            return self.gerar_resposta(prompt)

        except Exception as e:
            log_message(f"[Gemini contexto] erro: {str(e)}", level="error")
            return "⚠️ Erro ao processar a mensagem."

    # ============================================================
    # 🌊 CHAT COM CONTEXTO (STREAMING REAL)
    # ============================================================
    def gerar_stream_com_contexto(self, mensagens: list[dict]):
        try:
            if not mensagens:
                yield "⚠️ Nenhum contexto fornecido."
                return

            mensagens = mensagens[-10:]

            mensagens = [
                {
                    "role": m.get("role", "user"),
                    "content": self._sanitize_input(m.get("content", "")),
                }
                for m in mensagens
            ]

            conversa = ""
            for m in mensagens:
                if m["role"] == "user":
                    conversa += f"Utilizador: {m['content']}\n"
                else:
                    conversa += f"Assistente: {m['content']}\n"

            prompt = f"Conversa:\n{conversa}\n\nResposta:\n"

            # 🔥 MUDANÇA AQUI: usar generate_content_stream
            response = self.client.models.generate_content_stream(
                model=self.models_priority[0],
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=self.temperature,
                    max_output_tokens=self.max_tokens,
                    system_instruction=self.system_prompt,
                ),
            )

            # Iteramos sobre os pedaços (chunks) à medida que chegam
            for chunk in response:
                text = getattr(chunk, "text", "")
                if text:
                    yield text

        except Exception as e:
            log_message(f"[Gemini contexto stream] erro: {str(e)}", level="error")
            yield "⚠️ Erro ao processar a mensagem."
