import os
import sys
import ctypes
import traceback
import cv2
import numpy as np

def _setup_paddle_environment():
    """
    Garante que os caminhos do cache do PaddleX/PaddleOCR no Windows
    utilizem o caminho 8.3 (short path) para evitar falhas do runtime C++ do Paddle
    ao tentar abrir caminhos com caracteres acentuados ou especiais (ex: Francemysebastião).
    """
    try:
        user_paddlex = os.path.normpath(os.path.expanduser("~/.paddlex"))
        os.makedirs(user_paddlex, exist_ok=True)

        buf = ctypes.create_unicode_buffer(500)
        ctypes.windll.kernel32.GetShortPathNameW(user_paddlex, buf, 500)
        short_path = buf.value
        if short_path:
            os.environ["PADDLE_PDX_CACHE_HOME"] = short_path
    except Exception:
        pass

    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
    os.environ.setdefault("FLAGS_use_mkldnn", "0")
    os.environ.setdefault("FLAGS_use_onednn", "0")

_setup_paddle_environment()

from paddleocr import PaddleOCR

# Singleton do motor PaddleOCR para evitar recarga pesada de modelos a cada requisição
_ocr_engine = None

def get_ocr_engine():
    global _ocr_engine
    if _ocr_engine is None:
        _setup_paddle_environment()
        try:
            # Versões recentes do PaddleOCR usam use_textline_orientation
            _ocr_engine = PaddleOCR(lang="pt", use_textline_orientation=True)
        except TypeError:
            _ocr_engine = PaddleOCR(lang="pt", use_angle_cls=True)
    return _ocr_engine

def group_text_by_lines(texts, boxes, y_threshold=15):
    """
    texts: list[str]
    boxes: ndarray Nx4 -> [x_min, y_min, x_max, y_max]
    """
    items = []

    for text, box in zip(texts, boxes):
        x_min, y_min, x_max, y_max = box
        y_center = (y_min + y_max) / 2

        items.append({
            "text": text,
            "x": x_min,
            "y": y_center
        })

    # Ordena de cima para baixo
    items.sort(key=lambda i: i["y"])

    lines = []

    for item in items:
        placed = False

        for line in lines:
            if abs(line["y"] - item["y"]) < y_threshold:
                line["words"].append(item)
                placed = True
                break

        if not placed:
            lines.append({
                "y": item["y"],
                "words": [item]
            })

    # Ordena palavras da linha da esquerda para a direita
    final_lines = []
    for line in lines:
        line["words"].sort(key=lambda w: w["x"])
        sentence = " ".join(w["text"] for w in line["words"])
        final_lines.append(sentence)

    return final_lines

def analyze_image(image_bytes):
    try:
        ocr = get_ocr_engine()

        img = cv2.imdecode(
            np.frombuffer(image_bytes, np.uint8),
            cv2.IMREAD_COLOR
        )

        if img is None:
            raise RuntimeError("Imagem inválida ou não pôde ser decodificada.")

        result = ocr.ocr(img)

        if not result:
            raise RuntimeError("OCR executado, mas nenhum resultado retornado.")

        texts = []
        boxes = []

        # =====================================================
        # CASO 1: PaddleX / pipeline (dict dentro da lista)
        # =====================================================
        if isinstance(result[0], dict):
            page = result[0]

            texts = page.get("rec_texts", [])
            boxes = page.get("rec_boxes", [])

            if len(texts) != len(boxes):
                raise RuntimeError("Quantidade de textos e caixas delimitadoras não coincide.")

        # =====================================================
        # CASO 2: PaddleOCR padrão (lista de linhas)
        # =====================================================
        else:
            for line in result[0]:
                if not line or not isinstance(line, (list, tuple)):
                    continue

                box = line[0]
                text = line[1][0] if len(line) > 1 and isinstance(line[1], (list, tuple)) else str(line[1])

                if not isinstance(box, (list, tuple)):
                    continue

                x_coords = [p[0] for p in box]
                y_coords = [p[1] for p in box]

                x_min, x_max = min(x_coords), max(x_coords)
                y_min, y_max = min(y_coords), max(y_coords)

                texts.append(text)
                boxes.append([x_min, y_min, x_max, y_max])

        if not texts:
            # Fallback amigável se a imagem não contiver texto legível
            return {
                "text": "",
                "lines": [],
                "file": None
            }

        boxes = np.array(boxes)
        lines = group_text_by_lines(texts, boxes)
        full_text = "\n".join(lines)

        return {
            "text": full_text,
            "lines": lines,
            "file": None
        }

    except Exception as e:
        error_trace = traceback.format_exc()

        print("====== ERRO COMPLETO (PADDLE OCR) ======")
        print(error_trace)
        print("=======================================")

        return {
            "error": type(e).__name__,
            "message": str(e),
            "traceback": error_trace
        }
