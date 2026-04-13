import traceback
from paddleocr import PaddleOCR
import cv2
import numpy as np

# test_ocr.py
import os
os.environ['FLAGS_use_mkldnn'] = '0'
os.environ['FLAGS_use_onednn'] = '0'

from paddleocr import PaddleOCR

# Teste simples

# Teste com uma imagem (substitua pelo caminho da sua imagem)
# result = ocr.ocr('caminho/para/sua/imagem.jpg')
# print("OCR executado com sucesso!")

# from app.services.analyzer import save_report

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

    # ordena de cima para baixo
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

    # ordena palavras da linha da esquerda para direita
    final_lines = []
    for line in lines:
        line["words"].sort(key=lambda w: w["x"])
        sentence = " ".join(w["text"] for w in line["words"])
        final_lines.append(sentence)

    return final_lines
def analyze_image(image_bytes):
    try:
        
        ocr = PaddleOCR(
            lang="pt",
            use_angle_cls=True
        )

        img = cv2.imdecode(
            np.frombuffer(image_bytes, np.uint8),
            cv2.IMREAD_COLOR
        )

        if img is None:
            raise RuntimeError("Imagem inválida")

        result = ocr.ocr(img)

        if not result:
            raise RuntimeError("OCR executado, mas nenhum resultado retornado")

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
                raise RuntimeError("Quantidade de textos e boxes não coincide")

        # =====================================================
        # CASO 2: PaddleOCR padrão (lista de linhas)
        # =====================================================
        else:
            for line in result[0]:
                box = line[0]
                text = line[1][0]

                if not isinstance(box, (list, tuple)):
                    continue

                x_coords = [p[0] for p in box]
                y_coords = [p[1] for p in box]

                x_min, x_max = min(x_coords), max(x_coords)
                y_min, y_max = min(y_coords), max(y_coords)

                texts.append(text)
                boxes.append([x_min, y_min, x_max, y_max])

        if not texts:
            raise RuntimeError("OCR executado, mas nenhum texto reconhecido")

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
