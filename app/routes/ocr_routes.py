import traceback
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.ocr.newpaddleORC import analyze_image
from app.ultils.get_id_by_token import get_current_user_id


router = APIRouter(prefix="/ocr", tags=["ocr"])


@router.post("/analyze-image")
async def analyze_image_route(
    image: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    print("ENTROU NA ROTA")
    try:
        # Validação do tipo
        if not image.content_type or not image.content_type.startswith("image/"):
            raise HTTPException(
                status_code=400,
                detail="Arquivo enviado não é uma imagem válida"
            )

        # Lê os bytes da imagem
        image_bytes = await image.read()

        if not image_bytes:
            raise HTTPException(
                status_code=400,
                detail="Imagem vazia ou corrompida"
            )

        # Executa OCR
        result = analyze_image(image_bytes)
        
        # print("===== RESULT OCR =====")
        # print(result)

        # Erro vindo do serviço OCR
        if isinstance(result, dict) and result.get("error"):
            raise HTTPException(
                status_code=422,
                detail={
                    "error": result.get("error"),
                    "message": result.get("message")
                }
            )

        return {
            "success": True,
            "text": result.get("text", ""),
            "lines": result.get("lines", []),
            "file": result.get("file")
        }

    except HTTPException:
        raise

    except Exception as e:
        error_trace = traceback.format_exc()
        print("====== ERRO OCR ROUTE ======")
        print(error_trace)
        print("============================")

        raise HTTPException(
            status_code=500,
            detail={
                "error": type(e).__name__,
                "message": str(e)
            }
        )
