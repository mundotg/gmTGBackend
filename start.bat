@echo off
title FastAPI Server - GestorBD
color 0A

echo ========================================
echo    FastAPI Server - GestorBD
echo ========================================
echo.

:: Verificar/Criar venv
if not exist venv\ (
    echo Ambiente virtual nao encontrado. Criando...
    python -m venv venv
)

:: Ativar venv
@REM call venv\Scripts\activate.bat

:: Instalar dependencias basicas
echo Instalando dependencias basicas...
@REM pip install fastapi uvicorn

:: Configurar ambiente
set PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=1

:: Verificar main.py
if exist main.py (
    set MODULE=main:app
) else if exist app\main.py (
    set MODULE=app.main:app
) else (
    echo ERRO: main.py nao encontrado!
    pause
    exit /b
)

:: Iniciar servidor
echo.
echo Iniciando servidor...
echo.
uvicorn %MODULE% --reload --host 0.0.0.0 --port 8000

:: Desativar venv
call deactivate
pause


:: pyinstaller --onefile app\main.py --hidden-import=fastapi --hidden-import=uvicorn --hidden-import=starlette --hidden-import=pydantic --paths=.  --collect-submodules app --hidden-import=app.routes --hidden-import=app.config  --hidden-import=paddlex --hidden-import=paddleocr --collect-all paddlex --collect-all paddleocr --add-data ".env;." --add-data "init_done.bin;."