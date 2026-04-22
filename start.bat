
py -3.12 -m venv venv 

 pip install -r .\requirements.txt 

Get-History | ForEach-Object { $_.CommandLine } > historico.txt 

Get-Content (Get-PSReadLineOption).HistorySavePath > historico_completo.txt


uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

em ambiente virtual:
deactivate
:: pyinstaller --onefile app\main.py --hidden-import=fastapi --hidden-import=uvicorn --hidden-import=starlette --hidden-import=pydantic --hidden-import=pyodbc --paths=.  --collect-submodules app --hidden-import=app.routes --hidden-import=app.config  --hidden-import=paddlex --hidden-import=paddleocr --collect-all paddlex --collect-all paddleocr --add-data ".env;." --add-data "init_done.bin;."

para memorizar execução e não demorara:
pyinstaller app\main.py --hidden-import=fastapi --hidden-import=uvicorn --hidden-import=starlette --hidden-import=pydantic --hidden-import=pyodbc --paths=.  --collect-submodules app --hidden-import=app.routes --hidden-import=app.config  --hidden-import=paddlex --hidden-import=paddleocr --collect-all paddlex --collect-all paddleocr --add-data ".env;."
