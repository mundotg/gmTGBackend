@echo off
echo 🔄 Reiniciando com rebuild (cache ativo)...

docker compose down
docker compose up -d --build

echo ✅ Pronto!
pause