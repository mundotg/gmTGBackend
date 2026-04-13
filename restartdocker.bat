# 1. Para o container atual
docker stop minha-api
docker rm minha-api

# 2. Reconstrói a imagem (o Docker vai usar cache para ser mais rápido)
docker build -t minha-api .

# 3. Roda novamente
docker run -d -p 8000:8000 --name minha-api --add-host=host.docker.internal:host-gateway minha-api