# Assets do Swagger UI / ReDoc

Ficheiros de terceiros, versionados de propósito.

O `/docs` embutido do FastAPI carrega o JS e o CSS de `cdn.jsdelivr.net`. Num
contentor sem saída para a internet, na rede fechada de um cliente, ou no `.exe`
do PyInstaller numa máquina isolada, o browser não consegue ir buscá-los e a
página abre em branco — o sintoma clássico de "o Swagger não funciona".

`app/main.py` serve estes ficheiros a partir de `/static/swagger/` e só recorre
ao CDN se algum deles faltar aqui.

## Origem

| Ficheiro | Origem |
|---|---|
| `swagger-ui.css` | `https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css` |
| `swagger-ui-bundle.js` | `https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js` |
| `redoc.standalone.js` | `https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js` |
| `favicon.png` | `https://fastapi.tiangolo.com/img/favicon.png` |

## Actualizar

```bash
cd app/static/swagger
curl -O https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css
curl -O https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js
curl -o redoc.standalone.js https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js
```

Apagar um destes ficheiros não parte nada: a página volta a apontar para o CDN.
