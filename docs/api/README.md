# API Documentation

The API is described as an [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0) specification:

- `openapi.yaml` — the full specification (admin, producer and consumer modules)

Validate the spec:

```bash
# any OpenAPI validator, e.g. openapi-spec-validator
pip install openapi-spec-validator
openapi-spec-validator docs/api/openapi.yaml

# or Redocly CLI
npx @redocly/cli lint docs/api/openapi.yaml
```

## Serving Swagger UI

The gateway itself does not serve the documentation — host it on the NGINX reverse proxy
that already fronts the gateway.

### Option 1: static Swagger UI + the spec file

Download a [Swagger UI release](https://github.com/swagger-api/swagger-ui/releases) and
serve it together with the spec:

```nginx
# swagger-ui dist files (swagger-ui.css, swagger-ui-bundle.js, ...)
location /docs/ {
    alias /opt/kafka-gateway/docs/swagger-ui/;
}

# the OpenAPI spec
location = /docs/openapi.yaml {
    alias /opt/kafka-gateway/docs/api/openapi.yaml;
}

# gateway proxy (existing)
location / {
    proxy_pass http://127.0.0.1:8086;
}
```

Then point Swagger UI at the spec (e.g. an `index.html` in the `swagger-ui` dir):

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Kafka HTTP Gateway API</title>
  <link rel="stylesheet" href="/docs/swagger-ui.css"/>
</head>
<body>
<div id="swagger-ui"></div>
<script src="/docs/swagger-ui-bundle.js"></script>
<script>
  window.onload = () => {
    window.ui = SwaggerUIBundle({
      url: "/docs/openapi.yaml",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis]
    });
  };
</script>
</body>
</html>
```

### Option 2: Docker

```bash
docker run -d --name swagger-ui \
  -p 8088:8080 \
  -e SWAGGER_JSON=/spec/openapi.yaml \
  -v /opt/kafka-gateway/docs/api:/spec \
  swaggerapi/swagger-ui
```

If Swagger UI runs on a different origin than the spec, enable CORS on the spec location:

```nginx
location = /docs/openapi.yaml {
    alias /opt/kafka-gateway/docs/api/openapi.yaml;
    add_header Access-Control-Allow-Origin *;
}
```
