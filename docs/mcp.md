# MCP Server

Moat ships an [MCP](https://modelcontextprotocol.io) server that exposes selected Moat
APIs as tools, so an LLM client (Claude Code, Claude Desktop, or any other MCP client)
can query Moat directly.

The server is `moat/src/mcp_serve.py`. It does not reimplement anything: it reads the
Flask app's OpenAPI document at runtime and turns each matching operation into an MCP
tool via `FastMCP.from_openapi`.

## Which endpoints become tools

Only operations tagged `mcp` are exposed. Everything else is explicitly excluded, so
adding an API to Moat does *not* silently publish it to LLM clients.

To publish an endpoint, tag it in the `flask-smorest` blueprint:

```python
@bp.route("", methods=["GET"])
@bp.doc(tags=["mcp"])          # <- this is what makes it an MCP tool
@bp.arguments(UserEntitlementQuerySchema, location="query")
@bp.response(200, PaginatedEntitlementSchema)
def list_user_entitlements(query_args):
    ...
```

The endpoint must be registered through the `flask_smorest.Api` object in `app.py`
(`api.register_blueprint(...)`), not `flask_app.register_blueprint(...)` - only the
former contributes to `/openapi.json`.

Currently exposed:

| Tool | Endpoint |
| --- | --- |
| `Paginated_list_of_user_entitlements` | `GET /api/entitlements/v1/users` |

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `MOAT_BASE_URL` | `http://127.0.0.1:8001` | Moat instance the tools call |
| `MOAT_OPENAPI_URL` | `$MOAT_BASE_URL/openapi.json` | Where the spec is read from |
| `MOAT_MCP_TAG` | `mcp` | OpenAPI tag that marks an operation as a tool |

## Starting the server

The Moat app must already be running - the MCP server fetches its OpenAPI document at
startup and will fail immediately if Moat is unreachable.

```bash
# 1. start Moat (defaults assume port 8001)
export PYTHONPATH=moat/src
flask --app moat.src.app run --port 8001

# 2. start the MCP server over stdio, in a second shell
export PYTHONPATH=moat/src
export MOAT_BASE_URL=http://127.0.0.1:8001
python moat/src/mcp_serve.py
```

Running it by hand like this is mostly a smoke test - `stdio` transport expects a client
on the other end of the pipe. Normally the client launches the process itself.

### Registering with Claude Code

```bash
claude mcp add moat \
  --env PYTHONPATH=moat/src \
  --env MOAT_BASE_URL=http://127.0.0.1:8001 \
  -- .venv/bin/python moat/src/mcp_serve.py
```

### Registering with Claude Desktop / any `mcpServers` config

```json
{
  "mcpServers": {
    "moat": {
      "command": ".venv/bin/python",
      "args": ["moat/src/mcp_serve.py"],
      "env": {
        "PYTHONPATH": "moat/src",
        "MOAT_BASE_URL": "http://127.0.0.1:8001"
      }
    }
  }
}
```

### HTTP transport

For a shared/remote deployment, run it over HTTP instead of stdio:

```bash
fastmcp run moat/src/mcp_serve.py:mcp --transport http --port 8002
```

## Verifying it works

With Moat running, check the endpoint that backs the tool returns 200:

```bash
curl -s http://127.0.0.1:8001/api/entitlements/v1/users?page_size=2

# confirm the operation is tagged and present in the spec
curl -s http://127.0.0.1:8001/openapi.json | jq '.paths["/api/entitlements/v1/users"].get.tags'
```

Then list and call the tools through an MCP client:

```bash
PYTHONPATH=moat/src MOAT_BASE_URL=http://127.0.0.1:8001 python - <<'PY'
import asyncio
from fastmcp import Client
from mcp_serve import build_mcp

async def main():
    async with Client(build_mcp()) as client:
        tools = await client.list_tools()
        print("tools:", [t.name for t in tools])
        result = await client.call_tool(tools[0].name, {"page_size": 1})
        print(result.content[0].text)

asyncio.run(main())
PY
```

## Troubleshooting

**`httpx.ConnectError` on startup** - Moat is not running, or `MOAT_BASE_URL` points at
the wrong port. The MCP server reads the spec eagerly at import time.

**No tools listed** - nothing in the spec carries the `mcp` tag. Check the blueprint is
registered via `api.register_blueprint` and that `@bp.doc(tags=["mcp"])` is present.

**Tool returns a 500** - the tool is a thin proxy, so the fault is in the underlying
Moat endpoint. Call it with `curl` and read the Flask log.
