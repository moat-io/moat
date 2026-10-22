import os

import httpx
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap

MOAT_BASE_URL = os.getenv("MOAT_BASE_URL", "http://127.0.0.1:8001").rstrip("/")
OPENAPI_URL = os.getenv("MOAT_OPENAPI_URL", f"{MOAT_BASE_URL}/openapi.json")
MCP_TAG = os.getenv("MOAT_MCP_TAG", "mcp")
MCP_TRANSPORT = os.getenv("MOAT_MCP_TRANSPORT", "http")
MCP_HOST = os.getenv("MOAT_MCP_HOST", "127.0.0.1")
MCP_PORT = int(os.getenv("MOAT_MCP_PORT", "8000"))


def _load_openapi_spec() -> dict:
    response = httpx.get(OPENAPI_URL, timeout=30.0)
    response.raise_for_status()
    return response.json()


def build_mcp() -> FastMCP:
    client = httpx.AsyncClient(base_url=MOAT_BASE_URL, timeout=30.0)
    return FastMCP.from_openapi(
        openapi_spec=_load_openapi_spec(),
        client=client,
        name="Moat MCP Server",
        route_maps=[
            RouteMap(tags={MCP_TAG}, mcp_type=MCPType.TOOL),
            RouteMap(mcp_type=MCPType.EXCLUDE),
        ],
    )


mcp = build_mcp()


if __name__ == "__main__":
    if MCP_TRANSPORT == "stdio":
        mcp.run()
    else:
        mcp.run(transport=MCP_TRANSPORT, host=MCP_HOST, port=MCP_PORT)
