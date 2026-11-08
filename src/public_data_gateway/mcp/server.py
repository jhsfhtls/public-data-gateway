"""Model Context Protocol (MCP) JSON-RPC 2.0 stdio server with HITL Tier 3 guard."""

import json
import sys
from pathlib import Path
from typing import Any

from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.normalizer import CanonicalNormalizer
from public_data_gateway.core.security import wrap_untrusted_gov_data
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.enrichment.engine import EnrichmentEngine

MCP_PROTOCOL_VERSION = "2024-11-05"


class MCPDispatcher:
    """In-memory testable JSON-RPC 2.0 Model Context Protocol request dispatcher."""

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self.catalog = CatalogRegistry(self.settings)
        self.normalizer = CanonicalNormalizer(self.settings)
        self.storage = HybridStorageEngine(self.settings)
        self.enricher = EnrichmentEngine(
            settings=self.settings,
            storage_engine=self.storage,
            normalizer=self.normalizer,
        )

    def handle_request(self, req: dict[str, Any]) -> dict[str, Any]:
        """Process a single JSON-RPC 2.0 MCP request dict and return response dict."""
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if not method:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32600, "message": "Invalid Request: missing method"},
            }

        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {"tools": {}},
                        "serverInfo": {
                            "name": "public-data-gateway-mcp",
                            "version": "0.1.0",
                        },
                    },
                }

            if method == "ping":
                return {"jsonrpc": "2.0", "id": req_id, "result": {}}

            if method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "tools": [
                            {
                                "name": "bdg_list_sources",
                                "description": "List Brazilian public datasets available in the catalog (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "wave": {"type": "integer"},
                                        "segment": {"type": "string"},
                                        "status": {"type": "string"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_resolve_municipality",
                                "description": "Resolve a Brazilian municipality name or 6D/7D code to canonical IBGE record (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "code_or_name": {"type": "string"},
                                        "uf": {"type": "string"},
                                    },
                                    "required": ["code_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_ufs",
                                "description": "List all 27 Brazilian Federative Units with codes and regions (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                },
                            },
                            {
                                "name": "bdg_list_municipalities",
                                "description": "List Brazilian municipalities with optional filters by UF, region, or capitals (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "uf": {"type": "string"},
                                        "regiao": {"type": "string"},
                                        "capitais_only": {"type": "boolean"},
                                        "limit": {"type": "integer"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_profile_dataset",
                                "description": "Compute compact summary metrics for a Parquet dataset (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "parquet_path": {"type": "string"}
                                    },
                                    "required": ["parquet_path"],
                                },
                            },
                            {
                                "name": "bdg_enrich_dataset",
                                "description": "Enrich a dataset with canonical codes. Enforces Client Path Shield (Tier 2).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "input_path": {"type": "string"},
                                        "output_path": {"type": "string"},
                                        "entity_type": {"type": "string", "enum": ["municipio", "cnpj", "cep"]},
                                        "key_column": {"type": "string"},
                                        "uf_column": {"type": "string"},
                                    },
                                    "required": ["input_path", "output_path", "key_column"],
                                },
                            },
                            {
                                "name": "bdg_sync_dataset",
                                "description": "Trigger bulk sync of government dataset. Requires Human-in-the-Loop approval (Tier 3).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "source_id": {"type": "string"},
                                        "human_approved": {"type": "boolean"},
                                    },
                                    "required": ["source_id"],
                                },
                            },
                        ]
                    },
                }

            if method == "tools/call":
                tool_name = params.get("name")
                args = params.get("arguments", {})
                return self._call_tool(req_id, tool_name, args)

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method '{method}' not found"},
            }

        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    def _call_tool(self, req_id: Any, tool_name: str | None, args: dict[str, Any]) -> dict[str, Any]:
        """Dispatch MCP tool execution with safety guardrails."""
        if tool_name == "bdg_list_sources":
            sources = self.catalog.list_sources(
                wave=args.get("wave"),
                segment_id=args.get("segment"),
                status=args.get("status"),
            )
            raw = [s.model_dump() for s in sources]
            wrapped = wrap_untrusted_gov_data(raw, source_id="bdg_catalog")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_resolve_municipality":
            code_or_name = args.get("code_or_name")
            uf = args.get("uf")
            record = self.normalizer.get_municipio_record(code_or_name, uf=uf)
            wrapped = wrap_untrusted_gov_data(record or {}, source_id="ibge_municipios")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_ufs":
            from public_data_gateway.connectors.demografico.ibge_localidades import (
                IBGELocalidadesConnector,
            )

            conn = IBGELocalidadesConnector(settings=self.settings, storage=self.storage)
            ufs = conn.list_ufs()
            wrapped = wrap_untrusted_gov_data(ufs, source_id="ibge_localidades_ufs")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_municipalities":
            from public_data_gateway.connectors.demografico.ibge_localidades import (
                IBGELocalidadesConnector,
            )

            conn = IBGELocalidadesConnector(settings=self.settings, storage=self.storage)
            items = conn.list_municipios(
                uf=args.get("uf"),
                regiao=args.get("regiao"),
                capitais_only=args.get("capitais_only", False),
                limit=args.get("limit", 50),
            )
            wrapped = wrap_untrusted_gov_data(items, source_id="ibge_localidades_municipios")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_profile_dataset":
            parquet_path = Path(args["parquet_path"])
            summary = self.storage.profile_parquet(parquet_path)
            wrapped = wrap_untrusted_gov_data(summary, source_id=parquet_path.stem)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_enrich_dataset":
            _, report = self.enricher.enrich(
                entity_type=args.get("entity_type", "municipio"),
                key_column=args["key_column"],
                input_path=Path(args["input_path"]),
                uf_column=args.get("uf_column"),
                output_path=Path(args["output_path"]),
            )
            wrapped = wrap_untrusted_gov_data(report.model_dump(), source_id="bdg_enrichment")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_sync_dataset":
            # HITL Tier 3 Guard: Reject bulk sync without explicit human approval
            human_approved = args.get("human_approved", False)
            if not human_approved:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "HITL Tier 3 Guard: Bulk sync requires human_approved=True.",
                            }
                        ],
                        "isError": True,
                    },
                }

            source_id = args["source_id"]
            if source_id == "ibge_localidades":
                from public_data_gateway.connectors.demografico.ibge_localidades import (
                    IBGELocalidadesConnector,
                )

                conn = IBGELocalidadesConnector(settings=self.settings, storage=self.storage)
                table = conn.extract()
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Bulk sync for '{source_id}' completed successfully: {table.num_rows} records materialized into Bronze, Silver, and Gold.",
                            }
                        ],
                        "isError": False,
                    },
                }

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Bulk sync for '{source_id}' approved and queued.",
                        }
                    ],
                    "isError": False,
                },
            }

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Unknown tool: '{tool_name}'"},
        }


def main() -> None:
    """Run standard I/O loop for Model Context Protocol JSON-RPC."""
    dispatcher = MCPDispatcher()
    for line in sys.stdin:
        line_clean = line.strip()
        if not line_clean:
            continue
        try:
            req = json.loads(line_clean)
            res = dispatcher.handle_request(req)
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()
        except json.JSONDecodeError:
            err_res = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": "Parse error: invalid JSON"},
            }
            sys.stdout.write(json.dumps(err_res) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
