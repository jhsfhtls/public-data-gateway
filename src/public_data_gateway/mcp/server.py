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
                            {
                                "name": "bdg_get_simples_status",
                                "description": "Resolve Simples Nacional and MEI tax regime for a given CNPJ.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj": {"type": "string"},
                                    },
                                    "required": ["cnpj"],
                                },
                            },
                            {
                                "name": "bdg_get_pncp_contratacoes",
                                "description": "Query public procurement processes and tenders from PNCP (Lei nº 14.133/2021).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "uf": {"type": "string"},
                                        "modalidade_id": {"type": "integer"},
                                        "ano": {"type": "integer"},
                                        "limit": {"type": "integer"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_pncp_contratos",
                                "description": "Query formal administrative contracts and commitments from PNCP.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj_orgao": {"type": "string"},
                                        "ni_fornecedor": {"type": "string"},
                                        "ano": {"type": "integer"},
                                        "limit": {"type": "integer"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_compliance_status",
                                "description": "Check whether a Brazilian company (CNPJ) or person (CPF) has active sanctions in CGU CEIS, CNEP, or CEPIM (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento": {"type": "string", "description": "CNPJ or CPF to check"},
                                    },
                                    "required": ["documento"],
                                },
                            },
                            {
                                "name": "bdg_get_pep_status",
                                "description": "Check whether an individual is registered as Politically Exposed Person (PEP) under COAF Resolução nº 40/2021 (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "CPF (masked or digits) or name to search"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_litigation_risk",
                                "description": "Assess corporate or individual litigation risk and court proceedings from CNJ DataJud (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento": {"type": "string", "description": "CNPJ or CPF to evaluate"},
                                    },
                                    "required": ["documento"],
                                    },
                            },
                            {
                                "name": "bdg_get_cndt_certificate",
                                "description": "Query official CNDT labor compliance certificate from TST (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento": {"type": "string", "description": "CNPJ or CPF to query"},
                                    },
                                    "required": ["documento"],
                                },
                            },
                            {
                                "name": "bdg_get_labor_risk",
                                "description": "Assess labor compliance risk score and BNDT debtor exposure from TST (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento": {"type": "string", "description": "CNPJ or CPF to evaluate"},
                                    },
                                    "required": ["documento"],
                                },
                            },
                            {
                                "name": "bdg_get_justica_aberta_offices",
                                "description": "Get municipal extrajudicial notary offices registry, Real Estate offices, and titular tenure share (CNJ Justiça Aberta).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_justica_aberta_emoluments",
                                "description": "Get municipal notary emoluments revenue, statutory transfers, and unit ticket (CNJ Justiça Aberta).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_justica_aberta_features",
                                "description": "Get Gold CNJ Justiça Aberta cartorial efficiency score, real-estate dynamism score, and notary hub classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_justica_aberta_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite CNJ Justiça Aberta cartorial efficiency score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_suframa_status",
                                "description": "Query official SUFRAMA registration, incentive approval, and operational status in Polo Industrial de Manaus and ALCs (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj_or_inscricao": {"type": "string", "description": "CNPJ or 9-digit SUFRAMA inscription number to query"},
                                    },
                                    "required": ["cnpj_or_inscricao"],
                                },
                            },
                            {
                                "name": "bdg_get_tax_incentives_profile",
                                "description": "Assess corporate fiscal incentive score and industrial capacity profile in PIM, ZFM, and Western Amazon (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj": {"type": "string", "description": "CNPJ to evaluate"},
                                    },
                                    "required": ["cnpj"],
                                },
                            },
                            {
                                "name": "bdg_get_cvm_company",
                                "description": "Query official CVM registration, sector, and latest financial snapshot for a publicly traded company (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj_or_codigo": {"type": "string", "description": "CNPJ or CVM registration code (e.g. 009512 for Petrobras)"},
                                    },
                                    "required": ["cnpj_or_codigo"],
                                },
                            },
                            {
                                "name": "bdg_get_financial_ratios",
                                "description": "Assess fundamental financial health, solvency score (0-100), liquidity, and profit margins from CVM DFP statements (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj_or_codigo": {"type": "string", "description": "CNPJ or CVM registration code"},
                                    },
                                    "required": ["cnpj_or_codigo"],
                                },
                            },
                            {
                                "name": "bdg_get_municipality_trade",
                                "description": "Query foreign trade flow, trade balance, export/import totals, and export competitiveness score for a Brazilian municipality from MDIC Comex Stat (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3548500 for Santos/SP)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ncm_trade_profile",
                                "description": "Query commodity trade statistics, export/import flows, and trade orientation for an 8-digit NCM code from MDIC Comex Stat (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "codigo_ncm": {"type": "string", "description": "8-digit NCM commodity code (e.g. 12019000 for Soja)"},
                                    },
                                    "required": ["codigo_ncm"],
                                },
                            },
                            {
                                "name": "bdg_get_fund_profile",
                                "description": "Query investment fund regulatory registration, status, class, manager, administrator, NAV, and liquidity profile from CVM (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj": {"type": "string", "description": "14-digit CNPJ of the investment fund (e.g. 03243208000158)"},
                                    },
                                    "required": ["cnpj"],
                                },
                            },
                            {
                                "name": "bdg_get_asset_manager_funds",
                                "description": "Query Asset Management firms (Gestoras de Recursos) ranking by AuM and managed fund classes from CVM (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "gestor_query": {"type": "string", "description": "Optional search filter for asset manager name or CNPJ"},
                                        "min_aum": {"type": "number", "description": "Minimum AuM filter in BRL"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_bank_financial_profile",
                                "description": "Query financial institution prudential segment (S1-S5), quarterly balance sheet, P&L, and Basiléia ratio from BACEN IF.data (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj": {"type": "string", "description": "Financial institution CNPJ or BACEN code (e.g. 60701190000104 for Itaú)"},
                                    },
                                    "required": ["cnpj"],
                                },
                            },
                            {
                                "name": "bdg_get_banking_ranking",
                                "description": "Query Brazilian banking ranking by Total Assets, Credit Portfolio, and Basiléia Index with optional segment filter from BACEN IF.data (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "segmento": {"type": "string", "description": "Optional prudential segment filter (S1, S2, S3, S4, S5)"},
                                        "top": {"type": "integer", "description": "Top N institutions to return (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_municipality_fiscal_health",
                                "description": "Query municipality fiscal health, CAPAG grade (A, B, C), debt ratio DCL/RCL, and continuous fiscal score from STN Tesouro Transparente (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "IBGE 7-digit code or municipality name (e.g. 3550308 or São Paulo)"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_sadipem_credit_operations",
                                "description": "Query SADIPEM subnational credit operations, financing limits, lenders and loan purposes from STN (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "IBGE 7-digit code, UF acronym (e.g. SP) or entity name"},
                                        "status": {"type": "string", "description": "Optional filter by pleito status (e.g. Deferido)"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_siconfi_fiscal_profile",
                                "description": "Query Gold SICONFI fiscal accounting profile, LRF personnel ratio, tax autonomy, and solidity score for a Brazilian municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "IBGE 7-digit code or municipality name (e.g. 1302603 or Manaus)"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_siconfi_rgf_limits",
                                "description": "Query SICONFI Fiscal Management Report (RGF) LRF personnel spending % of RCL and net consolidated debt limits (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code"},
                                        "ano": {"type": "integer", "description": "Optional exercise year (e.g. 2024)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_siconfi_dca_balance",
                                "description": "Query SICONFI Annual Accounts Declaration (DCA/Finbra) assets, liabilities, net equity, revenues, and expenses (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code"},
                                        "ano": {"type": "integer", "description": "Optional exercise year (e.g. 2024)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_siconfi_municipalities",
                                "description": "List top Brazilian municipalities ranked by SICONFI fiscal & accounting solidity score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_pgfn_debtor_inscriptions",
                                "description": "Lookup active PGFN Dívida Ativa da União inscriptions by CNPJ, masked CPF, debtor name, or IBGE municipality code (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento_or_name": {"type": "string", "description": "CNPJ, debtor name, or 7-digit IBGE municipality code"},
                                    },
                                    "required": ["documento_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_pgfn_municipal_summary",
                                "description": "Retrieve consolidated municipal PGFN active debt summary across general, previdenciário, and FGTS stocks (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "IBGE 7-digit municipality code or name (e.g. 1302603 or Manaus)"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_pgfn_risk_features",
                                "description": "Retrieve Gold PGFN tax default risk score, judicial execution rate, and exposure tier for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "IBGE 7-digit municipality code or name (e.g. 3509502 or Campinas)"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_pgfn_municipalities",
                                "description": "List municipalities ranked by consolidated PGFN active debt & FGTS volume (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sinesp_ocorrencias",
                                "description": "Lookup SINESP-VDE criminal occurrences (cargo theft, vehicle theft, fraud) by code, crime type, IBGE code, UF, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Occurrence code, crime type, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_sinesp_vitimas_mvi",
                                "description": "Lookup SINESP/FBSP intentional violent deaths (MVI) and police clearance rates by code, IBGE code, UF, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "MVI code, IBGE code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_sinesp_seguranca_features",
                                "description": "Get normalized Gold SINESP/FBSP public safety score, MVI rate, cargo theft exposure, and territorial security tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sinesp_seguranca_municipalities",
                                "description": "List Brazilian municipalities ranked by SINESP/FBSP public safety score or logistics cargo theft rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_cargo_theft": {"type": "boolean", "description": "Set true to rank municipalities by highest freight cargo theft per 100k hab first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cgu_gastos_diretos",
                                "description": "Lookup CGU federal expenses and direct execution by code, SIAFI agency, function, modality, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "CGU expense code, SIAFI agency, function, modality, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cgu_transferencias_fpm",
                                "description": "Lookup federal FPM, FUNDEB, and SUS transfers and fiscal dependency share by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Transfer code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cgu_transparencia_features",
                                "description": "Get normalized Gold CGU federal spending execution integrity score, transfers, and transparency tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cgu_transparencia_municipalities",
                                "description": "List Brazilian municipalities ranked by CGU execution integrity score or total federal spending (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_spending": {"type": "boolean", "description": "Set true to rank municipalities by highest total federal spend first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cgu_servidores_lotacao",
                                "description": "Lookup SIAPE federal civil servants by code, agency, legal regime, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "SIAPE lotacao code, agency code, regime, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cgu_servidores_folha",
                                "description": "Lookup SIAPE monthly average gross salary, constitutional ceiling share, and DAS/FCE extras by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Payroll code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cgu_servidores_features",
                                "description": "Get normalized Gold CGU/SIAPE civil service governance score, monthly payroll, and functional tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cgu_servidores_municipalities",
                                "description": "List Brazilian municipalities ranked by SIAPE civil service governance score or monthly gross payroll (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_payroll": {"type": "boolean", "description": "Set true to rank municipalities by highest monthly federal payroll first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_tse_candidaturas",
                                "description": "Lookup TSE municipal candidacies and party competition by election code, office, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Election code, office, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_tse_votacao",
                                "description": "Lookup TSE municipal electorate, turnout, abstention, and valid votes by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Voting code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_tse_features",
                                "description": "Get normalized Gold TSE electoral integrity score, turnout, campaign approval rate, and maturity tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_tse_municipalities",
                                "description": "List Brazilian municipalities ranked by TSE electoral integrity score or eligible electorate size.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_electorate": {"type": "boolean", "description": "Set true to rank municipalities by highest eligible electorate first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cnciai_improbidade_condemnations",
                                "description": "Query CNJ CNCIAI administrative misconduct condemnations by document, party name, or IBGE code (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "documento_or_name": {"type": "string", "description": "CPF/CNPJ (or masked), party name, or 7-digit IBGE code"},
                                    },
                                    "required": ["documento_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_cnciai_municipal_summary",
                                "description": "Retrieve municipal consolidated CNJ CNCIAI administrative misconduct summary (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_cnciai_risk_features",
                                "description": "Retrieve Gold CNJ CNCIAI administrative misconduct & contracting debarment risk features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cnciai_municipalities",
                                "description": "List municipalities ranked by Gold CNJ CNCIAI misconduct risk exposure (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_capag_municipal_evaluation",
                                "description": "Retrieve official STN CAPAG municipal evaluation and ICF quality ranking (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_capag_ifgf_gestao_fiscal",
                                "description": "Retrieve FIRJAN Municipal Fiscal Management Index (IFGF) and sub-indices (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_capag_sovereign_features",
                                "description": "Retrieve Gold CAPAG + IFGF subnational sovereign credit rating and Union guarantee features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_capag_municipalities",
                                "description": "List municipalities ranked by composite CAPAG + IFGF subnational sovereign credit score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_bndes_operacoes",
                                "description": "Lookup BNDES direct FINEM and indirect credit operations by code, modality, sector, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "BNDES operation code, modality, CNAE sector, IBGE 7-digit code, UF, or city"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_bndes_desembolsos",
                                "description": "Lookup annual BNDES disbursements, green/ESG share, and MSME participation by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Disbursement code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_bndes_features",
                                "description": "Get normalized Gold BNDES development financing dynamism score, green share, and maturity tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_bndes_municipalities",
                                "description": "List Brazilian municipalities ranked by BNDES development financing score or total contracted volume (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_volume": {"type": "boolean", "description": "Set true to rank municipalities by highest total BNDES volume first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cvm_ofertas_rcvm160",
                                "description": "Lookup CVM RCVM 160 public offerings by code, security type, rite, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "RCVM 160 code, security type, rite, IBGE 7-digit code, UF, or city"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cvm_administradores",
                                "description": "Lookup CVM portfolio managers and FII/FIP managers, AUM, and regulatory compliance by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Administrator code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_cvm_extra_features",
                                "description": "Get normalized Gold CVM capital markets sophistication score, RCVM 160 volume, and ecosystem tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cvm_extra_municipalities",
                                "description": "List Brazilian municipalities ranked by CVM capital markets sophistication score or RCVM 160 volume raised (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_volume": {"type": "boolean", "description": "Set true to rank municipalities by highest RCVM 160 volume first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_susep_premios_ramo",
                                "description": "Lookup SUSEP direct insurance premiums and loss ratios by SUSEP code, branch, IBGE, UF, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "SUSEP code, insurance branch (e.g. VIDA_PESSOAS), IBGE code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_susep_previdencia_vgbl",
                                "description": "Lookup SUSEP VGBL/PGBL open supplementary pension contributions and technical provisions.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Pension record code, IBGE code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_susep_features",
                                "description": "Get normalized Gold SUSEP insurance market maturity score, issued premiums, and solvency tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_susep_municipalities",
                                "description": "List Brazilian municipalities ranked by SUSEP insurance maturity score or annual issued premiums (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_premios": {"type": "boolean", "description": "Set true to rank municipalities by highest issued premiums first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_estban_municipal_balance",
                                "description": "Retrieve monthly BACEN ESTBAN COSIF municipal banking balance sheet (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_estban_credit_portfolio",
                                "description": "Retrieve municipal sectoral credit portfolio (rural, real estate, PCLD) from BACEN ESTBAN (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_estban_liquidity_features",
                                "description": "Retrieve Gold BACEN ESTBAN local banking depth and credit/deposit liquidity features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_estban_municipalities",
                                "description": "List municipalities ranked by Gold BACEN ESTBAN local banking depth score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_bcb_agencias_branches",
                                "description": "Lookup registered BACEN bank branches by IBGE 7D, CNPJ, CEP, or name (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_cnpj": {"type": "string", "description": "7-digit IBGE code, CNPJ, CEP, UF, or bank/city name"},
                                    },
                                    "required": ["id_municipio_or_cnpj"],
                                },
                            },
                            {
                                "name": "bdg_get_bcb_agencias_coverage",
                                "description": "Retrieve municipal bank branch density per 100k inhabitants and public/private share (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_bcb_agencias_capilaridade_features",
                                "description": "Retrieve Gold BACEN physical banking capillarity and inclusion features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_bcb_agencias_municipalities",
                                "description": "List municipalities ranked by Gold BACEN physical banking capillarity score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cno_works",
                                "description": "Lookup registered RFB CNO civil construction works by CNO code, CNPJ, IBGE 7D, CEP, CNAE, or name (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "CNO inscription, CNPJ, IBGE 7D, CEP, CNAE, UF, or name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cno_municipal_summary",
                                "description": "Retrieve municipal consolidated RFB CNO work counts and built area (m²) (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cno_construction_features",
                                "description": "Retrieve Gold RFB CNO civil construction dynamism and CAPEX expansion features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_rfb_cno_municipalities",
                                "description": "List municipalities ranked by Gold RFB CNO construction dynamism score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to return (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter UF state filter"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_search_catmat_materials",
                                "description": "Search materials and catalog descriptions in Compras.gov.br CATMAT/SIASG with PDM, class, and sustainability flags (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string", "description": "Search keyword for materials (e.g. CADEIRA, NOTEBOOK)"},
                                        "limit": {"type": "integer", "description": "Maximum number of items to return (default: 10)"},
                                    },
                                    "required": ["query"],
                                },
                            },
                            {
                                "name": "bdg_get_procurement_price_benchmark",
                                "description": "Query official price benchmark, market dispersion, and overpricing risk rating from Compras.gov.br procurement price records (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "PDM code (e.g. 00313), item code (e.g. 206504), or description"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_querido_diario_licitacoes",
                                "description": "Lookup municipal official gazette tenders and dispensations in Querido Diário (OKBR) by code, theme, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Gazette act code, theme, IBGE 7-digit code, UF, or city"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_querido_diario_excertos",
                                "description": "Lookup 12-month indexed municipal gazette editions, education/health/ESG mentions, and OCR legibility rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "Excerpt code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_querido_diario_features",
                                "description": "Get normalized Gold Querido Diário OKBR official gazette transparency score, OCR rate, and maturity tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_querido_diario_municipalities",
                                "description": "List Brazilian municipalities ranked by Querido Diário OKBR gazette transparency score or cited contract value (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_volume": {"type": "boolean", "description": "Set true to rank municipalities by highest cited contract values first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_municipality_gdp",
                                "description": "Query official municipal GDP, agricultural, industrial, and services gross value added (VAB) from IBGE SIDRA (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_municipality_socioeconomic_profile",
                                "description": "Query comprehensive Gold socioeconomic indicators: GDP per capita, economic profile, dynamism score, and inequality from IBGE SIDRA (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ipca_history",
                                "description": "Query official IPCA inflation series (monthly, 12-month accumulated, year-to-date) from IBGE SIDRA (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Number of months to retrieve (default: 12)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_census_summary",
                                "description": "Query official Censo Demográfico 2022 population, occupied households, and density by municipality ID from IBGE (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_demographic_profile",
                                "description": "Query Gold demographic profile: age structure, median age, aging index, literacy, and social maturity from Censo 2022 (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_highest_aging_municipalities",
                                "description": "List municipalities with the highest aging index ratio from IBGE Censo 2022 (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_employment_summary",
                                "description": "Query official Novo CAGED monthly employment admissions, separations, and net balance by municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                        "competencia": {"type": "string", "description": "Optional competence in YYYY-MM format"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_labor_market_profile",
                                "description": "Query Gold labor market profile: turnover, salary dynamics, heating score, and employment vulnerability from Novo CAGED (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_job_creating_municipalities",
                                "description": "List municipalities with highest net formal job creation balance from MTE Novo CAGED (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_rais_employment_summary",
                                "description": "Query MTE RAIS annual formal employment stock by municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                        "ano": {
                                            "type": "integer",
                                            "description": "Optional RAIS reference year",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_rais_market_profile",
                                "description": "Query RAIS Gold municipal formalization score and structural labor features (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_formalized_municipalities",
                                "description": "Rank municipalities by RAIS formalization score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        }
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_mps_inss_beneficios",
                                "description": "Lookup INSS active issued benefits (RGPS & BPC/LOAS) by code, species, group, clientele, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "INSS benefit code, species code, group, clientele, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mps_inss_concessoes",
                                "description": "Lookup annual INSS benefit grants, denial rates, and concession lead times by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Grant code, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mps_inss_features",
                                "description": "Get normalized Gold MPS/INSS social security coverage score, monthly benefit payout, and RGPS sustainability tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_mps_inss_municipalities",
                                "description": "List Brazilian municipalities ranked by INSS coverage score or monthly benefit payout volume (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                        "sort_by_volume": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest monthly INSS payout volume first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_municipio_boundary",
                                "description": "Query IBGE official boundary polygon GeoJSON, centroid and bounding box for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code (e.g. 3550308 for São Paulo)",
                                        },
                                        "qualidade": {
                                            "type": "string",
                                            "enum": ["minima", "intermediaria", "maxima"],
                                            "description": "Mesh resolution quality (default: minima)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_uf_boundary",
                                "description": "Query IBGE official boundary polygon GeoJSON and centroid for a Federative Unit (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "2-letter state abbreviation (e.g. SP, RJ)",
                                        },
                                        "qualidade": {
                                            "type": "string",
                                            "enum": ["minima", "intermediaria", "maxima"],
                                            "description": "Mesh resolution quality (default: minima)",
                                        },
                                    },
                                    "required": ["sigla_uf"],
                                },
                            },
                            {
                                "name": "bdg_get_territorial_features",
                                "description": "Query IBGE Gold Feature Store spatial morphology features and compactness score for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_compact_municipalities",
                                "description": "List top municipalities ranked by Polsby-Popper territorial compactness score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_anatel_stations",
                                "description": "Query licensed ANATEL Mosaico mobile base stations (ERBs 5G/4G/3G/2G) and urban coverage for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code (e.g. 3550308 for São Paulo)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_anatel_broadband",
                                "description": "Query ANATEL fixed broadband (SCM), backhaul fiber availability, FTTH share and median speed for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_anatel_telecom_features",
                                "description": "Query Gold Feature Store digital connectivity score, 5G maturity score and infrastructure tier for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_connected_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite ANATEL digital connectivity score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_antt_highway_corridors",
                                "description": "Query ANTT federal highway corridors, concession length, active toll plazas and heavy vehicle traffic (VMD) for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code (e.g. 3550308 for São Paulo)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_antt_rntrc_fleet",
                                "description": "Query ANTT RNTRC cargo transport registry (ETC, TAC, CTC), enabled freight vehicles and static capacity for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_antt_logistics_features",
                                "description": "Query Gold Feature Store logistics infrastructure score, intermodality score and node classification for a municipality (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "IBGE 7-digit municipality code",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_logistics_hubs",
                                "description": "List top Brazilian municipalities ranked by composite ANTT logistics infrastructure and intermodality score (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_senatran_frota_tipos",
                                "description": "Lookup SENATRAN/RENAVAM active vehicle fleet by vehicle type (AUTOMOVEL, MOTOCICLETA, CAMINHAO_PESADO), IBGE code, UF, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Vehicle category, survey code, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_senatran_frota_propulsao_ev",
                                "description": "Lookup SENATRAN vehicle fleet fuel matrix (Flex, Diesel) and electrified BEV/PHEV/HEV share by municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Propulsion code, energy matrix, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_senatran_frota_features",
                                "description": "Get normalized Gold SENATRAN fleet modernization score, fleet age, motorization rate, and EV transition tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_senatran_frota_municipalities",
                                "description": "List Brazilian municipalities ranked by SENATRAN fleet modernization score or electrified BEV/HEV volume.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                        "sort_by_ev": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest electrified BEV/HEV fleet count first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_prf_sinistros_rodovias",
                                "description": "Lookup PRF federal highway accident records by code, BR highway, cause, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "PRF code, BR highway, cause, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_prf_corredores_logisticos",
                                "description": "Lookup PRF federal logistics corridors, heavy freight crashes, and corridor severity by code, BR, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Corridor code, BR highway, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_prf_sinistralidade_features",
                                "description": "Get normalized Gold PRF road safety score, lethality rate, heavy truck crashes, and highway risk tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_prf_seguranca_viaria",
                                "description": "List Brazilian municipalities ranked by PRF road safety score or highest annual highway fatalities.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                        "sort_by_fatalities": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest annual highway fatalities first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cnes_establishments",
                                "description": "Get municipal DataSUS CNES summary of active health establishments, hospitals, UBS, UPA, and SUS coverage share.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_cnes_beds_icu",
                                "description": "Get municipal DataSUS CNES hospital bed capacity, SUS beds, and Adult/Pediatric/Neonatal ICU bed counts.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_cnes_health_features",
                                "description": "Get Gold health capacity score, ICU complexity score, and assistive network classification for a municipality (DataSUS CNES).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_health_hubs",
                                "description": "List top Brazilian municipalities ranked by composite DataSUS CNES hospital capacity and ICU complexity score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sih_admissions",
                                "description": "Get municipal DataSUS SIH/SUS hospital admissions summary (AIH), urgency share, and mean length of stay.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sih_hospital_costs",
                                "description": "Get municipal DataSUS SIH/SUS approved AIH hospital billing, ICU cost share, and mean ticket per admission.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sih_morbidity_features",
                                "description": "Get Gold SIH hospital complexity score, morbidity pressure score, and epidemiological profile for a municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sih_complexity_hubs",
                                "description": "List top Brazilian municipalities ranked by composite DataSUS SIH/SUS surgical and ICU hospital complexity score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sim_general_mortality",
                                "description": "Get municipal DataSUS SIM general mortality summary, hospital deaths share, and ill-defined causes rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sim_maternal_infant_mortality",
                                "description": "Get municipal DataSUS SIM infant, neonatal, and maternal mortality rates alongside death investigation coverage.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sim_mortality_features",
                                "description": "Get Gold DataSUS SIM surveillance quality score, preventable mortality burden score, and transition tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sim_surveillance_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite DataSUS SIM mortality surveillance quality score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sinasc_live_births",
                                "description": "Get municipal DataSUS SINASC live births volume, crude birth rate, hospital deliveries, and cesarean share.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sinasc_newborn_vitality",
                                "description": "Get municipal newborn vitality indicators (low birth weight, prematurity, Apgar 5m, and congenital anomalies) from DataSUS SINASC.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sinasc_perinatal_features",
                                "description": "Get Gold DataSUS SINASC prenatal care quality score, perinatal vitality score, and maternal-perinatal tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sinasc_prenatal_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite DataSUS SINASC prenatal care quality score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cmed_presentations_summary",
                                "description": "Get municipal ANVISA CMED registered drug presentations inventory, reference lab CNPJ, and generics/similares share.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, CNPJ-14, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_cmed_public_procurement_pmvg",
                                "description": "Get municipal ANVISA CMED CAP mandatory scope, PF/PMC/PMVG ceilings, potential savings, and SUS compliance share.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, CNPJ-14, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_cmed_drug_pricing_features",
                                "description": "Get Gold ANVISA CMED generics competitiveness score, PMVG/CAP regulatory efficiency score, and pharmaceutical hub classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, CNPJ-14, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cmed_regulatory_efficiency_hubs",
                                "description": "List top Brazilian pharmaceutical hub municipalities ranked by composite ANVISA CMED PMVG/CAP regulatory efficiency score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_vigilancia_srag_respiratory_surveillance",
                                "description": "Get official OpenDataSUS municipal Flu Syndrome (e-SUS) and SARI/SRAG (SIVEP-Gripe) hospitalizations, ICU share, lethality, and RT-PCR confirmation.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_vigilancia_sipni_vaccination_coverage",
                                "description": "Get official OpenDataSUS municipal SI-PNI/RNDS vaccination coverage (Pentavalent, VIP, MMR/Tríplice Viral), homogeneity, and dropout rates.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_vigilancia_epidemiological_features",
                                "description": "Get normalized Gold OpenDataSUS epidemiological surveillance readiness score, PNI collective immunization protection score, and 4-tier municipal classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_vigilancia_surveillance_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite OpenDataSUS epidemiological readiness and PNI immunization protection scores.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_ans_beneficiarios",
                                "description": "Lookup ANS private health plan beneficiaries by code, care line, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "ANS code, care segmentation, IBGE 7-digit code, UF, or city"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ans_idss_qualidade",
                                "description": "Lookup ANS health operators IDSS quality score, average monthly ticket, and NIP complaints by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {"type": "string", "description": "IDSS code, IBGE 7-digit code, UF, or municipality name"},
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ans_features",
                                "description": "Get normalized Gold ANS supplementary health market maturity score, private coverage, IDSS, and tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {"type": "string", "description": "7-digit IBGE municipality code or municipality name"},
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_ans_municipalities",
                                "description": "List Brazilian municipalities ranked by ANS supplementary health maturity score or active beneficiaries.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Maximum rows (default: 10)"},
                                        "sigla_uf": {"type": "string", "description": "Optional 2-letter state filter (e.g. SP)"},
                                        "sort_by_beneficiaries": {"type": "boolean", "description": "Set true to rank municipalities by highest active beneficiaries first"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_censo_escolar_schools",
                                "description": "Get municipal active schools inventory, broadband internet, science labs, libraries, and PcD accessibility (INEP Censo Escolar).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_censo_escolar_teachers",
                                "description": "Get municipal teaching workforce qualification, higher-ed share, postgraduate share, and INEP AFD indicator.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_censo_escolar_features",
                                "description": "Get Gold INEP Censo Escolar school infrastructure score, educational flow quality score, and basic education tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_censo_escolar_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite INEP Censo Escolar infrastructure score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_censo_superior_institutions",
                                "description": "Get municipal Higher Education Institutions (IES) inventory, public/private split, and IGC continuous score (INEP Censo da Educação Superior).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_censo_superior_faculty",
                                "description": "Get municipal IES faculty qualifications (PhD, Masters stricto sensu) and full-time DE regime share (INEP Censo Superior).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_censo_superior_features",
                                "description": "Get Gold INEP Censo Superior academic excellence score, higher human capital score, and university hub tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_censo_superior_hubs",
                                "description": "List top Brazilian municipalities ranked by composite INEP Censo Superior academic and research excellence score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_ideb_municipal_results",
                                "description": "Get official municipal IDEB biennial scores across public/private basic education stages, P flow indicator, SAEB N score, and INEP goal fulfillment.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ideb_saeb_proficiency",
                                "description": "Get municipal SAEB Portuguese and Mathematics average proficiency (5th and 9th grades), adequate learning percentages, and SAEB participation rate (INEP IDEB).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ideb_education_quality_features",
                                "description": "Get normalized Gold INEP IDEB education performance score, SAEB proficiency/equity score, INSE ratio, and 4-tier municipal classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_ideb_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite INEP IDEB educational performance and SAEB proficiency/equity scores.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_enem_municipal_area_scores",
                                "description": "Get official municipal INEP ENEM mean TRI scores across all four objective knowledge areas (LC, CH, CN, MT), confirmed applicants, and 2-day attendance rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_enem_essay_competencies",
                                "description": "Get municipal INEP ENEM Essay mean score, Competencies I/III/V scores, 900+ excellence essays share, and zero/void essay rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_enem_performance_equity_features",
                                "description": "Get normalized Gold INEP ENEM cognitive performance score, higher-education access equity score, SiSU/ProUni/FIES eligibility rate, and 4-tier municipal classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_enem_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite INEP ENEM cognitive performance and higher-education access equity scores.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sucupira_program_evaluation",
                                "description": "Get official municipal CAPES Sucupira stricto sensu postgraduate programs count, excellence 6-7 share, and mean quadrennial grade.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sucupira_scientific_production",
                                "description": "Get municipal CAPES Sucupira Qualis A1/A2 papers, defended theses/dissertations, patents/software, open access rate, and estimated grants.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sucupira_postgrad_features",
                                "description": "Get normalized Gold CAPES Sucupira postgraduate excellence density, scientific production/innovation scores, and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sucupira_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite CAPES Sucupira postgraduate and research ecosystem score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_terrabrasilis_prodes_deforestation",
                                "description": "Get official municipal INPE TerraBrasilis PRODES annual deforestation increment, accumulated deforestation, and remaining forest cover.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP (e.g. 1302603)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_terrabrasilis_esg_governance",
                                "description": "Get municipal INPE TerraBrasilis IBAMA embargoes, CAR regularization, ESG rural credit eligibility, and EUDR non-compliance risk.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_terrabrasilis_environmental_features",
                                "description": "Get normalized Gold INPE TerraBrasilis forest preservation, DETER pressure, composite ESG score, and 4-tier EUDR classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_terrabrasilis_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite INPE TerraBrasilis ESG compliance and low deforestation risk score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_hidroweb_stations",
                                "description": "Get official municipal ANA HidroWeb telemetric station network coverage, fluviometric/pluviometric counts, and transmission availability.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP (e.g. 4106902)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_hidroweb_water_security",
                                "description": "Get municipal ANA HidroWeb water quality (IQA), water security index (ISH), water balance commitment, and grant regularization.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_hidroweb_hydrology_features",
                                "description": "Get normalized Gold ANA HidroWeb hydro telemetry, river flow/level averages, composite water security score, and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_hidroweb_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite ANA HidroWeb water security and telemetry resilience score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_mapbiomas_land_cover",
                                "description": "Get official municipal MapBiomas annual native vegetation, agricultural area, urban surface, and water cover.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP (e.g. 1302603)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_mapbiomas_alerts",
                                "description": "Get municipal MapBiomas Alerta validated deforestation alerts, embargo overlaps, and protected-area incidence.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_mapbiomas_features",
                                "description": "Get normalized Gold MapBiomas land-cover preservation, agricultural sustainability, and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_mapbiomas_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite MapBiomas land-cover sustainability score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. AM)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sicar_properties",
                                "description": "Get official SFB/SICAR municipal rural property registry, CAR coverage, and IBAMA embargo-free rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sicar_pra_compliance",
                                "description": "Get municipal SFB/SICAR Environmental Regularization Program (PRA) adherence and recovery progress.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_sicar_features",
                                "description": "Get normalized Gold SFB/SICAR rural environmental compliance score and 4-tier EUDR/CAR classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sicar_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite SFB/SICAR rural environmental compliance score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_zarc_climate_aptitude",
                                "description": "Get official MAPA ZARC municipal agro-climatic aptitude, zoned crops count, and suitable planting decendios.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_zarc_proagro_coverage",
                                "description": "Get municipal MAPA ZARC PROAGRO, Rural Insurance (PSR) coverage, and climatic loss rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_zarc_features",
                                "description": "Get normalized Gold MAPA ZARC agro-climatic risk score and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_zarc_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite MAPA ZARC agro-climatic aptitude score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_ibama_infractions",
                                "description": "Lookup official IBAMA SIFISC environmental infraction notices by AI code, CNPJ/CPF, IBGE municipality code, UF, or name.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "AI number, CNPJ, IBGE 7-digit code, UF, or infractor/municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ibama_embargoes",
                                "description": "Lookup IBAMA environmental embargo orders (TAD) and linked CAR/EUDR risk properties by TAD/AI, CNPJ/CPF, IBGE, UF, or biome.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "TAD/AI number, CNPJ, IBGE code, UF, biome, or name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ibama_enforcement_features",
                                "description": "Get normalized Gold IBAMA enforcement compliance score, active embargoes, embargoed area (ha), and EUDR risk tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_ibama_municipalities",
                                "description": "List Brazilian municipalities ranked by IBAMA enforcement compliance score or highest embargo pressure.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PA)",
                                        },
                                        "sort_by_risk": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities with highest embargo pressure first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_bcb_sicor_custeio",
                                "description": "Lookup BACEN SICOR operating rural credit (custeio) by IBGE municipality code, UF, crop, or program.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "IBGE code, UF, crop product (e.g. Soja), program (PRONAF/PRONAMP), or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_bcb_sicor_pronaf_proagro",
                                "description": "Get municipal BACEN SICOR Pronaf family-farming share, Proagro coverage, RenovAgro ABC+ low-carbon share, and Res. BCB 140 ESG block rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_bcb_sicor_credit_features",
                                "description": "Get normalized Gold BACEN SICOR rural credit dynamism score, volume BRL, Proagro protection, and agricultural tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_bcb_sicor_municipalities",
                                "description": "List Brazilian municipalities ranked by BACEN SICOR rural credit dynamism score or Pronaf share.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. GO)",
                                        },
                                        "sort_by_pronaf": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest Pronaf share first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_ibge_agro_pam_crops",
                                "description": "Lookup IBGE PAM municipal agricultural crops (Tabela 5457) by IBGE code, UF, crop, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "IBGE code, UF, crop name (e.g. Soja, Cafe), or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ibge_agro_ppm_livestock",
                                "description": "Lookup IBGE PPM livestock herds and milk production (Tabela 3939) by IBGE code, UF, species, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "IBGE code, UF, species (e.g. Bovino), or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_ibge_agro_production_features",
                                "description": "Get normalized Gold IBGE Agro Municipal gross value (VBP), harvested hectares, cattle herd, and vocation tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_ibge_agro_municipalities",
                                "description": "List Brazilian municipalities ranked by IBGE agricultural power score or gross VBP value.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. GO)",
                                        },
                                        "sort_by_vbp": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest gross agro value (VBP) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cafir_properties",
                                "description": "Lookup RFB CAFIR/CIB rural properties by NIRF/CIB code, INCRA SNCR, CNPJ/CPF, IBGE municipality code, UF, or property/owner name.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "NIRF/CIB, SNCR code, CNPJ, IBGE 7-digit code, UF, or property/owner name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cafir_cnir_linkage",
                                "description": "Get municipal RFB CAFIR-CNIR/INCRA linkage rate, SIGEF georeferencing share, and CIB-CAR consistency.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_get_rfb_cafir_land_features",
                                "description": "Get normalized Gold RFB CAFIR rural land regularization score, CNIR rate, agrarian Gini index, and structure tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_rfb_cafir_municipalities",
                                "description": "List Brazilian municipalities ranked by RFB CAFIR/CNIR land regularization score or registered rural area.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                        "sort_by_area": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest total registered rural area (ha) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_inpe_queimadas_focos",
                                "description": "Lookup INPE active fire hotspots by hotspot code, satellite (AQUA/NPP-375), biome, IBGE municipality code, UF, or name.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Hotspot code, satellite, biome, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_seeg_emissoes_gee",
                                "description": "Lookup SEEG municipal greenhouse gas (GHG) emissions in tCO2e by emission code, sector (Agropecuaria, Energia, LULUCF), IBGE code, or UF.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Emission code, sector, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_queimadas_seeg_features",
                                "description": "Get normalized Gold INPE Queimadas & SEEG GHG climate resilience score, active fire hotspots count, and emissions tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_queimadas_seeg_municipalities",
                                "description": "List Brazilian municipalities ranked by INPE Queimadas & SEEG climate resilience score or net GHG emissions (tCO2e).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PA)",
                                        },
                                        "sort_by_emissions": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest net SEEG GHG emissions (tCO2e) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_aneel_mmgd_empreendimentos",
                                "description": "Lookup ANEEL distributed generation (MMGD) plants by plant code, source (UFV, UTE), class (Rural, Comercial), utility, IBGE code, or UF.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Plant code, generation type, class, utility name, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_aneel_mmgd_fontes",
                                "description": "Lookup ANEEL MMGD installed capacity (kW) and annual estimated MWh generation by renewable source and municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Source key, renewable source name, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_aneel_mmgd_features",
                                "description": "Get normalized Gold ANEEL MMGD energy transition score, installed solar/rural kW, and maturity tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_aneel_mmgd_municipalities",
                                "description": "List Brazilian municipalities ranked by ANEEL MMGD energy transition score or total installed kW.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. MT)",
                                        },
                                        "sort_by_power": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest total installed MMGD power (kW) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_anp_combustiveis_precos",
                                "description": "Lookup ANP weekly fuel retail and distribution prices (Gasolina, Etanol, Diesel S10) by product, survey code, IBGE code, UF, or municipality.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Fuel product, survey code, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_anp_combustiveis_vendas",
                                "description": "Lookup ANP biofuel (Ethanol/Biodiesel) and petroleum derivative sales volume (m3) by code, category, IBGE, or UF.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Sales code, fuel category, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_anp_combustiveis_features",
                                "description": "Get normalized Gold ANP fuel competitiveness score, Diesel S10 price, biofuel share, and matrix tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_anp_combustiveis_municipalities",
                                "description": "List Brazilian municipalities ranked by ANP fuel competitiveness score or highest biofuel share (%).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                        "sort_by_biofuel": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest biofuel share (%) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_mdr_snis_agua_esgoto",
                                "description": "Lookup SNIS/SINISA municipal water supply, sewage coverage, treatment rate, and distribution losses by code, utility, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "SNIS code, utility name, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mdr_snis_residuos",
                                "description": "Lookup SNIS/SINISA municipal solid waste collection, landfill adequacy, and recycling metrics.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "SNIS RSU code, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mdr_snis_features",
                                "description": "Get normalized Gold SNIS/SINISA sanitation universalization score, water/sewage coverage, and 2033 readiness tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_mdr_snis_municipalities",
                                "description": "List Brazilian municipalities ranked by SNIS sanitation universalization score or sewage coverage (%).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                        "sort_by_sewage": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest total sewage coverage (%) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_sedec_desastres_ocorrencias",
                                "description": "Lookup S2ID/COBRADE municipal natural disaster occurrences and economic damages by code, typology, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "S2ID code, COBRADE typology, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_sedec_decretos_reconhecimento",
                                "description": "Lookup federal emergency (SE) and calamity (ECP) decrees and federal relief funds by code, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Decree code, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_sedec_desastres_features",
                                "description": "Get normalized Gold SEDEC/CEMADEN disaster resilience score, 5-year losses, and climate vulnerability tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_sedec_resilient_municipalities",
                                "description": "List Brazilian municipalities ranked by SEDEC disaster resilience score or highest economic damages (R$ mil).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. RS)",
                                        },
                                        "sort_by_damages": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest accumulated economic damages first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_mme_ons_geracao_sin",
                                "description": "Lookup National Interconnected System (SIN) power plants by ONS code, CEG, source, subsystem, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "ONS code, CEG, energy source, subsystem, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mme_ons_balanco_carga",
                                "description": "Lookup ONS subsystem electrical load (MWmed), reservoir stored energy EAR (%), and CMO by code, subsystem, IBGE, UF, or city.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "Balance code, subsystem, IBGE 7-digit code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_mme_ons_features",
                                "description": "Get normalized Gold MME/ONS energy security score, installed SIN MW, renewable share, and grid tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_mme_ons_municipalities",
                                "description": "List Brazilian municipalities ranked by MME/ONS energy security score or SIN renewable generation share (%).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. CE)",
                                        },
                                        "sort_by_renewable": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest renewable generation share (%) first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_anm_sigmine_processes",
                                "description": "Lookup ANM SIGMINE mining processes by ANM process code, mineral substance, phase, IBGE code, UF, or municipality name.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "ANM process code, mineral substance (e.g. FERRO), IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_anm_cfem_royalties",
                                "description": "Lookup ANM CFEM mineral royalty collections by guide code, main substance, IBGE code, UF, or municipality name.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "termo": {
                                            "type": "string",
                                            "description": "CFEM guide code, mineral substance, IBGE code, UF, or municipality name",
                                        }
                                    },
                                    "required": ["termo"],
                                },
                            },
                            {
                                "name": "bdg_get_anm_features",
                                "description": "Get Gold ANM mineral ESG maturity score, CFEM revenue, SIGBM dam conformity, and sustainable mining tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio_or_name": {
                                            "type": "string",
                                            "description": "7-digit IBGE code or municipality name",
                                        }
                                    },
                                    "required": ["id_municipio_or_name"],
                                },
                            },
                            {
                                "name": "bdg_list_top_anm_municipalities",
                                "description": "List Brazilian municipalities ranked by ANM mineral ESG maturity score or annual CFEM royalty collections.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. MG)",
                                        },
                                        "sort_by_cfem": {
                                            "type": "boolean",
                                            "description": "Set true to rank municipalities by highest annual CFEM royalties first",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_bd_coverage",
                                "description": "Get official Federated Public Datalake (DuckDB / BigQuery) municipal treated dataset and table coverage metrics.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_bd_governance",
                                "description": "Get municipal Federated Public Datalake dictionary standardization and canonical key governance metrics.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_bd_features",
                                "description": "Get normalized Gold Federated Public Datalake maturity score and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_bd_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite Federated Public Datalake maturity score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_ckan_organizations",
                                "description": "Get municipal Dados.gov.br (CKAN) publishing organizations, open dataset count, and PDA adherence.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ckan_governance",
                                "description": "Get municipal Dados.gov.br (CKAN) open license, metadata dictionary, and LAI governance metrics.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_ckan_features",
                                "description": "Get normalized Gold Dados.gov.br (CKAN) open data transparency score and 4-tier classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP",
                                        }
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_ckan_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite Dados.gov.br (CKAN) transparency score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. PR)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cross_domain_features",
                                "description": "Get consolidated cross-domain ML Feature Store vector for a Brazilian municipality (territorial, B2B, macro ASOF, ESG/Agro, human capital & health).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "6- or 7-digit IBGE municipality code (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_cross_domain_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite cross-domain B2B propensity score.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10, max: 100)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_solution_pack_insights",
                                "description": "Get commercial and operational insights by Solution Pack (P01..P10) and Brazilian state (UF).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "pack_id": {
                                            "type": "string",
                                            "description": "Solution Pack ID (P01 to P10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. MG)",
                                        },
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10, max: 27)",
                                        },
                                    },
                                    "required": ["pack_id"],
                                },
                            },
                            {
                                "name": "bdg_get_lifecycle_stage_report",
                                "description": "Query a 9-stage commercial & delivery lifecycle mart (S01..S09) for a Solution Pack (P01..P10).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "stage_id": {
                                            "type": "string",
                                            "description": "Lifecycle stage ID (S01..S09)",
                                        },
                                        "pack_id": {
                                            "type": "string",
                                            "description": "Solution Pack ID (P01..P10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter",
                                        },
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "Optional 6- or 7-digit IBGE municipality code (for S01)",
                                        },
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                    },
                                    "required": ["stage_id", "pack_id"],
                                },
                            },
                            {
                                "name": "bdg_generate_solution_dossier",
                                "description": "Generate a unified 9-stage (S01..S09) commercial & delivery dossier for a Solution Pack (P01..P10) in a Brazilian state.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "pack_id": {
                                            "type": "string",
                                            "description": "Solution Pack ID (P01..P10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "2-letter state abbreviation (e.g. SP)",
                                        },
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "Optional IBGE municipality code",
                                        },
                                    },
                                    "required": ["pack_id", "sigla_uf"],
                                },
                            },
                            {
                                "name": "bdg_list_public_apis_catalog",
                                "description": "List curated Phase 10 Public & Global APIs (KYC/Geo, Market, Spatial ML, Global Research) filtered by block or Solution Pack.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "block": {
                                            "type": "string",
                                            "description": "Optional block filter (BLOCO_1_KYC_GEO, BLOCO_2_MERCADO_REGULATORIO, BLOCO_3_ESPACIAL_CLIMA_ML, BLOCO_4_GLOBAL_PESQUISA)",
                                        },
                                        "pack_id": {
                                            "type": "string",
                                            "description": "Optional Solution Pack filter (P01..P10)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_enrich_cnpj_cascade",
                                "description": "Enrich a Brazilian CNPJ using automatic cascade fallback (Minha Receita -> CNPJa Open -> BrasilAPI -> ReceitaWS).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cnpj": {"type": "string", "description": "14-digit CNPJ"},
                                    },
                                    "required": ["cnpj"],
                                },
                            },
                            {
                                "name": "bdg_count_osm_pois_radius",
                                "description": "Count OpenStreetMap POIs (schools, pharmacies, banks, etc.) within a radius in meters via Overpass API.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "lat": {"type": "number", "description": "Latitude"},
                                        "lon": {"type": "number", "description": "Longitude"},
                                        "amenity": {"type": "string", "description": "OSM amenity tag (default: school)"},
                                        "radius_m": {"type": "integer", "description": "Radius in meters (default: 1000)"},
                                    },
                                    "required": ["lat", "lon"],
                                },
                            },
                            {
                                "name": "bdg_fetch_ipeadata_series",
                                "description": "Fetch historical macroeconomic/regional time series from IPEA Data OData v4.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "sercodigo": {"type": "string", "description": "IPEA series code (e.g. BM12_TJOVER12)"},
                                        "limit": {"type": "integer", "description": "Max records (default: 24)"},
                                    },
                                    "required": ["sercodigo"],
                                },
                            },
                            {
                                "name": "bdg_fetch_brapi_quote",
                                "description": "Fetch B3 equities, ETFs, and FII quotes from brapi.dev.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "tickers": {"type": "string", "description": "Comma-separated B3 tickers (e.g. PETR4)"},
                                    },
                                    "required": ["tickers"],
                                },
                            },
                            {
                                "name": "bdg_fetch_awesomeapi_rates",
                                "description": "Fetch real-time FX and cryptocurrency quotes from AwesomeAPI.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "pairs": {"type": "string", "description": "Currency pairs (default: USD-BRL,EUR-BRL,BTC-BRL)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_query_ckan_datastore",
                                "description": "Query ANEEL or ANP CKAN DataStore regulatory datasets.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "resource_id": {"type": "string", "description": "CKAN resource UUID"},
                                        "portal": {"type": "string", "description": "aneel or anp (default: aneel)"},
                                        "limit": {"type": "integer", "description": "Max records (default: 20)"},
                                    },
                                    "required": ["resource_id"],
                                },
                            },
                            {
                                "name": "bdg_fetch_camara_deputados",
                                "description": "Fetch active federal deputies from Camara dos Deputados Open Data API.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "pagina": {"type": "integer", "description": "Page number (default: 1)"},
                                        "itens": {"type": "integer", "description": "Items per page (default: 20)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_fetch_world_bank_indicator",
                                "description": "Fetch World Bank v2 macroeconomic indicator series.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "country": {"type": "string", "description": "ISO-3 country code (default: BRA)"},
                                        "indicator": {"type": "string", "description": "Indicator ID (default: NY.GDP.MKTP.KD.ZG)"},
                                        "per_page": {"type": "integer", "description": "Max records (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_fetch_who_gho_indicator",
                                "description": "Fetch WHO Global Health Observatory OData indicator records.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "indicator": {"type": "string", "description": "WHO GHO indicator code (default: WHOSIS_000001)"},
                                        "top": {"type": "integer", "description": "Max records (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_compute_osrm_route",
                                "description": "Compute real road distance (km) and travel duration (min) via OSRM.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "lat1": {"type": "number"},
                                        "lon1": {"type": "number"},
                                        "lat2": {"type": "number"},
                                        "lon2": {"type": "number"},
                                    },
                                    "required": ["lat1", "lon1", "lat2", "lon2"],
                                },
                            },
                            {
                                "name": "bdg_fetch_open_meteo_forecast",
                                "description": "Fetch Open-Meteo daily weather forecast variables.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "lat": {"type": "number"},
                                        "lon": {"type": "number"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_fetch_nasa_power_daily",
                                "description": "Fetch NASA POWER solar radiation and agro-climate variables.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "lat": {"type": "number"},
                                        "lon": {"type": "number"},
                                        "start": {"type": "string"},
                                        "end": {"type": "string"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_fetch_elevation",
                                "description": "Fetch SRTM terrain altitude in meters via Open-Elevation.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "lat": {"type": "number"},
                                        "lon": {"type": "number"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_describe_upgini_features",
                                "description": "Describe Upgini public ML feature groups.",
                                "inputSchema": {"type": "object", "properties": {}},
                            },
                            {
                                "name": "bdg_geocode_nominatim",
                                "description": "Forward geocode an address using OpenStreetMap Nominatim.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"},
                                        "limit": {"type": "integer"},
                                    },
                                    "required": ["query"],
                                },
                            },
                            {
                                "name": "bdg_lookup_rdap_registro_br",
                                "description": "Lookup .br domain registration and age metadata via RDAP Registro.br.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"domain": {"type": "string"}},
                                    "required": ["domain"],
                                },
                            },
                            {
                                "name": "bdg_lookup_gleif_lei",
                                "description": "Search global Legal Entity Identifiers (LEI) in GLEIF API.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"},
                                        "limit": {"type": "integer"},
                                    },
                                    "required": ["query"],
                                },
                            },
                            {
                                "name": "bdg_lookup_ip_intel",
                                "description": "Lookup IP ASN, reverse DNS, and geolocation via BGPView / ipwho.is.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"ip": {"type": "string"}},
                                    "required": ["ip"],
                                },
                            },
                            {
                                "name": "bdg_lookup_b2b_domain_profile",
                                "description": "Lookup optional freemium B2B corporate domain profile.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"domain": {"type": "string"}},
                                    "required": ["domain"],
                                },
                            },
                            {
                                "name": "bdg_lookup_country",
                                "description": "Normalize country ISO-3166 metadata via REST Countries v3.1.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"code": {"type": "string"}},
                                    "required": ["code"],
                                },
                            },
                            {
                                "name": "bdg_search_openalex_works",
                                "description": "Search open scientific works & citations via OpenAlex API.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"},
                                        "per_page": {"type": "integer"},
                                    },
                                    "required": ["query"],
                                },
                            },
                            {
                                "name": "bdg_query_gdelt_doc",
                                "description": "Query global news articles and sentiment streams via GDELT Project Doc 2.0.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"},
                                        "maxrecords": {"type": "integer"},
                                    },
                                    "required": ["query"],
                                },
                            },
                            {
                                "name": "bdg_get_onr_property_registry",
                                "description": "Get official ONR SREI municipal active property registry CNM stock, digital conversion rate, and online certificate emissions.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name (e.g. 3550308)",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_onr_real_estate_credit_guarantees",
                                "description": "Get municipal fiduciary lien real estate credit volume, SFI/SBPE financing share, Home Equity CGI contracts, and foreclosure execution rate (ONR).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_onr_real_estate_features",
                                "description": "Get normalized Gold ONR/SREI SERP digital maturity score, real estate liquidity/security score, and 4-tier municipal classification.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_onr_real_estate_municipalities",
                                "description": "List top Brazilian municipalities ranked by composite ONR SREI/SERP digital maturity and real estate liquidity scores.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_iptu_itbi_property_cadastre",
                                "description": "Query municipal IPTU property cadastre, taxed area, PGV venal value, revenue, and compliance rate.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_iptu_itbi_price_dynamics",
                                "description": "Query municipal ITBI declared price per m², YoY appreciation, PGV-to-market ratio, and real estate liquidity.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_iptu_itbi_real_estate_features",
                                "description": "Query Gold IPTU/ITBI market dynamism score, fiscal CTM governance score, and analytical tier.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {
                                            "type": "string",
                                            "description": "7-digit IBGE municipality code, 6-digit code, 8-digit CEP, or municipality name",
                                        },
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_iptu_itbi_capitals",
                                "description": "List top Brazilian capitals ranked by ITBI real estate dynamism and CTM/PGV fiscal governance.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum rows (default: 10)",
                                        },
                                        "sigla_uf": {
                                            "type": "string",
                                            "description": "Optional 2-letter state filter (e.g. SP)",
                                        },
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_labor_force_indicators",
                                "description": "Query official quarterly labor force, population, and unemployment rate by Brazilian UF from IBGE PNAD Contínua (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "uf": {"type": "string", "description": "IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)"},
                                        "trimestre": {"type": "string", "description": "Optional quarter in AAAA-TT format (e.g. 2026-T2)"},
                                    },
                                    "required": ["uf"],
                                },
                            },
                            {
                                "name": "bdg_get_state_labor_profile",
                                "description": "Query Gold labor resilience profile, informality rate, and real habitual earnings by Brazilian UF from IBGE PNAD Contínua (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "uf": {"type": "string", "description": "IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)"},
                                    },
                                    "required": ["uf"],
                                },
                            },
                            {
                                "name": "bdg_list_lowest_unemployment_states",
                                "description": "List Brazilian UFs with lowest unemployment rate from IBGE PNAD Contínua (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max states to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_list_highest_earnings_states",
                                "description": "List Brazilian UFs with highest average real monthly earnings from IBGE PNAD Contínua (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max states to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_cnefe_municipality_summary",
                                "description": "Query IBGE CNEFE address totals, building species and density by municipality from Censo Demográfico 2022 (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "municipio": {"type": "string", "description": "IBGE 7-digit municipality code or name"},
                                    },
                                    "required": ["municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_cnefe_cep_profile",
                                "description": "Query IBGE CNEFE address counts, centroid coordinates and mixed-use index for an 8-digit CEP (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "cep": {"type": "string", "description": "8-digit Brazilian postal code (CEP)"},
                                    },
                                    "required": ["cep"],
                                },
                            },
                            {
                                "name": "bdg_list_cnefe_building_types",
                                "description": "List IBGE CNEFE building species and municipal purpose breakdown (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "municipio": {"type": "string", "description": "IBGE 7-digit municipality code or name"},
                                    },
                                    "required": ["municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_cnefe_top_address_density",
                                "description": "List Brazilian municipalities ranked by address density per km2 from IBGE CNEFE (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_get_pib_municipal_detail",
                                "description": "Query official IBGE municipal GDP accounts, sectoral VAB, taxes, and relative shares (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                        "year": {"type": "integer", "description": "Reference year (default: 2023)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_get_pib_municipal_features",
                                "description": "Query Gold economic features, diversification score, and economic vocation from IBGE municipal accounts (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id_municipio": {"type": "string", "description": "IBGE 7-digit municipality code (e.g. 3550308)"},
                                    },
                                    "required": ["id_municipio"],
                                },
                            },
                            {
                                "name": "bdg_list_top_municipal_pib",
                                "description": "List top Brazilian municipalities ranked by total municipal GDP from IBGE (Tier 1).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "limit": {"type": "integer", "description": "Max municipalities to retrieve (default: 10)"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_lakehouse_sla_audit",
                                "description": "Audit freshness SLA hours, update frequency, and cron schedules across cataloged sources (Phase 11).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "source_id": {"type": "string", "description": "Optional source_id or api_id filter"},
                                        "include_public_apis": {"type": "boolean", "description": "Include curated public APIs registry"},
                                    },
                                },
                            },
                            {
                                "name": "bdg_lakehouse_cdc_plan",
                                "description": "Compute incremental CDC delta plan comparing local Silver/Gold Parquet SHA-256 against watermark state (Phase 11).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "layers": {"type": "array", "items": {"type": "string"}},
                                        "sources": {"type": "array", "items": {"type": "string"}},
                                    },
                                },
                            },
                            {
                                "name": "bdg_lakehouse_mirror_sync",
                                "description": "Plan or execute Cloud Lakehouse Mirror sync (local | s3 | r2) for Silver/Gold Parquet snapshots (Phase 11).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "provider": {"type": "string", "enum": ["local", "s3", "r2"]},
                                        "dry_run": {"type": "boolean"},
                                        "layers": {"type": "array", "items": {"type": "string"}},
                                        "sources": {"type": "array", "items": {"type": "string"}},
                                    },
                                },
                            },
                            {
                                "name": "bdg_lakehouse_warehouse_ddl",
                                "description": "Generate serverless SQL DDL for DuckDB, MotherDuck, or BigQuery external tables/views (Phase 11).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "provider": {"type": "string", "enum": ["duckdb", "motherduck", "bigquery"]},
                                        "layers": {"type": "array", "items": {"type": "string"}},
                                        "sources": {"type": "array", "items": {"type": "string"}},
                                    },
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

        if tool_name == "bdg_get_simples_status":
            from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
                RFBSimplesMEIConnector,
            )

            conn = RFBSimplesMEIConnector(settings=self.settings)
            status_data = conn.get_simples_mei_status(args["cnpj"])
            wrapped = wrap_untrusted_gov_data(status_data, source_id="rfb_simples_mei")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_pncp_contratacoes":
            from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
                PNCPContratacoesConnector,
            )

            conn = PNCPContratacoesConnector(settings=self.settings)
            res = conn.list_contratacoes(
                uf=args.get("uf"),
                modalidade_id=args.get("modalidade_id"),
                ano=args.get("ano"),
                limit=args.get("limit", 10),
            )
            wrapped = wrap_untrusted_gov_data(res, source_id="pncp_contratacoes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_pncp_contratos":
            from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
                PNCPContratacoesConnector,
            )

            conn = PNCPContratacoesConnector(settings=self.settings)
            res = conn.list_contratos(
                cnpj_orgao=args.get("cnpj_orgao"),
                ni_fornecedor=args.get("ni_fornecedor"),
                ano=args.get("ano"),
                limit=args.get("limit", 10),
            )
            wrapped = wrap_untrusted_gov_data(res, source_id="pncp_contratacoes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_compliance_status", "pdg_get_compliance_status"}:
            from public_data_gateway.connectors.compliance.cgu_ceis_cnep.connector import (
                CGUCEISCNEPConnector,
            )

            conn = CGUCEISCNEPConnector(settings=self.settings)
            doc = args.get("documento") or args.get("cnpj_or_cpf") or ""
            res = conn.get_compliance_status(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="cgu_ceis_cnep")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_pep_status", "pdg_get_pep_status"}:
            from public_data_gateway.connectors.compliance.cgu_pep.connector import (
                CGUPEPConnector,
            )

            conn = CGUPEPConnector(settings=self.settings)
            termo = args.get("termo") or args.get("cpf_or_name") or ""
            res = conn.get_pep_status(termo)
            wrapped = wrap_untrusted_gov_data(res, source_id="cgu_pep")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_litigation_risk", "pdg_get_litigation_risk"}:
            from public_data_gateway.connectors.judicial.cnj_datajud.connector import (
                CNJDataJudConnector,
            )

            conn = CNJDataJudConnector(settings=self.settings)
            doc = args.get("documento") or args.get("cnpj_or_cpf") or ""
            res = conn.get_litigation_risk(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="cnj_datajud")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_cndt_certificate", "pdg_get_cndt_certificate"}:
            from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                TstCndtConnector,
            )

            tst_conn = TstCndtConnector(settings=self.settings)
            doc = args.get("documento") or args.get("cnpj_or_cpf") or ""
            res = tst_conn.get_cndt(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="tst_cndt")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_labor_risk", "pdg_get_labor_risk"}:
            from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                TstCndtConnector,
            )

            tst_conn = TstCndtConnector(settings=self.settings)
            doc = args.get("documento") or args.get("cnpj_or_cpf") or ""
            res = tst_conn.get_labor_risk(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="tst_cndt")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_justica_aberta_offices",
            "bdg_get_justica_aberta_emoluments",
            "bdg_get_justica_aberta_features",
            "bdg_list_top_justica_aberta_municipalities",
        }:
            from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
                CNJJusticaAbertaConnector,
            )

            ja_conn = CNJJusticaAbertaConnector(settings=self.settings)
            if tool_name == "bdg_get_justica_aberta_offices":
                result = ja_conn.get_notary_offices_registry(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_justica_aberta_emoluments":
                result = ja_conn.get_emoluments_revenue(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_justica_aberta_features":
                result = ja_conn.get_justica_aberta_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = ja_conn.list_top_notary_efficiency_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No CNJ Justiça Aberta data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="cnj_justica_aberta")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_suframa_status", "pdg_get_suframa_status"}:
            from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
                SuframaPimZfmConnector,
            )

            suf_conn = SuframaPimZfmConnector(settings=self.settings)
            query = args.get("cnpj_or_inscricao") or args.get("documento") or args.get("cnpj") or ""
            res = suf_conn.get_suframa_status(query)
            wrapped = wrap_untrusted_gov_data(res, source_id="suframa_pim_zfm")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_tax_incentives_profile", "pdg_get_tax_incentives_profile"}:
            from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
                SuframaPimZfmConnector,
            )

            suf_conn = SuframaPimZfmConnector(settings=self.settings)
            cnpj_val = args.get("cnpj") or args.get("documento") or ""
            res = suf_conn.get_tax_incentives_profile(cnpj_val)
            wrapped = wrap_untrusted_gov_data(res, source_id="suframa_pim_zfm")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_cvm_company", "pdg_get_cvm_company"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
                CvmDfpItrConnector,
            )

            cvm_conn = CvmDfpItrConnector(settings=self.settings)
            doc = args.get("cnpj_or_codigo") or args.get("documento") or ""
            res = cvm_conn.get_company(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="cvm_dfp_itr")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_financial_ratios", "pdg_get_financial_ratios"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
                CvmDfpItrConnector,
            )

            cvm_conn = CvmDfpItrConnector(settings=self.settings)
            doc = args.get("cnpj_or_codigo") or args.get("documento") or ""
            res = cvm_conn.get_financial_ratios(doc)
            wrapped = wrap_untrusted_gov_data(res, source_id="cvm_dfp_itr")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_municipality_trade", "pdg_get_municipality_trade"}:
            from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
                MDICComexStatConnector,
            )

            comex_conn = MDICComexStatConnector(settings=self.settings)
            m_id = str(args.get("id_municipio") or args.get("codigo_municipio") or "").strip()
            res = comex_conn.get_municipality_trade(m_id)
            wrapped = wrap_untrusted_gov_data(res, source_id="mdic_comexstat")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_ncm_trade_profile", "pdg_get_ncm_trade_profile"}:
            from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
                MDICComexStatConnector,
            )

            comex_conn = MDICComexStatConnector(settings=self.settings)
            ncm_code = str(args.get("codigo_ncm") or args.get("ncm_8") or "").strip()
            res = comex_conn.get_ncm_trade(ncm_code)
            wrapped = wrap_untrusted_gov_data(res, source_id="mdic_comexstat")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_fund_profile", "pdg_get_fund_profile"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
                CvmFundosConnector,
            )

            fund_conn = CvmFundosConnector(settings=self.settings)
            cnpj_val = str(args.get("cnpj") or "").strip()
            res = fund_conn.get_fund_by_cnpj(cnpj_val) or {}
            features = fund_conn.get_fund_features(cnpj_val)
            if features:
                res["gold_features"] = features
            wrapped = wrap_untrusted_gov_data(res, source_id="cvm_fundos")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_asset_manager_funds", "pdg_get_asset_manager_funds"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
                CvmFundosConnector,
            )

            fund_conn = CvmFundosConnector(settings=self.settings)
            q = args.get("gestor_query")
            min_aum = float(args.get("min_aum") or 0.0)
            managers = fund_conn.get_asset_managers(query=q, min_aum=min_aum)
            wrapped = wrap_untrusted_gov_data(managers, source_id="cvm_fundos")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_bank_financial_profile", "pdg_get_bank_financial_profile"}:
            from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
                BcbIfdataConnector,
            )

            bank_conn = BcbIfdataConnector(settings=self.settings)
            cnpj = args.get("cnpj", "")
            bank = bank_conn.get_bank_by_cnpj(cnpj)
            if not bank:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Financial institution not found for '{cnpj}'",
                            }
                        ],
                        "isError": True,
                    },
                }
            sheet = bank_conn.get_bank_balance_sheet(cnpj)
            sol = bank_conn.get_bank_solidity(cnpj)
            payload = {
                "instituicao": bank,
                "balanco": sheet,
                "solidez": sol,
            }
            wrapped = wrap_untrusted_gov_data(payload, source_id="bcb_ifdata")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_banking_ranking", "pdg_get_banking_ranking"}:
            from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
                BcbIfdataConnector,
            )

            bank_conn = BcbIfdataConnector(settings=self.settings)
            segmento = args.get("segmento")
            top = int(args.get("top") or 10)
            ranking = bank_conn.get_banking_ranking(segmento=segmento, top=top)
            wrapped = wrap_untrusted_gov_data(ranking, source_id="bcb_ifdata")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_municipality_fiscal_health", "pdg_get_municipality_fiscal_health"}:
            from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
                StnTesouroTransparenteConnector,
            )

            treas_conn = StnTesouroTransparenteConnector(settings=self.settings)
            termo = args.get("id_municipio_or_name", "")
            prof = treas_conn.get_municipio_fiscal_profile(termo)
            if not prof:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Fiscal health profile not found for '{termo}'",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(prof, source_id="stn_tesouro_transparente")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_sadipem_credit_operations", "pdg_get_sadipem_credit_operations"}:
            from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
                StnTesouroTransparenteConnector,
            )

            treas_conn = StnTesouroTransparenteConnector(settings=self.settings)
            termo = args.get("termo", "")
            st = args.get("status")
            pvls = treas_conn.get_credit_operations_pvl(termo, status=st)
            wrapped = wrap_untrusted_gov_data(pvls, source_id="stn_tesouro_transparente")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_siconfi_fiscal_profile":
            from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
                StnSiconfiConnector,
            )

            sic_conn = StnSiconfiConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            prof = sic_conn.get_siconfi_features(termo)
            if not prof:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"SICONFI fiscal profile not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(prof, source_id="stn_siconfi")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_siconfi_rgf_limits":
            from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
                StnSiconfiConnector,
            )

            sic_conn = StnSiconfiConnector(settings=self.settings)
            ano_val = int(args["ano"]) if args.get("ano") is not None else None
            rows = sic_conn.get_rgf_limits(str(args.get("id_municipio", "")), ano=ano_val)
            wrapped = wrap_untrusted_gov_data(rows, source_id="stn_siconfi")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_siconfi_dca_balance":
            from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
                StnSiconfiConnector,
            )

            sic_conn = StnSiconfiConnector(settings=self.settings)
            ano_val = int(args["ano"]) if args.get("ano") is not None else None
            rows = sic_conn.get_dca_balance(str(args.get("id_municipio", "")), ano=ano_val)
            wrapped = wrap_untrusted_gov_data(rows, source_id="stn_siconfi")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_siconfi_municipalities":
            from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
                StnSiconfiConnector,
            )

            sic_conn = StnSiconfiConnector(settings=self.settings)
            rows = sic_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="stn_siconfi")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_pgfn_debtor_inscriptions":
            from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
                PgfnDividaAtivaConnector,
            )

            pgfn_conn = PgfnDividaAtivaConnector(settings=self.settings)
            rows = pgfn_conn.get_debtor_inscriptions(str(args.get("documento_or_name", "")))
            wrapped = wrap_untrusted_gov_data(rows, source_id="pgfn_divida_ativa")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_pgfn_municipal_summary":
            from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
                PgfnDividaAtivaConnector,
            )

            pgfn_conn = PgfnDividaAtivaConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            summary = pgfn_conn.get_municipal_summary(termo)
            if not summary:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"PGFN municipal summary not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(summary, source_id="pgfn_divida_ativa")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_pgfn_risk_features":
            from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
                PgfnDividaAtivaConnector,
            )

            pgfn_conn = PgfnDividaAtivaConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = pgfn_conn.get_pgfn_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"PGFN risk features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="pgfn_divida_ativa")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_pgfn_municipalities":
            from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
                PgfnDividaAtivaConnector,
            )

            pgfn_conn = PgfnDividaAtivaConnector(settings=self.settings)
            rows = pgfn_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="pgfn_divida_ativa")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sinesp_ocorrencias",
            "bdg_get_sinesp_vitimas_mvi",
            "bdg_get_sinesp_seguranca_features",
            "bdg_list_top_sinesp_seguranca_municipalities",
        }:
            from public_data_gateway.connectors.compliance.sinesp_seguranca import (
                SinespSegurancaConnector,
            )

            connector = SinespSegurancaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_sinesp_ocorrencias":
                    result: Any = connector.get_sinesp_crimes(str(args.get("termo", "")))
                elif tool_name == "bdg_get_sinesp_vitimas_mvi":
                    result = connector.get_sinesp_mvi_victims(str(args.get("termo", "")))
                elif tool_name == "bdg_get_sinesp_seguranca_features":
                    result = connector.get_sinesp_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_safest_sinesp_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_cargo_theft=bool(args.get("sort_by_cargo_theft", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"SINESP Seguranca parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No SINESP public safety data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="sinesp_seguranca")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_cgu_gastos_diretos",
            "bdg_get_cgu_transferencias_fpm",
            "bdg_get_cgu_transparencia_features",
            "bdg_list_top_cgu_transparencia_municipalities",
        }:
            from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
                CguTransparenciaGastosConnector,
            )

            connector = CguTransparenciaGastosConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_cgu_gastos_diretos":
                    result: Any = connector.get_cgu_direct_expenses(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_cgu_transferencias_fpm":
                    result = connector.get_cgu_constitutional_transfers(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_cgu_transparencia_features":
                    result = connector.get_cgu_expenses_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_cgu_transparency_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_spending=bool(args.get("sort_by_spending", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"CGU Transparência parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No CGU Portal da Transparência data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="cgu_transparencia_gastos")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_cgu_servidores_lotacao",
            "bdg_get_cgu_servidores_folha",
            "bdg_get_cgu_servidores_features",
            "bdg_list_top_cgu_servidores_municipalities",
        }:
            from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
                CguServidoresFederaisConnector,
            )

            connector = CguServidoresFederaisConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_cgu_servidores_lotacao":
                    result: Any = connector.get_siape_active_staff(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_cgu_servidores_folha":
                    result = connector.get_siape_payroll(str(args.get("termo", "")))
                elif tool_name == "bdg_get_cgu_servidores_features":
                    result = connector.get_siape_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_siape_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_payroll=bool(args.get("sort_by_payroll", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"CGU/SIAPE Servidores parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No CGU/SIAPE federal staff data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="cgu_servidores_federais")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_tse_candidaturas",
            "bdg_get_tse_votacao",
            "bdg_get_tse_features",
            "bdg_list_top_tse_municipalities",
        }:
            from public_data_gateway.connectors.compliance.tse_eleicoes import (
                TseEleicoesConnector,
            )

            connector = TseEleicoesConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_tse_candidaturas":
                    result: Any = connector.get_candidacies(str(args.get("termo", "")))
                elif tool_name == "bdg_get_tse_votacao":
                    result = connector.get_turnout_stats(str(args.get("termo", "")))
                elif tool_name == "bdg_get_tse_features":
                    result = connector.get_electoral_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_electoral_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_electorate=bool(args.get("sort_by_electorate", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"TSE Eleições parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No TSE electoral data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="tse_eleicoes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_cnciai_improbidade_condemnations":
            from public_data_gateway.connectors.judicial.cnj_improbidade import (
                CnjImprobidadeConnector,
            )

            imp_conn = CnjImprobidadeConnector(settings=self.settings)
            rows = imp_conn.get_condemnations(str(args.get("documento_or_name", "")))
            wrapped = wrap_untrusted_gov_data(rows, source_id="cnj_improbidade")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_cnciai_municipal_summary":
            from public_data_gateway.connectors.judicial.cnj_improbidade import (
                CnjImprobidadeConnector,
            )

            imp_conn = CnjImprobidadeConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            summary = imp_conn.get_municipal_summary(termo)
            if not summary:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"CNJ CNCIAI municipal summary not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(summary, source_id="cnj_improbidade")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_cnciai_risk_features":
            from public_data_gateway.connectors.judicial.cnj_improbidade import (
                CnjImprobidadeConnector,
            )

            imp_conn = CnjImprobidadeConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = imp_conn.get_improbidade_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"CNJ CNCIAI risk features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="cnj_improbidade")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_cnciai_municipalities":
            from public_data_gateway.connectors.judicial.cnj_improbidade import (
                CnjImprobidadeConnector,
            )

            imp_conn = CnjImprobidadeConnector(settings=self.settings)
            rows = imp_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="cnj_improbidade")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_capag_municipal_evaluation":
            from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
                StnCapagConnector,
            )

            cap_conn = StnCapagConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            ev = cap_conn.get_municipal_evaluation(termo)
            if not ev:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"STN CAPAG municipal evaluation not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(ev, source_id="stn_capag")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_capag_ifgf_gestao_fiscal":
            from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
                StnCapagConnector,
            )

            cap_conn = StnCapagConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            ifgf = cap_conn.get_ifgf_index(termo)
            if not ifgf:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"FIRJAN IFGF index not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(ifgf, source_id="stn_capag")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_capag_sovereign_features":
            from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
                StnCapagConnector,
            )

            cap_conn = StnCapagConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = cap_conn.get_capag_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"STN CAPAG sovereign features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="stn_capag")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_capag_municipalities":
            from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
                StnCapagConnector,
            )

            cap_conn = StnCapagConnector(settings=self.settings)
            rows = cap_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="stn_capag")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_bndes_operacoes",
            "bdg_get_bndes_desembolsos",
            "bdg_get_bndes_features",
            "bdg_list_top_bndes_municipalities",
        }:
            from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
                BndesOperacoesConnector,
            )

            connector = BndesOperacoesConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_bndes_operacoes":
                    result: Any = connector.get_credit_operations(str(args.get("termo", "")))
                elif tool_name == "bdg_get_bndes_desembolsos":
                    result = connector.get_disbursements(str(args.get("termo", "")))
                elif tool_name == "bdg_get_bndes_features":
                    result = connector.get_bndes_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_bndes_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_volume=bool(args.get("sort_by_volume", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"BNDES Operações parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No BNDES development financing data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="bndes_operacoes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_cvm_ofertas_rcvm160",
            "bdg_get_cvm_administradores",
            "bdg_get_cvm_extra_features",
            "bdg_list_top_cvm_extra_municipalities",
        }:
            from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
                CvmMercadoExtraConnector,
            )

            connector = CvmMercadoExtraConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_cvm_ofertas_rcvm160":
                    result: Any = connector.get_rcvm160_offerings(str(args.get("termo", "")))
                elif tool_name == "bdg_get_cvm_administradores":
                    result = connector.get_fii_administrators(str(args.get("termo", "")))
                elif tool_name == "bdg_get_cvm_extra_features":
                    result = connector.get_cvm_extra_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_cvm_extra_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_volume=bool(args.get("sort_by_volume", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"CVM Mercado Extra parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No CVM capital markets data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="cvm_mercado_extra")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_susep_premios_ramo",
            "bdg_get_susep_previdencia_vgbl",
            "bdg_get_susep_features",
            "bdg_list_top_susep_municipalities",
        }:
            from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
                SusepSegurosConnector,
            )

            connector = SusepSegurosConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_susep_premios_ramo":
                    result: Any = connector.get_susep_premios_ramo(str(args.get("termo", "")))
                elif tool_name == "bdg_get_susep_previdencia_vgbl":
                    result = connector.get_susep_previdencia_vgbl(str(args.get("termo", "")))
                elif tool_name == "bdg_get_susep_features":
                    result = connector.get_susep_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_susep_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_premios=bool(args.get("sort_by_premios", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"SUSEP Seguros parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No SUSEP insurance data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="susep_seguros")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_estban_municipal_balance":
            from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
                BcbEstbanConnector,
            )

            est_conn = BcbEstbanConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            bal = est_conn.get_municipal_balance(termo)
            if not bal:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"BACEN ESTBAN balance not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(bal, source_id="bcb_estban")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_estban_credit_portfolio":
            from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
                BcbEstbanConnector,
            )

            est_conn = BcbEstbanConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            port = est_conn.get_credit_portfolio(termo)
            if not port:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"BACEN ESTBAN credit portfolio not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(port, source_id="bcb_estban")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_estban_liquidity_features":
            from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
                BcbEstbanConnector,
            )

            est_conn = BcbEstbanConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = est_conn.get_estban_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"BACEN ESTBAN features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="bcb_estban")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_estban_municipalities":
            from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
                BcbEstbanConnector,
            )

            est_conn = BcbEstbanConnector(settings=self.settings)
            rows = est_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="bcb_estban")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_bcb_agencias_branches":
            from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
                BcbAgenciaConnector,
            )

            ag_conn = BcbAgenciaConnector(settings=self.settings)
            rows = ag_conn.get_branches(str(args.get("id_municipio_or_cnpj", "")))
            wrapped = wrap_untrusted_gov_data(rows, source_id="bcb_agencia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_bcb_agencias_coverage":
            from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
                BcbAgenciaConnector,
            )

            ag_conn = BcbAgenciaConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            cov = ag_conn.get_municipal_coverage(termo)
            if not cov:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"BACEN Agencias coverage not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(cov, source_id="bcb_agencia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_bcb_agencias_capilaridade_features":
            from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
                BcbAgenciaConnector,
            )

            ag_conn = BcbAgenciaConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = ag_conn.get_agencia_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"BACEN Agencias features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="bcb_agencia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_bcb_agencias_municipalities":
            from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
                BcbAgenciaConnector,
            )

            ag_conn = BcbAgenciaConnector(settings=self.settings)
            rows = ag_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="bcb_agencia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_rfb_cno_works":
            from public_data_gateway.connectors.empresarial.rfb_cno import (
                RfbCnoConnector,
            )

            cno_conn = RfbCnoConnector(settings=self.settings)
            rows = cno_conn.get_works(str(args.get("termo", "")))
            wrapped = wrap_untrusted_gov_data(rows, source_id="rfb_cno")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_rfb_cno_municipal_summary":
            from public_data_gateway.connectors.empresarial.rfb_cno import (
                RfbCnoConnector,
            )

            cno_conn = RfbCnoConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            summary = cno_conn.get_municipal_summary(termo)
            if not summary:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"RFB CNO municipal summary not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(summary, source_id="rfb_cno")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_get_rfb_cno_construction_features":
            from public_data_gateway.connectors.empresarial.rfb_cno import (
                RfbCnoConnector,
            )

            cno_conn = RfbCnoConnector(settings=self.settings)
            termo = str(args.get("id_municipio_or_name", ""))
            feat = cno_conn.get_cno_features(termo)
            if not feat:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"RFB CNO features not found for '{termo}'"}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(feat, source_id="rfb_cno")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name == "bdg_list_top_rfb_cno_municipalities":
            from public_data_gateway.connectors.empresarial.rfb_cno import (
                RfbCnoConnector,
            )

            cno_conn = RfbCnoConnector(settings=self.settings)
            rows = cno_conn.list_top_municipalities(
                limit=int(args.get("limit", 10)),
                sigla_uf=args.get("sigla_uf"),
            )
            wrapped = wrap_untrusted_gov_data(rows, source_id="rfb_cno")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_search_catmat_materials", "pdg_search_catmat_materials"}:
            from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
                ComprasGovCatmatConnector,
            )

            cat_conn = ComprasGovCatmatConnector(settings=self.settings)
            query = args.get("query", "")
            limit = args.get("limit", 10)
            items = cat_conn.search_materials(query=query, limit=limit)
            wrapped = wrap_untrusted_gov_data(items, source_id="comprasgov_catmat")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_procurement_price_benchmark", "pdg_get_procurement_price_benchmark"}:
            from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
                ComprasGovCatmatConnector,
            )

            cat_conn = ComprasGovCatmatConnector(settings=self.settings)
            termo = args.get("termo", "")
            bench = cat_conn.get_price_benchmark(termo)
            if not bench:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Price benchmark not found for '{termo}'",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(bench, source_id="comprasgov_catmat")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_querido_diario_licitacoes",
            "bdg_get_querido_diario_excertos",
            "bdg_get_querido_diario_features",
            "bdg_list_top_querido_diario_municipalities",
        }:
            from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
                QueridoDiarioOkbrConnector,
            )

            connector = QueridoDiarioOkbrConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_querido_diario_licitacoes":
                    result: Any = connector.get_gazette_tenders(str(args.get("termo", "")))
                elif tool_name == "bdg_get_querido_diario_excertos":
                    result = connector.get_thematic_excerpts(str(args.get("termo", "")))
                elif tool_name == "bdg_get_querido_diario_features":
                    result = connector.get_gazette_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_gazette_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_volume=bool(args.get("sort_by_volume", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"Querido Diário OKBR parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No Querido Diário OKBR gazette data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="querido_diario_okbr")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_municipality_gdp", "pdg_get_municipality_gdp"}:
            from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
                IBGESidraConnector,
            )

            sidra_conn = IBGESidraConnector(settings=self.settings)
            id_mun = args.get("id_municipio", "")
            gdp_res = sidra_conn.get_municipality_gdp(id_mun)
            if not gdp_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Municipal GDP not found for ID '{id_mun}'",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(gdp_res, source_id="ibge_sidra")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_municipality_socioeconomic_profile",
            "pdg_get_municipality_socioeconomic_profile",
        }:
            from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
                IBGESidraConnector,
            )

            sidra_conn = IBGESidraConnector(settings=self.settings)
            id_mun = args.get("id_municipio", "")
            prof_res = sidra_conn.get_municipality_socioeconomic_profile(id_mun)
            if not prof_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Socioeconomic profile not found for ID '{id_mun}'",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(prof_res, source_id="ibge_sidra")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_ipca_history", "pdg_get_ipca_history"}:
            from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
                IBGESidraConnector,
            )

            sidra_conn = IBGESidraConnector(settings=self.settings)
            lim = args.get("limit", 12)
            ipca_res = sidra_conn.get_ipca_history(limit=lim)
            wrapped = wrap_untrusted_gov_data(ipca_res, source_id="ibge_sidra")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_census_summary", "pdg_get_census_summary"}:
            from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
                IBGECenso2022Connector,
            )

            censo_conn = IBGECenso2022Connector(settings=self.settings)
            id_mun = str(args.get("id_municipio", "")).strip()
            summary_res = censo_conn.get_census_summary(id_mun)
            if not summary_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"Municipality census summary not found for ID '{id_mun}'."}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(summary_res, source_id="ibge_censo2022")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_demographic_profile", "pdg_get_demographic_profile"}:
            from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
                IBGECenso2022Connector,
            )

            censo_conn = IBGECenso2022Connector(settings=self.settings)
            id_mun = str(args.get("id_municipio", "")).strip()
            profile_res = censo_conn.get_demographic_profile(id_mun)
            if not profile_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"Municipality demographic profile not found for ID '{id_mun}'."}],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(profile_res, source_id="ibge_censo2022")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_highest_aging_municipalities", "pdg_list_highest_aging_municipalities"}:
            from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
                IBGECenso2022Connector,
            )

            censo_conn = IBGECenso2022Connector(settings=self.settings)
            lim = int(args.get("limit", 10))
            aging_res = censo_conn.list_highest_aging_municipalities(limit=lim)
            wrapped = wrap_untrusted_gov_data(aging_res, source_id="ibge_censo2022")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_employment_summary", "pdg_get_employment_summary"}:
            from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
                MTENovoCagedConnector,
            )

            caged_conn = MTENovoCagedConnector(settings=self.settings)
            id_mun = str(args.get("id_municipio", "")).strip()
            comp = args.get("competencia")
            summary_res = caged_conn.get_employment_summary(id_mun, competencia=comp)
            if not summary_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Error: No Novo CAGED summary found for municipality '{id_mun}'.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(summary_res, source_id="mte_novo_caged")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_labor_market_profile", "pdg_get_labor_market_profile"}:
            from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
                MTENovoCagedConnector,
            )

            caged_conn = MTENovoCagedConnector(settings=self.settings)
            id_mun = str(args.get("id_municipio", "")).strip()
            profile_res = caged_conn.get_labor_market_profile(id_mun)
            if not profile_res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Error: No Novo CAGED labor market profile found for municipality '{id_mun}'.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(profile_res, source_id="mte_novo_caged")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_top_job_creating_municipalities", "pdg_list_top_job_creating_municipalities"}:
            from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
                MTENovoCagedConnector,
            )

            caged_conn = MTENovoCagedConnector(settings=self.settings)
            lim = int(args.get("limit", 10))
            ranking_res = caged_conn.list_top_job_creating_municipalities(limit=lim)
            wrapped = wrap_untrusted_gov_data(ranking_res, source_id="mte_novo_caged")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_rais_employment_summary",
            "bdg_get_rais_market_profile",
            "bdg_list_top_formalized_municipalities",
        }:
            from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

            connector = MTERaisConnector(settings=self.settings)
            if tool_name == "bdg_get_rais_employment_summary":
                raw_year = args.get("ano")
                result = connector.get_formal_employment_summary(
                    str(args.get("id_municipio", "")).strip(),
                    ano=int(raw_year) if raw_year is not None else None,
                )
            elif tool_name == "bdg_get_rais_market_profile":
                result = connector.get_rais_market_profile(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                result = connector.list_top_formalized_municipalities(
                    limit=int(args.get("limit", 10))
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No RAIS data found for the requested query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mte_rais")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_mps_inss_beneficios",
            "bdg_get_mps_inss_concessoes",
            "bdg_get_mps_inss_features",
            "bdg_list_top_mps_inss_municipalities",
        }:
            from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
                MpsInssBeneficiosConnector,
            )

            connector = MpsInssBeneficiosConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_mps_inss_beneficios":
                    result: Any = connector.get_inss_issued_benefits(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_mps_inss_concessoes":
                    result = connector.get_inss_benefit_grants(str(args.get("termo", "")))
                elif tool_name == "bdg_get_mps_inss_features":
                    result = connector.get_inss_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_inss_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_volume=bool(args.get("sort_by_volume", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"MPS/INSS parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No MPS/INSS social security data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mps_inss_beneficios")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_municipio_boundary",
            "bdg_get_uf_boundary",
            "bdg_get_territorial_features",
            "bdg_list_top_compact_municipalities",
        }:
            from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
                IBGEMalhasConnector,
            )

            connector = IBGEMalhasConnector(settings=self.settings)
            if tool_name == "bdg_get_municipio_boundary":
                result = connector.get_municipio_boundary(
                    str(args.get("id_municipio", "")).strip(),
                    qualidade=str(args.get("qualidade", "minima")),
                )
            elif tool_name == "bdg_get_uf_boundary":
                result = connector.get_uf_boundary(
                    str(args.get("sigla_uf", "")).strip(),
                    qualidade=str(args.get("qualidade", "minima")),
                )
            elif tool_name == "bdg_get_territorial_features":
                result = connector.get_territorial_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_compact_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No territorial mesh data found for the query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="ibge_malhas")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_anatel_stations",
            "bdg_get_anatel_broadband",
            "bdg_get_anatel_telecom_features",
            "bdg_list_top_connected_municipalities",
        }:
            from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
                AnatelMosaicoConnector,
            )

            connector = AnatelMosaicoConnector(settings=self.settings)
            if tool_name == "bdg_get_anatel_stations":
                result = connector.get_station_coverage_summary(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_anatel_broadband":
                result = connector.get_broadband_scm_profile(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_anatel_telecom_features":
                result = connector.get_telecom_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_connected_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No ANATEL Mosaico telecom data found for the query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="anatel_mosaico")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_antt_highway_corridors",
            "bdg_get_antt_rntrc_fleet",
            "bdg_get_antt_logistics_features",
            "bdg_list_top_logistics_hubs",
        }:
            from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
                AnttMalhaTransporteConnector,
            )

            connector = AnttMalhaTransporteConnector(settings=self.settings)
            if tool_name == "bdg_get_antt_highway_corridors":
                result = connector.get_highway_corridors(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_antt_rntrc_fleet":
                result = connector.get_rntrc_fleet_profile(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_antt_logistics_features":
                result = connector.get_logistics_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_logistics_hubs(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No ANTT logistics data found for the query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="antt_malha_transporte")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_senatran_frota_tipos",
            "bdg_get_senatran_frota_propulsao_ev",
            "bdg_get_senatran_frota_features",
            "bdg_list_top_senatran_frota_municipalities",
        }:
            from public_data_gateway.connectors.geografico.senatran_frota import (
                SenatranFrotaConnector,
            )

            connector = SenatranFrotaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_senatran_frota_tipos":
                    result: Any = connector.get_fleet_by_type(str(args.get("termo", "")))
                elif tool_name == "bdg_get_senatran_frota_propulsao_ev":
                    result = connector.get_fleet_propulsion_ev(str(args.get("termo", "")))
                elif tool_name == "bdg_get_senatran_frota_features":
                    result = connector.get_fleet_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_fleet_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_ev=bool(args.get("sort_by_ev", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"SENATRAN Frota parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No SENATRAN vehicle fleet data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="senatran_frota")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_prf_sinistros_rodovias",
            "bdg_get_prf_corredores_logisticos",
            "bdg_get_prf_sinistralidade_features",
            "bdg_list_top_prf_seguranca_viaria",
        }:
            from public_data_gateway.connectors.geografico.prf_acidentes import (
                PrfAccidentsConnector,
            )

            connector = PrfAccidentsConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_prf_sinistros_rodovias":
                    result: Any = connector.get_prf_accidents(str(args.get("termo", "")))
                elif tool_name == "bdg_get_prf_corredores_logisticos":
                    result = connector.get_prf_corridors(str(args.get("termo", "")))
                elif tool_name == "bdg_get_prf_sinistralidade_features":
                    result = connector.get_prf_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_safe_prf_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_fatalities=bool(args.get("sort_by_fatalities", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"PRF Acidentes parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No PRF highway accidents data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="prf_acidentes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_cnes_establishments",
            "bdg_get_cnes_beds_icu",
            "bdg_get_cnes_health_features",
            "bdg_list_top_health_hubs",
        }:
            from public_data_gateway.connectors.saude.datasus_cnes.connector import (
                DataSUSCnesConnector,
            )

            connector = DataSUSCnesConnector(settings=self.settings)
            if tool_name == "bdg_get_cnes_establishments":
                result: Any = connector.get_establishments_summary(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_cnes_beds_icu":
                result = connector.get_hospital_beds_icu(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_cnes_health_features":
                result = connector.get_health_capacity_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_health_hubs(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No DataSUS CNES data found for the query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="datasus_cnes")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sih_admissions",
            "bdg_get_sih_hospital_costs",
            "bdg_get_sih_morbidity_features",
            "bdg_list_top_sih_complexity_hubs",
        }:
            from public_data_gateway.connectors.saude.datasus_sih.connector import (
                DataSUSSihConnector,
            )

            connector = DataSUSSihConnector(settings=self.settings)
            if tool_name == "bdg_get_sih_admissions":
                result = connector.get_admissions_summary(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sih_hospital_costs":
                result = connector.get_hospital_costs(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sih_morbidity_features":
                result = connector.get_sih_morbidity_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_sih_complexity_hubs(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "No DataSUS SIH data found for the query."}
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="datasus_sih")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sim_general_mortality",
            "bdg_get_sim_maternal_infant_mortality",
            "bdg_get_sim_mortality_features",
            "bdg_list_top_sim_surveillance_municipalities",
        }:
            from public_data_gateway.connectors.saude.datasus_sim.connector import (
                DataSUSSimConnector,
            )

            connector = DataSUSSimConnector(settings=self.settings)
            if tool_name == "bdg_get_sim_general_mortality":
                result = connector.get_general_mortality(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sim_maternal_infant_mortality":
                result = connector.get_maternal_infant_mortality(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sim_mortality_features":
                result = connector.get_sim_mortality_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_sim_surveillance_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No DataSUS SIM data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="datasus_sim")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sinasc_live_births",
            "bdg_get_sinasc_newborn_vitality",
            "bdg_get_sinasc_perinatal_features",
            "bdg_list_top_sinasc_prenatal_municipalities",
        }:
            from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
                DataSUSSinascConnector,
            )

            connector = DataSUSSinascConnector(settings=self.settings)
            if tool_name == "bdg_get_sinasc_live_births":
                result = connector.get_live_births_summary(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sinasc_newborn_vitality":
                result = connector.get_newborn_vitality_conditions(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_sinasc_perinatal_features":
                result = connector.get_sinasc_perinatal_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_sinasc_prenatal_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No DataSUS SINASC data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="datasus_sinasc")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_cmed_presentations_summary",
            "bdg_get_cmed_public_procurement_pmvg",
            "bdg_get_cmed_drug_pricing_features",
            "bdg_list_top_cmed_regulatory_efficiency_hubs",
        }:
            from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
                ANVISACmedConnector,
            )

            connector = ANVISACmedConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_cmed_presentations_summary":
                    result = connector.get_presentations_summary(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_cmed_public_procurement_pmvg":
                    result = connector.get_public_procurement_pmvg(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_cmed_drug_pricing_features":
                    result = connector.get_cmed_drug_pricing_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_cmed_regulatory_efficiency_hubs(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ANVISA CMED parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANVISA CMED data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="anvisa_cmed")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_vigilancia_srag_respiratory_surveillance",
            "bdg_get_vigilancia_sipni_vaccination_coverage",
            "bdg_get_vigilancia_epidemiological_features",
            "bdg_list_top_vigilancia_surveillance_municipalities",
        }:
            from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
                OpenDataSUSVigilanciaConnector,
            )

            connector = OpenDataSUSVigilanciaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_vigilancia_srag_respiratory_surveillance":
                    result = connector.get_vigilancia_srag_respiratory_surveillance(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_vigilancia_sipni_vaccination_coverage":
                    result = connector.get_vigilancia_sipni_vaccination_coverage(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_vigilancia_epidemiological_features":
                    result = connector.get_vigilancia_epidemiological_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_vigilancia_surveillance_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"OpenDataSUS Vigilância parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No OpenDataSUS Vigilância data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="opendatasus_vigilancia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_ans_beneficiarios",
            "bdg_get_ans_idss_qualidade",
            "bdg_get_ans_features",
            "bdg_list_top_ans_municipalities",
        }:
            from public_data_gateway.connectors.saude.ans_saude_suplementar import (
                AnsSaudeSuplementarConnector,
            )

            connector = AnsSaudeSuplementarConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_ans_beneficiarios":
                    result: Any = connector.get_beneficiaries(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ans_idss_qualidade":
                    result = connector.get_operator_quality_idss(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ans_features":
                    result = connector.get_ans_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_ans_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_beneficiaries=bool(args.get("sort_by_beneficiaries", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ANS Saúde Suplementar parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANS supplementary health data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="ans_saude_suplementar")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_censo_escolar_schools",
            "bdg_get_censo_escolar_teachers",
            "bdg_get_censo_escolar_features",
            "bdg_list_top_censo_escolar_municipalities",
        }:
            from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
                INEPCensoEscolarConnector,
            )

            connector = INEPCensoEscolarConnector(settings=self.settings)
            if tool_name == "bdg_get_censo_escolar_schools":
                result = connector.get_school_infrastructure(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_censo_escolar_teachers":
                result = connector.get_teacher_qualifications(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_censo_escolar_features":
                result = connector.get_censo_escolar_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_censo_escolar_municipalities(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INEP Censo Escolar data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inep_censo_escolar")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_censo_superior_institutions",
            "bdg_get_censo_superior_faculty",
            "bdg_get_censo_superior_features",
            "bdg_list_top_censo_superior_hubs",
        }:
            from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
                INEPCensoSuperiorConnector,
            )

            connector = INEPCensoSuperiorConnector(settings=self.settings)
            if tool_name == "bdg_get_censo_superior_institutions":
                result = connector.get_institutions_summary(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_censo_superior_faculty":
                result = connector.get_faculty_profile(
                    str(args.get("id_municipio", "")).strip()
                )
            elif tool_name == "bdg_get_censo_superior_features":
                result = connector.get_censo_superior_features(
                    str(args.get("id_municipio", "")).strip()
                )
            else:
                raw_uf = args.get("sigla_uf")
                result = connector.list_top_censo_superior_hubs(
                    limit=int(args.get("limit", 10)),
                    sigla_uf=str(raw_uf).strip() if raw_uf else None,
                )
            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INEP Censo da Educação Superior data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inep_censo_superior")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_ideb_municipal_results",
            "bdg_get_ideb_saeb_proficiency",
            "bdg_get_ideb_education_quality_features",
            "bdg_list_top_ideb_municipalities",
        }:
            from public_data_gateway.connectors.educacao.inep_ideb.connector import (
                INEPIdebConnector,
            )

            connector = INEPIdebConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_ideb_municipal_results":
                    result = connector.get_ideb_municipal_results(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_ideb_saeb_proficiency":
                    result = connector.get_ideb_saeb_proficiency(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_ideb_education_quality_features":
                    result = connector.get_ideb_education_quality_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_ideb_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"INEP IDEB parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INEP IDEB data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inep_ideb")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_enem_municipal_area_scores",
            "bdg_get_enem_essay_competencies",
            "bdg_get_enem_performance_equity_features",
            "bdg_list_top_enem_municipalities",
        }:
            from public_data_gateway.connectors.educacao.inep_enem.connector import (
                INEPEnemConnector,
            )

            connector = INEPEnemConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_enem_municipal_area_scores":
                    result = connector.get_enem_municipal_area_scores(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_enem_essay_competencies":
                    result = connector.get_enem_essay_competencies(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_enem_performance_equity_features":
                    result = connector.get_enem_performance_equity_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_enem_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"INEP ENEM parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INEP ENEM data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inep_enem")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sucupira_program_evaluation",
            "bdg_get_sucupira_scientific_production",
            "bdg_get_sucupira_postgrad_features",
            "bdg_list_top_sucupira_municipalities",
        }:
            from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
                CapesSucupiraConnector,
            )

            connector = CapesSucupiraConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_sucupira_program_evaluation":
                    result = connector.get_sucupira_program_evaluation(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_sucupira_scientific_production":
                    result = connector.get_sucupira_scientific_production(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_sucupira_postgrad_features":
                    result = connector.get_sucupira_postgrad_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_sucupira_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"CAPES Sucupira parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No CAPES Sucupira data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="capes_sucupira")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_terrabrasilis_prodes_deforestation",
            "bdg_get_terrabrasilis_esg_governance",
            "bdg_get_terrabrasilis_environmental_features",
            "bdg_list_top_terrabrasilis_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
                InpeTerrabrasilisConnector,
            )

            connector = InpeTerrabrasilisConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_terrabrasilis_prodes_deforestation":
                    result = connector.get_terrabrasilis_prodes_deforestation(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_terrabrasilis_esg_governance":
                    result = connector.get_terrabrasilis_esg_governance(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_terrabrasilis_environmental_features":
                    result = connector.get_terrabrasilis_environmental_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_terrabrasilis_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"INPE TerraBrasilis parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INPE TerraBrasilis data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inpe_terrabrasilis")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_hidroweb_stations",
            "bdg_get_hidroweb_water_security",
            "bdg_get_hidroweb_hydrology_features",
            "bdg_list_top_hidroweb_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
                AnaHidrowebConnector,
            )

            connector = AnaHidrowebConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_hidroweb_stations":
                    result: Any = connector.get_hidroweb_stations(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_hidroweb_water_security":
                    result = connector.get_hidroweb_water_security(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_hidroweb_hydrology_features":
                    result = connector.get_hidroweb_hydrology_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_hidroweb_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ANA HidroWeb parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANA HidroWeb data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="ana_hidroweb_telemetria")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_mapbiomas_land_cover",
            "bdg_get_mapbiomas_alerts",
            "bdg_get_mapbiomas_features",
            "bdg_list_top_mapbiomas_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
                MapBiomasCoberturaConnector,
            )

            connector = MapBiomasCoberturaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_mapbiomas_land_cover":
                    result: Any = connector.get_mapbiomas_land_cover(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_mapbiomas_alerts":
                    result = connector.get_mapbiomas_alerts(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_mapbiomas_features":
                    result = connector.get_mapbiomas_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_mapbiomas_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"MapBiomas parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No MapBiomas data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mapbiomas_cobertura")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sicar_properties",
            "bdg_get_sicar_pra_compliance",
            "bdg_get_sicar_features",
            "bdg_list_top_sicar_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
                SfbSicarConnector,
            )

            connector = SfbSicarConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_sicar_properties":
                    result: Any = connector.get_sicar_properties(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_sicar_pra_compliance":
                    result = connector.get_sicar_pra_compliance(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_sicar_features":
                    result = connector.get_sicar_features(
                        str(args.get("id_municipio", ""))
                    )
                else:
                    result = connector.list_top_sicar_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"SFB/SICAR parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No SFB/SICAR data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="sfb_sicar")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_zarc_climate_aptitude",
            "bdg_get_zarc_proagro_coverage",
            "bdg_get_zarc_features",
            "bdg_list_top_zarc_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
                MapaZarcConnector,
            )

            connector = MapaZarcConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_zarc_climate_aptitude":
                    result: Any = connector.get_zarc_climate_aptitude(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_zarc_proagro_coverage":
                    result = connector.get_zarc_proagro_coverage(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_zarc_features":
                    result = connector.get_zarc_features(
                        str(args.get("id_municipio", ""))
                    )
                else:
                    result = connector.list_top_zarc_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"MAPA ZARC parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No MAPA ZARC data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mapa_zarc")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_ibama_infractions",
            "bdg_get_ibama_embargoes",
            "bdg_get_ibama_enforcement_features",
            "bdg_list_top_ibama_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
                IbamaFiscalizacaoConnector,
            )

            connector = IbamaFiscalizacaoConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_ibama_infractions":
                    result: Any = connector.get_infractions(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ibama_embargoes":
                    result = connector.get_embargoes(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ibama_enforcement_features":
                    result = connector.get_ibama_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_risk=bool(args.get("sort_by_risk", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"IBAMA SIFISC parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No IBAMA SIFISC data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="ibama_fiscalizacao")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_bcb_sicor_custeio",
            "bdg_get_bcb_sicor_pronaf_proagro",
            "bdg_get_bcb_sicor_credit_features",
            "bdg_list_top_bcb_sicor_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.bcb_sicor import (
                BcbSicorConnector,
            )

            connector = BcbSicorConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_bcb_sicor_custeio":
                    result: Any = connector.get_custeio(str(args.get("termo", "")))
                elif tool_name == "bdg_get_bcb_sicor_pronaf_proagro":
                    result = connector.get_pronaf_proagro(
                        str(args.get("id_municipio_or_name", ""))
                    )
                elif tool_name == "bdg_get_bcb_sicor_credit_features":
                    result = connector.get_sicor_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_pronaf=bool(args.get("sort_by_pronaf", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"BACEN SICOR parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No BACEN SICOR data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="bcb_sicor")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_ibge_agro_pam_crops",
            "bdg_get_ibge_agro_ppm_livestock",
            "bdg_get_ibge_agro_production_features",
            "bdg_list_top_ibge_agro_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
                IbgeAgroMunicipalConnector,
            )

            connector = IbgeAgroMunicipalConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_ibge_agro_pam_crops":
                    result: Any = connector.get_pam_crops(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ibge_agro_ppm_livestock":
                    result = connector.get_ppm_livestock(str(args.get("termo", "")))
                elif tool_name == "bdg_get_ibge_agro_production_features":
                    result = connector.get_agro_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_vbp=bool(args.get("sort_by_vbp", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"IBGE Agro parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No IBGE Agro Municipal data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="ibge_agro_municipal")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_rfb_cafir_properties",
            "bdg_get_rfb_cafir_cnir_linkage",
            "bdg_get_rfb_cafir_land_features",
            "bdg_list_top_rfb_cafir_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.rfb_cafir import (
                RfbCafirConnector,
            )

            connector = RfbCafirConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_rfb_cafir_properties":
                    result: Any = connector.get_rural_properties(str(args.get("termo", "")))
                elif tool_name == "bdg_get_rfb_cafir_cnir_linkage":
                    result = connector.get_cnir_linkage(
                        str(args.get("id_municipio_or_name", ""))
                    )
                elif tool_name == "bdg_get_rfb_cafir_land_features":
                    result = connector.get_cafir_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_area=bool(args.get("sort_by_area", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"RFB CAFIR parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No RFB CAFIR data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="rfb_cafir")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_inpe_queimadas_focos",
            "bdg_get_seeg_emissoes_gee",
            "bdg_get_queimadas_seeg_features",
            "bdg_list_top_queimadas_seeg_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
                InpeQueimadasSeegConnector,
            )

            connector = InpeQueimadasSeegConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_inpe_queimadas_focos":
                    result: Any = connector.get_fire_hotspots(str(args.get("termo", "")))
                elif tool_name == "bdg_get_seeg_emissoes_gee":
                    result = connector.get_seeg_emissions(str(args.get("termo", "")))
                elif tool_name == "bdg_get_queimadas_seeg_features":
                    result = connector.get_queimadas_seeg_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_emissions=bool(args.get("sort_by_emissions", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"INPE Queimadas/SEEG parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No INPE Queimadas/SEEG data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="inpe_queimadas_seeg")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_aneel_mmgd_empreendimentos",
            "bdg_get_aneel_mmgd_fontes",
            "bdg_get_aneel_mmgd_features",
            "bdg_list_top_aneel_mmgd_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
                AneelGeracaoDistribuidaConnector,
            )

            connector = AneelGeracaoDistribuidaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_aneel_mmgd_empreendimentos":
                    result: Any = connector.get_mmgd_plants(str(args.get("termo", "")))
                elif tool_name == "bdg_get_aneel_mmgd_fontes":
                    result = connector.get_mmgd_sources_by_municipality(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_aneel_mmgd_features":
                    result = connector.get_mmgd_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_mmgd_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_power=bool(args.get("sort_by_power", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ANEEL MMGD parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANEEL MMGD data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="aneel_geracao_distribuida")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_anp_combustiveis_precos",
            "bdg_get_anp_combustiveis_vendas",
            "bdg_get_anp_combustiveis_features",
            "bdg_list_top_anp_combustiveis_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.anp_combustiveis import (
                AnpCombustiveisConnector,
            )

            connector = AnpCombustiveisConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_anp_combustiveis_precos":
                    result: Any = connector.get_anp_fuel_prices(str(args.get("termo", "")))
                elif tool_name == "bdg_get_anp_combustiveis_vendas":
                    result = connector.get_anp_fuel_sales(str(args.get("termo", "")))
                elif tool_name == "bdg_get_anp_combustiveis_features":
                    result = connector.get_anp_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_anp_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_biofuel=bool(args.get("sort_by_biofuel", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ANP Combustiveis parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANP Combustiveis data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="anp_combustiveis")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_mdr_snis_agua_esgoto",
            "bdg_get_mdr_snis_residuos",
            "bdg_get_mdr_snis_features",
            "bdg_list_top_mdr_snis_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

            connector = MdrSnisConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_mdr_snis_agua_esgoto":
                    result: Any = connector.get_snis_water_sewage(str(args.get("termo", "")))
                elif tool_name == "bdg_get_mdr_snis_residuos":
                    result = connector.get_snis_solid_waste(str(args.get("termo", "")))
                elif tool_name == "bdg_get_mdr_snis_features":
                    result = connector.get_snis_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_snis_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_sewage=bool(args.get("sort_by_sewage", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"MDR SNIS parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No MDR SNIS data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mdr_snis")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_sedec_desastres_ocorrencias",
            "bdg_get_sedec_decretos_reconhecimento",
            "bdg_get_sedec_desastres_features",
            "bdg_list_top_sedec_resilient_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.sedec_desastres import (
                SedecDesastresConnector,
            )

            connector = SedecDesastresConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_sedec_desastres_ocorrencias":
                    result: Any = connector.get_sedec_disasters(str(args.get("termo", "")))
                elif tool_name == "bdg_get_sedec_decretos_reconhecimento":
                    result = connector.get_sedec_decrees(str(args.get("termo", "")))
                elif tool_name == "bdg_get_sedec_desastres_features":
                    result = connector.get_sedec_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_resilient_sedec_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_damages=bool(args.get("sort_by_damages", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"SEDEC Desastres parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No SEDEC Desastres data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="sedec_desastres")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_mme_ons_geracao_sin",
            "bdg_get_mme_ons_balanco_carga",
            "bdg_get_mme_ons_features",
            "bdg_list_top_mme_ons_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.mme_ons_energia import (
                MmeOnsEnergiaConnector,
            )

            connector = MmeOnsEnergiaConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_mme_ons_geracao_sin":
                    result: Any = connector.get_ons_generation_capacity(
                        str(args.get("termo", ""))
                    )
                elif tool_name == "bdg_get_mme_ons_balanco_carga":
                    result = connector.get_ons_load_balance(str(args.get("termo", "")))
                elif tool_name == "bdg_get_mme_ons_features":
                    result = connector.get_ons_features(
                        str(args.get("id_municipio_or_name", ""))
                    )
                else:
                    result = connector.list_top_ons_energy_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                        sort_by_renewable=bool(args.get("sort_by_renewable", False)),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"MME/ONS Energia parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No MME/ONS energy data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="mme_ons_energia")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_anm_sigmine_processes",
            "bdg_get_anm_cfem_royalties",
            "bdg_get_anm_features",
            "bdg_list_top_anm_municipalities",
        }:
            from public_data_gateway.connectors.ambiental.anm_mineracao import (
                AnmMineracaoConnector,
            )

            anm_conn = AnmMineracaoConnector(settings=self.settings)
            if tool_name == "bdg_get_anm_sigmine_processes":
                result: Any = anm_conn.get_anm_sigmine_processes(args["termo"])
            elif tool_name == "bdg_get_anm_cfem_royalties":
                result = anm_conn.get_anm_cfem_royalties(args["termo"])
            elif tool_name == "bdg_get_anm_features":
                result = anm_conn.get_anm_features(args["id_municipio_or_name"])
            else:
                result = anm_conn.list_top_anm_municipalities(
                    limit=args.get("limit", 10),
                    sigla_uf=args.get("sigla_uf"),
                    sort_by_cfem=bool(args.get("sort_by_cfem", False)),
                )
            if result is None:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ANM mining data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="anm_mineracao")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_bd_coverage",
            "bdg_get_bd_governance",
            "bdg_get_bd_features",
            "bdg_list_top_bd_municipalities",
        }:
            from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
                DatalakeFederadoBqConnector,
            )

            connector = DatalakeFederadoBqConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_bd_coverage":
                    result: Any = connector.get_bd_coverage(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_bd_governance":
                    result = connector.get_bd_governance(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_bd_features":
                    result = connector.get_bd_features(
                        str(args.get("id_municipio", ""))
                    )
                else:
                    result = connector.list_top_bd_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Datalake Público Federado parameter error: {exc}",
                            }
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No Datalake Público Federado data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="datalake_federado_bq")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_ckan_organizations",
            "bdg_get_ckan_governance",
            "bdg_get_ckan_features",
            "bdg_list_top_ckan_municipalities",
        }:
            from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
                DadosGovCkanConnector,
            )

            connector = DadosGovCkanConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_ckan_organizations":
                    result = connector.get_ckan_organizations(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_ckan_governance":
                    result = connector.get_ckan_governance(
                        str(args.get("id_municipio", ""))
                    )
                elif tool_name == "bdg_get_ckan_features":
                    result = connector.get_ckan_features(
                        str(args.get("id_municipio", ""))
                    )
                else:
                    result = connector.list_top_ckan_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=args.get("sigla_uf"),
                    )
            except (ValueError, KeyError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Dados.gov.br CKAN parameter error: {exc}",
                            }
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No Dados.gov.br CKAN data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="dadosgov_ckan")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_onr_property_registry",
            "bdg_get_onr_real_estate_credit_guarantees",
            "bdg_get_onr_real_estate_features",
            "bdg_list_top_onr_real_estate_municipalities",
        }:
            from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
                ONRRegistroImoveisConnector,
            )

            connector = ONRRegistroImoveisConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_onr_property_registry":
                    result = connector.get_onr_property_registry(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_onr_real_estate_credit_guarantees":
                    result = connector.get_onr_real_estate_credit_guarantees(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_onr_real_estate_features":
                    result = connector.get_onr_real_estate_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_onr_real_estate_municipalities(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"ONR SREI parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No ONR Registro de Imóveis data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="onr_registro_imoveis")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {
            "bdg_get_iptu_itbi_property_cadastre",
            "bdg_get_iptu_itbi_price_dynamics",
            "bdg_get_iptu_itbi_real_estate_features",
            "bdg_list_top_iptu_itbi_capitals",
        }:
            from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
                CapitaisIptuItbiConnector,
            )

            connector = CapitaisIptuItbiConnector(settings=self.settings)
            try:
                if tool_name == "bdg_get_iptu_itbi_property_cadastre":
                    result = connector.get_iptu_itbi_property_cadastre(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_iptu_itbi_price_dynamics":
                    result = connector.get_iptu_itbi_price_dynamics(
                        str(args.get("id_municipio", "")).strip()
                    )
                elif tool_name == "bdg_get_iptu_itbi_real_estate_features":
                    result = connector.get_iptu_itbi_real_estate_features(
                        str(args.get("id_municipio", "")).strip()
                    )
                else:
                    raw_uf = args.get("sigla_uf")
                    result = connector.list_top_iptu_itbi_capitals(
                        limit=int(args.get("limit", 10)),
                        sigla_uf=str(raw_uf).strip() if raw_uf else None,
                    )
            except ValueError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"Capitais IPTU/ITBI parameter error: {exc}"}
                        ],
                        "isError": True,
                    },
                }

            if not result:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "No Capitais IPTU/ITBI data found for the query.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(result, source_id="capitais_iptu_itbi")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_labor_force_indicators", "pdg_get_labor_force_indicators"}:
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            pnad_conn = IBGEPnadContinuaConnector(settings=self.settings)
            uf = str(args.get("uf", "")).strip()
            trimestre = args.get("trimestre")
            res = pnad_conn.get_labor_force_indicators(uf, ano_trimestre=trimestre)
            if not res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Error: No PNAD labor force indicators found for state '{uf}'.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_pnad_continua")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_state_labor_profile", "pdg_get_state_labor_profile"}:
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            pnad_conn = IBGEPnadContinuaConnector(settings=self.settings)
            uf = str(args.get("uf", "")).strip()
            res = pnad_conn.get_state_labor_profile(uf)
            if not res:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Error: No PNAD state labor profile found for state '{uf}'.",
                            }
                        ],
                        "isError": True,
                    },
                }
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_pnad_continua")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_lowest_unemployment_states", "pdg_list_lowest_unemployment_states"}:
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            pnad_conn = IBGEPnadContinuaConnector(settings=self.settings)
            lim = int(args.get("limit", 10))
            ranking_res = pnad_conn.list_lowest_unemployment_states(limit=lim)
            wrapped = wrap_untrusted_gov_data(ranking_res, source_id="ibge_pnad_continua")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_highest_earnings_states", "pdg_list_highest_earnings_states"}:
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            pnad_conn = IBGEPnadContinuaConnector(settings=self.settings)
            lim = int(args.get("limit", 10))
            ranking_res = pnad_conn.list_highest_earnings_states(limit=lim)
            wrapped = wrap_untrusted_gov_data(ranking_res, source_id="ibge_pnad_continua")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_cnefe_municipality_summary", "pdg_get_cnefe_municipality_summary"}:
            from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
                IBGECnefeConnector,
            )

            cnefe_conn = IBGECnefeConnector(settings=self.settings)
            muni = str(args.get("municipio", "")).strip()
            res = cnefe_conn.get_municipality_summary(muni)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_cnefe")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_cnefe_cep_profile", "pdg_get_cnefe_cep_profile"}:
            from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
                IBGECnefeConnector,
            )

            cnefe_conn = IBGECnefeConnector(settings=self.settings)
            cep = str(args.get("cep", "")).strip()
            res = cnefe_conn.get_cep_profile(cep)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_cnefe")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_cnefe_building_types", "pdg_list_cnefe_building_types"}:
            from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
                IBGECnefeConnector,
            )

            cnefe_conn = IBGECnefeConnector(settings=self.settings)
            muni = str(args.get("municipio", "")).strip()
            res = cnefe_conn.list_building_types(muni)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_cnefe")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_cnefe_top_address_density", "pdg_list_cnefe_top_address_density"}:
            from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
                IBGECnefeConnector,
            )

            cnefe_conn = IBGECnefeConnector(settings=self.settings)
            lim = int(args.get("limit", 10))
            ranking_res = cnefe_conn.list_top_address_density(limit=lim)
            wrapped = wrap_untrusted_gov_data(ranking_res, source_id="ibge_cnefe")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_pib_municipal_detail", "pdg_get_pib_municipal_detail"}:
            id_mun = str(args.get("id_municipio", "")).strip()
            year = int(args.get("year", 2023))
            from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
                IBGEPibMunicipalConnector,
            )

            pib_conn = IBGEPibMunicipalConnector(settings=self.settings)
            res = pib_conn.get_municipal_gdp_detail(id_mun, year=year)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_pib_municipal")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_get_pib_municipal_features", "pdg_get_pib_municipal_features"}:
            id_mun = str(args.get("id_municipio", "")).strip()
            from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
                IBGEPibMunicipalConnector,
            )

            pib_conn = IBGEPibMunicipalConnector(settings=self.settings)
            res = pib_conn.get_municipal_gdp_features(id_mun)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_pib_municipal")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": wrapped}], "isError": False},
            }

        if tool_name in {"bdg_list_top_municipal_pib", "pdg_list_top_municipal_pib"}:
            limit = int(args.get("limit", 10))
            from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
                IBGEPibMunicipalConnector,
            )

            pib_conn = IBGEPibMunicipalConnector(settings=self.settings)
            res = pib_conn.list_top_municipalities(limit=limit)
            wrapped = wrap_untrusted_gov_data(res, source_id="ibge_pib_municipal")
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

        if tool_name == "bdg_get_cross_domain_features":
            from public_data_gateway.core.cross_domain import CrossDomainService

            id_mun = str(args.get("id_municipio", "")).strip()
            if not id_mun:
                raise ValueError("Missing required parameter: id_municipio")
            data = CrossDomainService(settings=self.settings).get_cross_domain_features(id_mun)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(
                                data, source_id="cross_domain_gold"
                            ),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_list_top_cross_domain_municipalities":
            from public_data_gateway.core.cross_domain import CrossDomainService

            limit = int(args.get("limit", 10))
            sigla_uf = args.get("sigla_uf")
            data = CrossDomainService(settings=self.settings).list_top_cross_domain_municipalities(
                limit=limit, sigla_uf=sigla_uf
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(
                                data, source_id="cross_domain_gold"
                            ),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_get_solution_pack_insights":
            from public_data_gateway.core.cross_domain import CrossDomainService

            pack_id = str(args.get("pack_id", "")).strip()
            if not pack_id:
                raise ValueError("Missing required parameter: pack_id")
            sigla_uf = args.get("sigla_uf")
            limit = int(args.get("limit", 10))
            data = CrossDomainService(settings=self.settings).get_solution_pack_insights(
                pack_id=pack_id, sigla_uf=sigla_uf, limit=limit
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(
                                data, source_id="cross_domain_gold"
                            ),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_get_lifecycle_stage_report":
            from public_data_gateway.core.lifecycle_reports import LifecycleReportsService

            stage_id = str(args.get("stage_id", "")).strip()
            pack_id = str(args.get("pack_id", "")).strip()
            if not stage_id or not pack_id:
                raise ValueError("Missing required parameters: stage_id and pack_id")
            data = LifecycleReportsService(settings=self.settings).get_stage_report(
                stage_id=stage_id,
                pack_id=pack_id,
                sigla_uf=args.get("sigla_uf"),
                id_municipio=args.get("id_municipio"),
                limit=int(args.get("limit", 10)),
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(
                                data, source_id="gtm_lifecycle_gold"
                            ),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_generate_solution_dossier":
            from public_data_gateway.core.lifecycle_reports import LifecycleReportsService

            pack_id = str(args.get("pack_id", "")).strip()
            sigla_uf = str(args.get("sigla_uf", "")).strip()
            if not pack_id or not sigla_uf:
                raise ValueError("Missing required parameters: pack_id and sigla_uf")
            svc = LifecycleReportsService(settings=self.settings)
            dossier = svc.build_sales_dossier(
                pack_id=pack_id,
                sigla_uf=sigla_uf,
                id_municipio=args.get("id_municipio"),
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(
                                dossier, source_id="gtm_lifecycle_gold"
                            ),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_list_public_apis_catalog":
            from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

            cat = PublicApisHubService(settings=self.settings).load_catalog(
                block=args.get("block"), pack_id=args.get("pack_id")
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(cat, source_id="public_apis_hub"),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_enrich_cnpj_cascade":
            from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

            cnpj = str(args.get("cnpj", "")).strip()
            if not cnpj:
                raise ValueError("Missing required parameter: cnpj")
            res = PublicApisHubService(settings=self.settings).enrich_cnpj_cascade(cnpj)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(res, source_id="public_apis_hub"),
                        }
                    ],
                    "isError": False,
                },
            }

        if tool_name == "bdg_count_osm_pois_radius":
            from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

            res = PublicApisHubService(settings=self.settings).count_osm_pois_radius(
                float(args["lat"]),
                float(args["lon"]),
                amenity=str(args.get("amenity", "school")),
                radius_m=int(args.get("radius_m", 1000)),
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(res, source_id="public_apis_hub"),
                        }
                    ],
                    "isError": False,
                },
            }

        wave10_5a_handlers = {
            "bdg_fetch_ipeadata_series": (
                "ipeadata_odata",
                lambda sdk: sdk.public_apis.fetch_ipeadata_series(
                    str(args.get("sercodigo", "")).strip(), limit=int(args.get("limit", 24))
                ),
            ),
            "bdg_fetch_brapi_quote": (
                "brapi_b3",
                lambda sdk: sdk.public_apis.fetch_brapi_quote(str(args.get("tickers", "PETR4")).strip()),
            ),
            "bdg_fetch_awesomeapi_rates": (
                "awesomeapi_fx",
                lambda sdk: sdk.public_apis.fetch_awesomeapi_rates(
                    str(args.get("pairs", "USD-BRL,EUR-BRL,BTC-BRL")).strip()
                ),
            ),
            "bdg_query_ckan_datastore": (
                "aneel_anp_ckan",
                lambda sdk: sdk.public_apis.query_ckan_datastore(
                    str(args.get("resource_id", "")).strip(),
                    portal=str(args.get("portal", "aneel")).strip(),
                    limit=int(args.get("limit", 20)),
                ),
            ),
            "bdg_fetch_camara_deputados": (
                "camara_senado_dados_abertos",
                lambda sdk: sdk.public_apis.fetch_camara_deputados(
                    pagina=int(args.get("pagina", 1)), itens=int(args.get("itens", 20))
                ),
            ),
            "bdg_fetch_world_bank_indicator": (
                "world_bank_v2",
                lambda sdk: sdk.public_apis.fetch_world_bank_indicator(
                    country=str(args.get("country", "BRA")).strip(),
                    indicator=str(args.get("indicator", "NY.GDP.MKTP.KD.ZG")).strip(),
                    per_page=int(args.get("per_page", 10)),
                ),
            ),
            "bdg_fetch_who_gho_indicator": (
                "who_gho_odata",
                lambda sdk: sdk.public_apis.fetch_who_gho_indicator(
                    indicator=str(args.get("indicator", "WHOSIS_000001")).strip(),
                    top=int(args.get("top", 10)),
                ),
            ),
            "bdg_compute_osrm_route": (
                "osrm_routing",
                lambda sdk: sdk.public_apis.compute_osrm_route(
                    float(args["lat1"]), float(args["lon1"]), float(args["lat2"]), float(args["lon2"])
                ),
            ),
            "bdg_fetch_open_meteo_forecast": (
                "open_meteo",
                lambda sdk: sdk.public_apis.fetch_open_meteo_forecast(
                    lat=float(args.get("lat", -23.5505)), lon=float(args.get("lon", -46.6333))
                ),
            ),
            "bdg_fetch_nasa_power_daily": (
                "nasa_power",
                lambda sdk: sdk.public_apis.fetch_nasa_power_daily(
                    lat=float(args.get("lat", -23.5505)),
                    lon=float(args.get("lon", -46.6333)),
                    start=str(args.get("start", "20261101")),
                    end=str(args.get("end", "20261105")),
                ),
            ),
            "bdg_fetch_elevation": (
                "open_elevation",
                lambda sdk: sdk.public_apis.fetch_elevation(
                    lat=float(args.get("lat", -23.5505)), lon=float(args.get("lon", -46.6333))
                ),
            ),
            "bdg_describe_upgini_features": (
                "upgini_adapter",
                lambda sdk: sdk.public_apis.describe_upgini_features(),
            ),
            "bdg_geocode_nominatim": (
                "nominatim_osm",
                lambda sdk: sdk.public_apis.geocode_nominatim(
                    str(args["query"]), limit=int(args.get("limit", 3))
                ),
            ),
            "bdg_lookup_rdap_registro_br": (
                "rdap_registro_br",
                lambda sdk: sdk.public_apis.lookup_rdap_registro_br(str(args["domain"])),
            ),
            "bdg_lookup_gleif_lei": (
                "gleif_lei",
                lambda sdk: sdk.public_apis.lookup_gleif_lei(
                    str(args["query"]), limit=int(args.get("limit", 5))
                ),
            ),
            "bdg_lookup_ip_intel": (
                "bgpview_ip_intel",
                lambda sdk: sdk.public_apis.lookup_ip_intel(str(args["ip"])),
            ),
            "bdg_lookup_b2b_domain_profile": (
                "company_enrich_apollo_bdc",
                lambda sdk: sdk.public_apis.lookup_b2b_domain_profile(str(args["domain"])),
            ),
            "bdg_lookup_country": (
                "rest_countries",
                lambda sdk: sdk.public_apis.lookup_country(str(args.get("code", "BRA"))),
            ),
            "bdg_search_openalex_works": (
                "openalex_api",
                lambda sdk: sdk.public_apis.search_openalex_works(
                    str(args["query"]), per_page=int(args.get("per_page", 5))
                ),
            ),
            "bdg_query_gdelt_doc": (
                "gdelt_doc_api",
                lambda sdk: sdk.public_apis.query_gdelt_doc(
                    str(args["query"]), maxrecords=int(args.get("maxrecords", 5))
                ),
            ),
            "bdg_lakehouse_sla_audit": (
                "lakehouse_sla",
                lambda sdk: sdk.lakehouse.audit_sla(
                    source_id=args.get("source_id"),
                    include_public_apis=bool(args.get("include_public_apis", True)),
                ),
            ),
            "bdg_lakehouse_cdc_plan": (
                "lakehouse_cdc",
                lambda sdk: sdk.lakehouse.cdc_plan(
                    layers=tuple(args.get("layers") or ("silver", "gold")),
                    sources=args.get("sources"),
                ),
            ),
            "bdg_lakehouse_mirror_sync": (
                "lakehouse_mirror",
                lambda sdk: sdk.lakehouse.mirror_sync(
                    provider=args.get("provider"),
                    dry_run=bool(args.get("dry_run", True)),
                    layers=tuple(args.get("layers") or ("silver", "gold")),
                    sources=args.get("sources"),
                ),
            ),
            "bdg_lakehouse_warehouse_ddl": (
                "lakehouse_warehouse",
                lambda sdk: sdk.lakehouse.warehouse_ddl(
                    provider=args.get("provider"),
                    layers=tuple(args.get("layers") or ("silver", "gold")),
                    sources=args.get("sources"),
                ),
            ),
        }
        if tool_name in wave10_5a_handlers:
            from public_data_gateway.sdk import BrazilData

            source_id, fn = wave10_5a_handlers[tool_name]
            sdk = BrazilData(settings=self.settings)
            res = fn(sdk)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": wrap_untrusted_gov_data(res, source_id=source_id),
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
