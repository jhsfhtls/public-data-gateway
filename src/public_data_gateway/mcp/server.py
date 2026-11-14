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
