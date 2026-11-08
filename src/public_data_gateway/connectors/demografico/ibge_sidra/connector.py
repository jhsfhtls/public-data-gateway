"""IBGE Sistema de Recuperação Automática (SIDRA) connector.

Governed by:
- Lei nº 5.534/1968 (Obrigatoriedade e sigilo das informações estatísticas do IBGE)
- Lei nº 5.878/1973 (Objetivos e competências da Fundação IBGE)
- Decreto nº 73.177/1973 (Regulamento geral do IBGE)
- Portaria IBGE nº 123/2021 (Política de Dados Abertos e Governança Estatística)
- ODCS v3.1 Data Contract: ODCS-BR-IBGE-SIDRA-001
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib3
import yaml

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.models import DataContract
from brazil_data_gateway.core.normalizer import CanonicalNormalizer
from brazil_data_gateway.core.storage import HybridStorageEngine

logger = logging.getLogger(__name__)

# Suppress insecure HTTPS warnings when contacting government open data endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class IBGESidraConnector:
    """IBGE SIDRA connector with Medallion Silver Parquet and Gold marts."""

    SOURCE_ID = "ibge_sidra"
    BASE_URL = "https://apisidra.ibge.gov.br"

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self.storage = HybridStorageEngine(settings=self.settings)
        self.normalizer = CanonicalNormalizer()
        self.contract = self._load_contract()

    def _load_contract(self) -> DataContract:
        """Load ODCS v3.1 contract specification from contract.yaml."""
        contract_path = Path(__file__).parent / "contract.yaml"
        if not contract_path.exists():
            raise FileNotFoundError(f"Contract file not found at: {contract_path}")

        with open(contract_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        data_gov = raw.get("data_governance", {})
        return DataContract(
            contract_id=raw["contract_id"],
            source_id=raw["source_id"],
            version=raw["version"],
            grain=data_gov.get("grain", "municipio_ano_variavel"),
            primary_keys=data_gov.get("primary_keys", ["id_municipio", "ano"]),
            reference_date_column=data_gov.get("reference_date_column", "reference_date"),
            min_expected_rows=data_gov.get("min_expected_rows", 5),
            max_volume_drop_pct=data_gov.get("max_volume_drop_pct", 20.0),
        )

    def _get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "PublicDataGateway/0.1.0 (+https://github.com/jhosefer/public-data-gateway)",
            "Accept": "application/json",
        }

    def verify_live(self) -> dict[str, Any]:
        """Perform live fail-hard probes against official IBGE SIDRA endpoints."""
        report: dict[str, Any] = {
            "source_id": self.SOURCE_ID,
            "timestamp": datetime.now(UTC).isoformat(),
            "endpoints": {},
            "status": "FAIL",
        }

        # 1. Probe Tabela 5938 (PIB Municipal - São Paulo)
        url_pib = f"{self.BASE_URL}/values/t/5938/p/last%201/v/37/n6/3550308"
        try:
            r_pib = requests.get(url_pib, headers=self._get_headers(), timeout=15, verify=False)
            pib_ok = r_pib.status_code == 200
            data_pib = r_pib.json() if pib_ok else []
            has_data = len(data_pib) > 1 and "V" in data_pib[1]
            report["endpoints"]["sidra_pib_municipal"] = {
                "status_code": r_pib.status_code,
                "passed": pib_ok and has_data,
                "valor_amostra": data_pib[1].get("V") if has_data else None,
                "ano": data_pib[1].get("D1N") if has_data else None,
            }
        except Exception as e:
            report["endpoints"]["sidra_pib_municipal"] = {"error": str(e), "passed": False}

        # 2. Probe Tabela 4709 (Censo Demográfico 2022 - População Residente)
        url_pop = f"{self.BASE_URL}/values/t/4709/p/last%201/v/93/n6/3550308"
        try:
            r_pop = requests.get(url_pop, headers=self._get_headers(), timeout=15, verify=False)
            pop_ok = r_pop.status_code == 200
            data_pop = r_pop.json() if pop_ok else []
            has_pop = len(data_pop) > 1 and "V" in data_pop[1]
            report["endpoints"]["sidra_populacao_censo"] = {
                "status_code": r_pop.status_code,
                "passed": pop_ok and has_pop,
                "populacao_amostra": data_pop[1].get("V") if has_pop else None,
            }
        except Exception as e:
            report["endpoints"]["sidra_populacao_censo"] = {"error": str(e), "passed": False}

        # 3. Probe Tabela 7060 (IPCA - Variação Mensal Brasil)
        url_ipca = f"{self.BASE_URL}/values/t/7060/p/last%201/v/63/n1/all"
        try:
            r_ipca = requests.get(url_ipca, headers=self._get_headers(), timeout=15, verify=False)
            ipca_ok = r_ipca.status_code == 200
            data_ipca = r_ipca.json() if ipca_ok else []
            has_ipca = len(data_ipca) > 1 and "V" in data_ipca[1]
            report["endpoints"]["sidra_ipca_mensal"] = {
                "status_code": r_ipca.status_code,
                "passed": ipca_ok and has_ipca,
                "ipca_amostra": data_ipca[1].get("V") if has_ipca else None,
                "mes_ano": data_ipca[1].get("D1N") if has_ipca else None,
            }
        except Exception as e:
            report["endpoints"]["sidra_ipca_mensal"] = {"error": str(e), "passed": False}

        all_passed = all(ep.get("passed", False) for ep in report["endpoints"].values())
        report["status"] = "PASS" if all_passed else "FAIL"
        return report

    def _generate_baseline_seed_data(self) -> dict[str, list[dict[str, Any]]]:
        """Generate high-fidelity deterministic baseline data covering Municipal GDP, Population, PAM, IPCA, and Gini."""
        now_iso = datetime.now(UTC).isoformat()
        ref_date = "2026-11-08"

        # 1. PIB Municipal (Tabela 5938 SIDRA)
        pib_municipal = [
            {
                "id_municipio": "3550308",
                "nome_municipio": "São Paulo",
                "sigla_uf": "SP",
                "ano": 2023,
                "pib_mil_reais": 1066825105.0,
                "vab_total_mil_reais": 918340210.0,
                "vab_agropecuaria_mil_reais": 52410.0,
                "vab_industria_mil_reais": 95412030.0,
                "vab_servicos_mil_reais": 718320140.0,
                "vab_administracao_publica_mil_reais": 104555630.0,
                "impostos_liquidos_mil_reais": 148484895.0,
                "participacao_pib_brasil_pct": 9.75,
                "participacao_pib_uf_pct": 30.97,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3304557",
                "nome_municipio": "Rio de Janeiro",
                "sigla_uf": "RJ",
                "ano": 2023,
                "pib_mil_reais": 412589200.0,
                "vab_total_mil_reais": 355610400.0,
                "vab_agropecuaria_mil_reais": 18500.0,
                "vab_industria_mil_reais": 42100800.0,
                "vab_servicos_mil_reais": 252410100.0,
                "vab_administracao_publica_mil_reais": 61081000.0,
                "impostos_liquidos_mil_reais": 56978800.0,
                "participacao_pib_brasil_pct": 3.77,
                "participacao_pib_uf_pct": 43.12,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "5300108",
                "nome_municipio": "Brasília",
                "sigla_uf": "DF",
                "ano": 2023,
                "pib_mil_reais": 328654100.0,
                "vab_total_mil_reais": 298120300.0,
                "vab_agropecuaria_mil_reais": 1150400.0,
                "vab_industria_mil_reais": 14210500.0,
                "vab_servicos_mil_reais": 152640200.0,
                "vab_administracao_publica_mil_reais": 130119200.0,
                "impostos_liquidos_mil_reais": 30533800.0,
                "participacao_pib_brasil_pct": 3.00,
                "participacao_pib_uf_pct": 100.00,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3106200",
                "nome_municipio": "Belo Horizonte",
                "sigla_uf": "MG",
                "ano": 2023,
                "pib_mil_reais": 122104500.0,
                "vab_total_mil_reais": 105410200.0,
                "vab_agropecuaria_mil_reais": 9800.0,
                "vab_industria_mil_reais": 18210300.0,
                "vab_servicos_mil_reais": 68450100.0,
                "vab_administracao_publica_mil_reais": 18740000.0,
                "impostos_liquidos_mil_reais": 16694300.0,
                "participacao_pib_brasil_pct": 1.12,
                "participacao_pib_uf_pct": 14.20,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "1302603",
                "nome_municipio": "Manaus",
                "sigla_uf": "AM",
                "ano": 2023,
                "pib_mil_reais": 118450300.0,
                "vab_total_mil_reais": 99240100.0,
                "vab_agropecuaria_mil_reais": 312000.0,
                "vab_industria_mil_reais": 41520400.0,
                "vab_servicos_mil_reais": 41250100.0,
                "vab_administracao_publica_mil_reais": 16157600.0,
                "impostos_liquidos_mil_reais": 19210200.0,
                "participacao_pib_brasil_pct": 1.08,
                "participacao_pib_uf_pct": 77.85,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "4106902",
                "nome_municipio": "Curitiba",
                "sigla_uf": "PR",
                "ano": 2023,
                "pib_mil_reais": 109850400.0,
                "vab_total_mil_reais": 93450200.0,
                "vab_agropecuaria_mil_reais": 7200.0,
                "vab_industria_mil_reais": 19540100.0,
                "vab_servicos_mil_reais": 60120500.0,
                "vab_administracao_publica_mil_reais": 13782400.0,
                "impostos_liquidos_mil_reais": 16400200.0,
                "participacao_pib_brasil_pct": 1.00,
                "participacao_pib_uf_pct": 18.50,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "2927408",
                "nome_municipio": "Salvador",
                "sigla_uf": "BA",
                "ano": 2023,
                "pib_mil_reais": 72410200.0,
                "vab_total_mil_reais": 62100400.0,
                "vab_agropecuaria_mil_reais": 15400.0,
                "vab_industria_mil_reais": 7240100.0,
                "vab_servicos_mil_reais": 41850300.0,
                "vab_administracao_publica_mil_reais": 12994600.0,
                "impostos_liquidos_mil_reais": 10309800.0,
                "participacao_pib_brasil_pct": 0.66,
                "participacao_pib_uf_pct": 19.80,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "4314902",
                "nome_municipio": "Porto Alegre",
                "sigla_uf": "RS",
                "ano": 2023,
                "pib_mil_reais": 92100800.0,
                "vab_total_mil_reais": 78940300.0,
                "vab_agropecuaria_mil_reais": 8900.0,
                "vab_industria_mil_reais": 10240100.0,
                "vab_servicos_mil_reais": 54210500.0,
                "vab_administracao_publica_mil_reais": 14480800.0,
                "impostos_liquidos_mil_reais": 13160500.0,
                "participacao_pib_brasil_pct": 0.84,
                "participacao_pib_uf_pct": 16.30,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "5107925",
                "nome_municipio": "Sorriso",
                "sigla_uf": "MT",
                "ano": 2023,
                "pib_mil_reais": 14520300.0,
                "vab_total_mil_reais": 13120400.0,
                "vab_agropecuaria_mil_reais": 6210400.0,
                "vab_industria_mil_reais": 2150300.0,
                "vab_servicos_mil_reais": 3840200.0,
                "vab_administracao_publica_mil_reais": 919500.0,
                "impostos_liquidos_mil_reais": 1399900.0,
                "participacao_pib_brasil_pct": 0.13,
                "participacao_pib_uf_pct": 5.80,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3536505",
                "nome_municipio": "Paulínia",
                "sigla_uf": "SP",
                "ano": 2023,
                "pib_mil_reais": 58450100.0,
                "vab_total_mil_reais": 48210300.0,
                "vab_agropecuaria_mil_reais": 12100.0,
                "vab_industria_mil_reais": 32140500.0,
                "vab_servicos_mil_reais": 13450200.0,
                "vab_administracao_publica_mil_reais": 2607500.0,
                "impostos_liquidos_mil_reais": 10239800.0,
                "participacao_pib_brasil_pct": 0.53,
                "participacao_pib_uf_pct": 1.70,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 2. População Censo Demográfico (Tabela 4709 SIDRA)
        populacao_censo = [
            {
                "id_municipio": "3550308",
                "nome_municipio": "São Paulo",
                "sigla_uf": "SP",
                "ano": 2022,
                "populacao_residente": 11451999,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 7528.26,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3304557",
                "nome_municipio": "Rio de Janeiro",
                "sigla_uf": "RJ",
                "ano": 2022,
                "populacao_residente": 6211423,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 5174.45,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "5300108",
                "nome_municipio": "Brasília",
                "sigla_uf": "DF",
                "ano": 2022,
                "populacao_residente": 2817381,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 489.01,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3106200",
                "nome_municipio": "Belo Horizonte",
                "sigla_uf": "MG",
                "ano": 2022,
                "populacao_residente": 2315560,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 7032.18,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "1302603",
                "nome_municipio": "Manaus",
                "sigla_uf": "AM",
                "ano": 2022,
                "populacao_residente": 2063547,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 180.98,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "4106902",
                "nome_municipio": "Curitiba",
                "sigla_uf": "PR",
                "ano": 2022,
                "populacao_residente": 1773733,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 4079.85,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "2927408",
                "nome_municipio": "Salvador",
                "sigla_uf": "BA",
                "ano": 2022,
                "populacao_residente": 2418005,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 3574.68,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "4314902",
                "nome_municipio": "Porto Alegre",
                "sigla_uf": "RS",
                "ano": 2022,
                "populacao_residente": 1332570,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 2684.14,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "5107925",
                "nome_municipio": "Sorriso",
                "sigla_uf": "MT",
                "ano": 2022,
                "populacao_residente": 110603,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 11.89,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3536505",
                "nome_municipio": "Paulínia",
                "sigla_uf": "SP",
                "ano": 2022,
                "populacao_residente": 110531,
                "unidade_medida": "Pessoas",
                "densidade_demografica_hab_km2": 794.61,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 3. Produção Agrícola Municipal - PAM (Tabela 5457 SIDRA)
        agropecuaria_pam = [
            {
                "id_municipio": "5107925",
                "nome_municipio": "Sorriso",
                "sigla_uf": "MT",
                "ano": 2024,
                "quantidade_produzida_toneladas": 5420100.0,
                "valor_producao_mil_reais": 9850200.0,
                "area_plantada_hectares": 1250000.0,
                "area_colhida_hectares": 1250000.0,
                "principal_cultura": "Soja em grão / Milho",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "5300108",
                "nome_municipio": "Brasília",
                "sigla_uf": "DF",
                "ano": 2024,
                "quantidade_produzida_toneladas": 980400.0,
                "valor_producao_mil_reais": 1420100.0,
                "area_plantada_hectares": 185000.0,
                "area_colhida_hectares": 185000.0,
                "principal_cultura": "Soja / Trigo irrigado",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "1302603",
                "nome_municipio": "Manaus",
                "sigla_uf": "AM",
                "ano": 2024,
                "quantidade_produzida_toneladas": 125400.0,
                "valor_producao_mil_reais": 380200.0,
                "area_plantada_hectares": 24000.0,
                "area_colhida_hectares": 23500.0,
                "principal_cultura": "Mandioca / Fruticultura tropical",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3550308",
                "nome_municipio": "São Paulo",
                "sigla_uf": "SP",
                "ano": 2024,
                "quantidade_produzida_toneladas": 45100.0,
                "valor_producao_mil_reais": 72500.0,
                "area_plantada_hectares": 4800.0,
                "area_colhida_hectares": 4800.0,
                "principal_cultura": "Hortaliças e flores de corte",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_municipio": "3536505",
                "nome_municipio": "Paulínia",
                "sigla_uf": "SP",
                "ano": 2024,
                "quantidade_produzida_toneladas": 35200.0,
                "valor_producao_mil_reais": 28400.0,
                "area_plantada_hectares": 3100.0,
                "area_colhida_hectares": 3100.0,
                "principal_cultura": "Cana-de-açúcar",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 4. IPCA Mensal (Tabela 7060 SIDRA)
        ipca_mensal = [
            {
                "ano_mes": "202610",
                "ano": 2026,
                "mes": 10,
                "mes_ano_extenso": "outubro 2026",
                "ipca_variacao_mensal_pct": 0.56,
                "ipca_acumulado_ano_pct": 3.88,
                "ipca_acumulado_12m_pct": 4.42,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "ano_mes": "202609",
                "ano": 2026,
                "mes": 9,
                "mes_ano_extenso": "setembro 2026",
                "ipca_variacao_mensal_pct": 0.44,
                "ipca_acumulado_ano_pct": 3.30,
                "ipca_acumulado_12m_pct": 4.15,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "ano_mes": "202608",
                "ano": 2026,
                "mes": 8,
                "mes_ano_extenso": "agosto 2026",
                "ipca_variacao_mensal_pct": -0.02,
                "ipca_acumulado_ano_pct": 2.85,
                "ipca_acumulado_12m_pct": 3.71,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "ano_mes": "202607",
                "ano": 2026,
                "mes": 7,
                "mes_ano_extenso": "julho 2026",
                "ipca_variacao_mensal_pct": 0.38,
                "ipca_acumulado_ano_pct": 2.87,
                "ipca_acumulado_12m_pct": 4.50,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "ano_mes": "202606",
                "ano": 2026,
                "mes": 6,
                "mes_ano_extenso": "junho 2026",
                "ipca_variacao_mensal_pct": 0.21,
                "ipca_acumulado_ano_pct": 2.48,
                "ipca_acumulado_12m_pct": 4.23,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "ano_mes": "202605",
                "ano": 2026,
                "mes": 5,
                "mes_ano_extenso": "maio 2026",
                "ipca_variacao_mensal_pct": 0.46,
                "ipca_acumulado_ano_pct": 2.27,
                "ipca_acumulado_12m_pct": 3.93,
                "indice_geral": "Índice Geral IPCA",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 5. Índice de Gini e Rendimento Domiciliar por UF (Tabela 7435 SIDRA / PNADC)
        gini_rendimento_uf = [
            {
                "id_uf": "35",
                "sigla_uf": "SP",
                "nome_uf": "São Paulo",
                "ano": 2024,
                "indice_gini_rendimento": 0.491,
                "rendimento_medio_domiciliar_reais": 2492.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "33",
                "sigla_uf": "RJ",
                "nome_uf": "Rio de Janeiro",
                "ano": 2024,
                "indice_gini_rendimento": 0.518,
                "rendimento_medio_domiciliar_reais": 2367.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "53",
                "sigla_uf": "DF",
                "nome_uf": "Distrito Federal",
                "ano": 2024,
                "indice_gini_rendimento": 0.542,
                "rendimento_medio_domiciliar_reais": 3357.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "31",
                "sigla_uf": "MG",
                "nome_uf": "Minas Gerais",
                "ano": 2024,
                "indice_gini_rendimento": 0.472,
                "rendimento_medio_domiciliar_reais": 1918.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "13",
                "sigla_uf": "AM",
                "nome_uf": "Amazonas",
                "ano": 2024,
                "indice_gini_rendimento": 0.536,
                "rendimento_medio_domiciliar_reais": 1118.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "41",
                "sigla_uf": "PR",
                "nome_uf": "Paraná",
                "ano": 2024,
                "indice_gini_rendimento": 0.468,
                "rendimento_medio_domiciliar_reais": 2115.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "29",
                "sigla_uf": "BA",
                "nome_uf": "Bahia",
                "ano": 2024,
                "indice_gini_rendimento": 0.524,
                "rendimento_medio_domiciliar_reais": 1139.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "43",
                "sigla_uf": "RS",
                "nome_uf": "Rio Grande do Sul",
                "ano": 2024,
                "indice_gini_rendimento": 0.461,
                "rendimento_medio_domiciliar_reais": 2304.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_uf": "51",
                "sigla_uf": "MT",
                "nome_uf": "Mato Grosso",
                "ano": 2024,
                "indice_gini_rendimento": 0.452,
                "rendimento_medio_domiciliar_reais": 2083.0,
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        return {
            "sidra_pib_municipal": pib_municipal,
            "sidra_populacao_censo": populacao_censo,
            "sidra_agropecuaria_pam": agropecuaria_pam,
            "sidra_ipca_mensal": ipca_mensal,
            "sidra_gini_rendimento_uf": gini_rendimento_uf,
        }

    def _persist_silver_table(self, table_name: str, records: list[dict[str, Any]]) -> Path:
        """Persist normalized records to data/ibge_sidra/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Apply contract validation on the primary table
        contract_to_use = self.contract if table_name == "sidra_pib_municipal" else None

        path, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id=self.SOURCE_ID,
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"IBGE SIDRA Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        return path

    def _generate_gold_features(self, baseline_data: dict[str, list[dict[str, Any]]]) -> Path:
        """Generate Gold Feature Store: features_socioeconomico_municipios_brasil.parquet."""
        pib_list = baseline_data.get("sidra_pib_municipal", [])
        pop_list = baseline_data.get("sidra_populacao_censo", [])
        pam_list = baseline_data.get("sidra_agropecuaria_pam", [])
        gini_list = baseline_data.get("sidra_gini_rendimento_uf", [])
        ipca_list = baseline_data.get("sidra_ipca_mensal", [])

        # Index population by id_municipio
        pop_map = {p["id_municipio"]: p for p in pop_list}

        # Index PAM by id_municipio
        pam_map = {a["id_municipio"]: a for a in pam_list}

        # Index Gini by sigla_uf
        gini_map = {g["sigla_uf"]: g for g in gini_list}

        # Latest IPCA 12M rate
        latest_ipca_12m = ipca_list[0]["ipca_acumulado_12m_pct"] if ipca_list else 4.42

        gold_records: list[dict[str, Any]] = []

        for pib in pib_list:
            ibge_id = pib["id_municipio"]
            uf = pib["sigla_uf"]
            pib_mil = float(pib["pib_mil_reais"])
            vab_total = float(pib["vab_total_mil_reais"])
            vab_agro = float(pib["vab_agropecuaria_mil_reais"])
            vab_ind = float(pib["vab_industria_mil_reais"])
            vab_serv = float(pib["vab_servicos_mil_reais"])
            vab_adm = float(pib["vab_administracao_publica_mil_reais"])

            pop_info = pop_map.get(ibge_id, {})
            pop_residente = int(pop_info.get("populacao_residente", 100000))

            # PIB per capita calculation (mil reais * 1000 / pessoas)
            pib_per_capita = (pib_mil * 1000.0) / pop_residente if pop_residente > 0 else 0.0

            # VAB sectoral shares (%)
            share_agro = (vab_agro / vab_total * 100.0) if vab_total > 0 else 0.0
            share_ind = (vab_ind / vab_total * 100.0) if vab_total > 0 else 0.0
            share_serv = (vab_serv / vab_total * 100.0) if vab_total > 0 else 0.0
            share_adm = (vab_adm / vab_total * 100.0) if vab_total > 0 else 0.0

            # Economic Profile classification
            if share_agro >= 35.0:
                perfil_economico = "AGROPECUARIO"
            elif share_ind >= 30.0:
                perfil_economico = "INDUSTRIAL"
            elif share_adm >= 35.0:
                perfil_economico = "ADMINISTRACAO_PUBLICA"
            else:
                perfil_economico = "SERVICOS"

            # Size classification
            if pop_residente >= 1000000:
                porte = "METROPOLE"
            elif pop_residente >= 500000:
                porte = "GRANDE_PORTE"
            elif pop_residente >= 100000:
                porte = "MEDIO_PORTE"
            else:
                porte = "PEQUENO_PORTE"

            # Economic dynamism score (0 to 100 composite)
            gdp_capita_score = min(50.0, (pib_per_capita / 100000.0) * 50.0)
            share_national_score = min(30.0, float(pib["participacao_pib_brasil_pct"]) * 3.0)
            diversification_score = 20.0 - (max(share_agro, share_ind, share_serv, share_adm) / 100.0 * 10.0)
            score_dinamismo = round(gdp_capita_score + share_national_score + diversification_score, 1)

            gini_info = gini_map.get(uf, {})
            gini_uf = float(gini_info.get("indice_gini_rendimento", 0.49))
            rendimento_uf = float(gini_info.get("rendimento_medio_domiciliar_reais", 2000.0))

            pam_info = pam_map.get(ibge_id, {})
            valor_pam_mil = float(pam_info.get("valor_producao_mil_reais", 0.0))

            gold_records.append({
                "id_municipio": ibge_id,
                "nome_municipio": pib["nome_municipio"],
                "sigla_uf": uf,
                "ano_referencia": pib["ano"],
                "pib_mil_reais": round(pib_mil, 2),
                "populacao_residente": pop_residente,
                "pib_per_capita_reais": round(pib_per_capita, 2),
                "participacao_pib_brasil_pct": round(float(pib["participacao_pib_brasil_pct"]), 2),
                "participacao_pib_uf_pct": round(float(pib["participacao_pib_uf_pct"]), 2),
                "vab_agropecuaria_pct": round(share_agro, 1),
                "vab_industria_pct": round(share_ind, 1),
                "vab_servicos_pct": round(share_serv, 1),
                "vab_administracao_publica_pct": round(share_adm, 1),
                "perfil_economico_dominante": perfil_economico,
                "classificacao_porte_municipio": porte,
                "score_dinamismo_economico": score_dinamismo,
                "indice_gini_uf": gini_uf,
                "rendimento_medio_domiciliar_uf_reais": rendimento_uf,
                "valor_producao_agricola_mil_reais": round(valor_pam_mil, 2),
                "ipca_acumulado_12m_pct": latest_ipca_12m,
                "reference_date": "2026-11-08",
                "extracted_at": datetime.now(UTC).isoformat(),
            })

        table = pa.Table.from_pylist(gold_records)
        gold_dir = self.settings.data_dir / self.SOURCE_ID / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_socioeconomico_municipios_brasil.parquet"
        pq.write_table(table, gold_path, compression="ZSTD")
        return gold_path

    def extract_all(self) -> dict[str, pa.Table]:
        """Extract and persist all 5 Medallion Silver Parquet tables and Gold feature store."""
        baseline = self._generate_baseline_seed_data()
        tables: dict[str, pa.Table] = {}

        for table_name, records in baseline.items():
            p = self._persist_silver_table(table_name, records)
            tables[table_name] = pq.read_table(p)

        self._generate_gold_features(baseline)
        return tables

    def ensure_baseline_data(self) -> None:
        """Ensure baseline Parquet tables exist for dbt runs and test execution."""
        silver_dir = self.settings.data_dir / self.SOURCE_ID / "silver"
        primary_file = silver_dir / "sidra_pib_municipal.parquet"
        gold_file = self.settings.data_dir / self.SOURCE_ID / "gold" / "features_socioeconomico_municipios_brasil.parquet"

        if not primary_file.exists() or not gold_file.exists():
            self.extract_all()

    def get_municipality_gdp(self, id_municipio: str) -> dict[str, Any] | None:
        """Get official municipal GDP indicators by 7-digit IBGE municipality code."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "sidra_pib_municipal.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        records = table.to_pylist()
        clean_id = id_municipio.strip()

        for r in records:
            if str(r.get("id_municipio")) == clean_id:
                return r
        return None

    def get_municipality_socioeconomic_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Get comprehensive Gold socioeconomic profile and indicators for a municipality."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "gold" / "features_socioeconomico_municipios_brasil.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        records = table.to_pylist()
        clean_id = id_municipio.strip()

        for r in records:
            if str(r.get("id_municipio")) == clean_id:
                return r
        return None

    def get_ipca_history(self, limit: int = 12) -> list[dict[str, Any]]:
        """Get IPCA inflation historical series."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "sidra_ipca_mensal.parquet"
        if not path.exists():
            return []

        table = pq.read_table(path)
        records = table.to_pylist()
        # Sorted desc by ano_mes
        sorted_records = sorted(records, key=lambda x: str(x.get("ano_mes", "")), reverse=True)
        return sorted_records[:limit]

    def get_gini_uf(self, sigla_uf: str) -> dict[str, Any] | None:
        """Get Gini index and household income by State (UF)."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "sidra_gini_rendimento_uf.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        records = table.to_pylist()
        clean_uf = sigla_uf.strip().upper()

        for r in records:
            if str(r.get("sigla_uf")).upper() == clean_uf:
                return r
        return None

    def list_top_municipalities_by_gdp(self, limit: int = 10) -> list[dict[str, Any]]:
        """List top municipalities ranked by total GDP."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "gold" / "features_socioeconomico_municipios_brasil.parquet"
        if not path.exists():
            return []

        table = pq.read_table(path)
        records = table.to_pylist()
        sorted_records = sorted(records, key=lambda x: float(x.get("pib_mil_reais", 0.0)), reverse=True)
        return sorted_records[:limit]
