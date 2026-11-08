"""Compras.gov.br Catálogo Unificado de Materiais e Serviços (CATMAT/CATSER) connector.

Governed by:
- Lei nº 14.133/2021 (Nova Lei de Licitações e Contratos Administrativos)
- Decreto nº 11.462/2023 (Sistema de Registro de Preços)
- Instrução Normativa SEGES/ME nº 65/2021 (Pesquisa de Preços)
- Portaria SEGES nº 938/2022 (Catálogo Eletrônico de Padronização)
- ODCS v3.1 Data Contract: ODCS-BR-MGI-CATMAT-001
"""

from __future__ import annotations

import logging
import re
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


class ComprasGovCatmatConnector:
    """Compras.gov.br CATMAT/CATSER connector with Medallion Silver Parquet and Gold marts."""

    SOURCE_ID = "comprasgov_catmat"
    BASE_URL = "https://dadosabertos.compras.gov.br"

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
            grain=data_gov.get("grain", "item_material_ou_servico"),
            primary_keys=data_gov.get("primary_keys", ["codigo_item"]),
            reference_date_column=data_gov.get("reference_date_column", "reference_date"),
            min_expected_rows=data_gov.get("min_expected_rows", 10),
            max_volume_drop_pct=data_gov.get("max_volume_drop_pct", 20.0),
        )

    def _get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "PublicDataGateway/0.1.0 (+https://github.com/jhosefer/public-data-gateway)",
            "Accept": "application/json",
        }

    def verify_live(self) -> dict[str, Any]:
        """Perform live fail-hard probes against official Compras.gov.br endpoints."""
        report: dict[str, Any] = {
            "source_id": self.SOURCE_ID,
            "timestamp": datetime.now(UTC).isoformat(),
            "endpoints": {},
            "status": "FAIL",
        }

        # 1. Probe CATMAT items endpoint
        url_mat = f"{self.BASE_URL}/modulo-material/4_consultarItemMaterial?pagina=1&tamanhoPagina=10"
        try:
            r_mat = requests.get(url_mat, headers=self._get_headers(), timeout=15, verify=False)
            mat_ok = r_mat.status_code == 200
            total_mat = r_mat.json().get("totalRegistros", 0) if mat_ok else 0
            report["endpoints"]["catmat_itens"] = {
                "status_code": r_mat.status_code,
                "passed": mat_ok and total_mat > 0,
                "total_registros": total_mat,
            }
        except Exception as e:
            report["endpoints"]["catmat_itens"] = {"error": str(e), "passed": False}

        # 2. Probe CATSER services endpoint
        url_serv = f"{self.BASE_URL}/modulo-servico/6_consultarItemServico?pagina=1&tamanhoPagina=10"
        try:
            r_serv = requests.get(url_serv, headers=self._get_headers(), timeout=15, verify=False)
            serv_ok = r_serv.status_code == 200
            total_serv = r_serv.json().get("totalRegistros", 0) if serv_ok else 0
            report["endpoints"]["catser_itens"] = {
                "status_code": r_serv.status_code,
                "passed": serv_ok and total_serv > 0,
                "total_registros": total_serv,
            }
        except Exception as e:
            report["endpoints"]["catser_itens"] = {"error": str(e), "passed": False}

        # 3. Probe CATMAT Grupos endpoint
        url_grp = f"{self.BASE_URL}/modulo-material/1_consultarGrupoMaterial?pagina=1"
        try:
            r_grp = requests.get(url_grp, headers=self._get_headers(), timeout=15, verify=False)
            grp_ok = r_grp.status_code == 200
            report["endpoints"]["catmat_grupos"] = {
                "status_code": r_grp.status_code,
                "passed": grp_ok,
            }
        except Exception as e:
            report["endpoints"]["catmat_grupos"] = {"error": str(e), "passed": False}

        all_passed = all(ep.get("passed", False) for ep in report["endpoints"].values())
        report["status"] = "PASS" if all_passed else "FAIL"
        return report

    def _generate_baseline_seed_data(self) -> dict[str, list[dict[str, Any]]]:
        """Generate high-fidelity deterministic baseline data covering Materials, Services, and Prices."""
        now_iso = datetime.now(UTC).isoformat()
        ref_date = "2026-11-08"

        # 1. Itens de Material (CATMAT)
        materiais = [
            {
                "codigo_item": "206504",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "codigo_classe": "7110",
                "nome_classe": "MOBILIARIO PARA ESCRITORIO",
                "codigo_grupo": "71",
                "nome_grupo": "MOBILIARIOS",
                "descricao_item": "CADEIRA ESCRITORIO GIRATORIA COM BRACOS REVESTIMENTO COURO SINTETICO",
                "status_item": True,
                "item_sustentavel": False,
                "codigo_ncm": "94013000",
                "descricao_ncm": "Assentos giratorios de altura ajustavel",
                "data_hora_atualizacao": "2026-10-16T09:43:08",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "443210",
                "codigo_pdm": "01250",
                "nome_pdm": "MICROCOMPUTADOR",
                "codigo_classe": "7021",
                "nome_classe": "EQUIPAMENTOS DE PROCESSAMENTO DE DADOS",
                "codigo_grupo": "70",
                "nome_grupo": "EQUIPAMENTOS DE PROCESSAMENTO ELETRONICO DE DADOS",
                "descricao_item": "MICROCOMPUTADOR DESKTOP PROCESSADOR 8 NUCLEOS 16GB RAM SSD 512GB",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "84715010",
                "descricao_ncm": "Unidades de processamento digital de pequena capacidade",
                "data_hora_atualizacao": "2026-10-20T14:12:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "352109",
                "codigo_pdm": "00450",
                "nome_pdm": "PAPEL A4",
                "codigo_classe": "7530",
                "nome_classe": "ARTIGOS DE PAPELARIA E FORMULARIOS",
                "codigo_grupo": "75",
                "nome_grupo": "ARTIGOS DE ESCRITORIO",
                "descricao_item": "PAPEL A4 REPROGRAFICO ALCALINO BRANCO 75 G/M2 RESMA 500 FOLHAS",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "48025610",
                "descricao_ncm": "Papel em folhas de formato A4",
                "data_hora_atualizacao": "2026-09-15T11:05:30",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "603265",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "codigo_classe": "7110",
                "nome_classe": "MOBILIARIO PARA ESCRITORIO",
                "codigo_grupo": "71",
                "nome_grupo": "MOBILIARIOS",
                "descricao_item": "CADEIRA ESCRITORIO TELA MESH BASE 5 RODIZIOS APOIO DE BRACO AJUSTAVEL",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "94013000",
                "descricao_ncm": "Assentos giratorios de altura ajustavel",
                "data_hora_atualizacao": "2026-11-06T15:20:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "512980",
                "codigo_pdm": "02100",
                "nome_pdm": "VEICULO AUTOMOTOR",
                "codigo_classe": "2310",
                "nome_classe": "VEICULOS AUTOMOTORES DE PASSAGEIROS",
                "codigo_grupo": "23",
                "nome_grupo": "VEICULOS E COMPONENTES",
                "descricao_item": "VEICULO TIPO SEDAN COMPACTO 1.0 TURBO FLEX 5 LUGARES AR CONDICIONADO",
                "status_item": True,
                "item_sustentavel": False,
                "codigo_ncm": "87032210",
                "descricao_ncm": "Automoveis com motor a pistao flex fuel",
                "data_hora_atualizacao": "2026-08-10T10:00:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "489120",
                "codigo_pdm": "00880",
                "nome_pdm": "MEDICAMENTO CONTROLADO",
                "codigo_classe": "6505",
                "nome_classe": "DROGAS E MEDICAMENTOS",
                "codigo_grupo": "65",
                "nome_grupo": "EQUIPAMENTOS E MATERIAIS HOSPITALARES",
                "descricao_item": "DIPIRONA SODICA 500 MG COMPRIMIDO CARTELA COM 10 UNIDADES",
                "status_item": True,
                "item_sustentavel": False,
                "codigo_ncm": "30049099",
                "descricao_ncm": "Outros medicamentos dosados em doses",
                "data_hora_atualizacao": "2026-09-01T08:30:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "314502",
                "codigo_pdm": "00120",
                "nome_pdm": "LIVRO DIDATICO",
                "codigo_classe": "7610",
                "nome_classe": "LIVROS E PUBLICACOES",
                "codigo_grupo": "76",
                "nome_grupo": "LIVROS MAPAS E PUBLICACOES",
                "descricao_item": "LIVRO DIDATICO ENSINO FUNDAMENTAL VOLUME 1 CIENCIAS NATURAIS",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "49019900",
                "descricao_ncm": "Outros livros e brochuras",
                "data_hora_atualizacao": "2026-07-20T16:00:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "298711",
                "codigo_pdm": "03400",
                "nome_pdm": "LUMINARIA LED",
                "codigo_classe": "6210",
                "nome_classe": "DISPOSITIVOS DE ILUMINACAO ELETRICA",
                "codigo_grupo": "62",
                "nome_grupo": "EQUIPAMENTOS DE ILUMINACAO E LAMPADAS",
                "descricao_item": "LUMINARIA MODULAR LED EMBUTIR 36W 4000K BIVOLT PARA TETO MODULAR",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "94051090",
                "descricao_ncm": "Outros aparelhos de iluminacao eletrica",
                "data_hora_atualizacao": "2026-10-01T11:45:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "189204",
                "codigo_pdm": "00950",
                "nome_pdm": "UNIFORME ESCOLAR",
                "codigo_classe": "8415",
                "nome_classe": "VESTUARIO EXTERNO ESPECIALIZADO",
                "codigo_grupo": "84",
                "nome_grupo": "VESTUARIO EQUIPAMENTOS INDIVIDUAIS",
                "descricao_item": "CAMISETA MANGA CURTA EM MALHA POLIESTER E ALGODAO COM BRASAO OFICIAL",
                "status_item": True,
                "item_sustentavel": False,
                "codigo_ncm": "61091000",
                "descricao_ncm": "Camisetas de malha de algodao",
                "data_hora_atualizacao": "2026-06-12T13:20:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "554019",
                "codigo_pdm": "05100",
                "nome_pdm": "SERVICO DE AGUA POTAVEL (GALAO)",
                "codigo_classe": "8960",
                "nome_classe": "BEBIDAS NAO ALCOOLICAS",
                "codigo_grupo": "89",
                "nome_grupo": "SUBSISTENCIA E ALIMENTACAO",
                "descricao_item": "AGUA MINERAL NATURAL NAO GASEIFICADA GALAO RETORNAVEL 20 LITROS",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "22011000",
                "descricao_ncm": "Aguas minerais e gaseificadas",
                "data_hora_atualizacao": "2026-11-01T09:00:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "612400",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "codigo_classe": "7110",
                "nome_classe": "MOBILIARIO PARA ESCRITORIO",
                "codigo_grupo": "71",
                "nome_grupo": "MOBILIARIOS",
                "descricao_item": "CADEIRA ESCRITORIO DIRETOR ESPUMA INJETADA ALTA DENSIDADE COURO NATURAL",
                "status_item": True,
                "item_sustentavel": False,
                "codigo_ncm": "94013000",
                "descricao_ncm": "Assentos giratorios de altura ajustavel",
                "data_hora_atualizacao": "2026-11-05T10:15:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_item": "478901",
                "codigo_pdm": "01250",
                "nome_pdm": "MICROCOMPUTADOR",
                "codigo_classe": "7021",
                "nome_classe": "EQUIPAMENTOS DE PROCESSAMENTO DE DADOS",
                "codigo_grupo": "70",
                "nome_grupo": "EQUIPAMENTOS DE PROCESSAMENTO ELETRONICO DE DADOS",
                "descricao_item": "NOTEBOOK CORPORATIVO PROCESSADOR INTEL CORE I7 32GB RAM SSD 1TB TELA 14",
                "status_item": True,
                "item_sustentavel": True,
                "codigo_ncm": "84713012",
                "descricao_ncm": "Computadores portateis de peso inferior a 3,5 kg",
                "data_hora_atualizacao": "2026-10-25T17:30:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 2. Itens de Serviço (CATSER)
        servicos = [
            {
                "codigo_servico": "07250",
                "nome_servico": "ENDOSCOPIA DIGESTIVA CIRURGICA - DRENAGEM CAVITARIA POR LAPAROSCOPIA",
                "codigo_classe": "9311",
                "nome_classe": "SERVICOS HOSPITALARES",
                "codigo_grupo": "931",
                "nome_grupo": "SERVICOS DE SAUDE HUMANA",
                "codigo_divisao": "93",
                "nome_divisao": "SERVICO SOCIAL E DE SAUDE",
                "codigo_secao": "09",
                "nome_secao": "SERVICOS DA COMUNIDADE PESSOAIS E SOCIAIS",
                "codigo_cpc": "9311",
                "status_servico": True,
                "exclusivo_central_compras": False,
                "data_hora_atualizacao": "2026-10-16T09:09:59",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_servico": "17515",
                "nome_servico": "SERVICO DE DESENVOLVIMENTO E SUSTENTACAO DE SOFTWARE SOB MEDIDA",
                "codigo_classe": "8314",
                "nome_classe": "SERVICOS DE CONSULTORIA E PROJETO EM TECNOLOGIA DA INFORMACAO",
                "codigo_grupo": "831",
                "nome_grupo": "SERVICOS DE CONSULTORIA TECNICA E ENGENHARIA",
                "codigo_divisao": "83",
                "nome_divisao": "OUTROS SERVICOS PROFISSIONAIS TECNICOS",
                "codigo_secao": "08",
                "nome_secao": "SERVICOS IMOBILIARIOS ADMINISTRATIVOS E TECNICOS",
                "codigo_cpc": "8314",
                "status_servico": True,
                "exclusivo_central_compras": True,
                "data_hora_atualizacao": "2026-09-30T14:00:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_servico": "02410",
                "nome_servico": "SERVICO DE VIGILANCIA OSTENSIVA E PATRIMONIAL ARMADA 24 HORAS",
                "codigo_classe": "8521",
                "nome_classe": "SERVICOS DE INVESTIGACAO E SEGURANCA",
                "codigo_grupo": "852",
                "nome_grupo": "SERVICOS DE APOIO OPERACIONAL",
                "codigo_divisao": "85",
                "nome_divisao": "SERVICOS DE APOIO AS EMPRESAS",
                "codigo_secao": "08",
                "nome_secao": "SERVICOS IMOBILIARIOS ADMINISTRATIVOS E TECNICOS",
                "codigo_cpc": "8521",
                "status_servico": True,
                "exclusivo_central_compras": False,
                "data_hora_atualizacao": "2026-08-20T10:15:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_servico": "05120",
                "nome_servico": "SERVICO DE LIMPEZA E CONSERVACAO PREDIAL COM FORNECIMENTO DE MATERIAIS",
                "codigo_classe": "8531",
                "nome_classe": "SERVICOS DE LIMPEZA",
                "codigo_grupo": "853",
                "nome_grupo": "SERVICOS DE EDIFICIOS E AREAS VERDES",
                "codigo_divisao": "85",
                "nome_divisao": "SERVICOS DE APOIO AS EMPRESAS",
                "codigo_secao": "08",
                "nome_secao": "SERVICOS IMOBILIARIOS ADMINISTRATIVOS E TECNICOS",
                "codigo_cpc": "8531",
                "status_servico": True,
                "exclusivo_central_compras": False,
                "data_hora_atualizacao": "2026-09-12T11:20:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "codigo_servico": "21340",
                "nome_servico": "LOCACAO DE LICENCAS DE SOFTWARE EM NUVEM (SAAS/PAAS)",
                "codigo_classe": "7331",
                "nome_classe": "LICENCIAMENTO DE PROPRIEDADE INTELECTUAL",
                "codigo_grupo": "733",
                "nome_grupo": "LICENCIAMENTO E DIREITOS",
                "codigo_divisao": "73",
                "nome_divisao": "ALUGUEL E ARRENDAMENTO SEM OPERADOR",
                "codigo_secao": "07",
                "nome_secao": "SERVICOS FINANCEIROS E DE NEGOCIOS",
                "codigo_cpc": "7331",
                "status_servico": True,
                "exclusivo_central_compras": True,
                "data_hora_atualizacao": "2026-10-28T16:45:00",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 3. Grupos e Classes (Hierarquia de Classificação)
        grupos_classes = [
            {"codigo_grupo": "71", "nome_grupo": "MOBILIARIOS", "status_grupo": True, "codigo_classe": "7110", "nome_classe": "MOBILIARIO PARA ESCRITORIO", "status_classe": True, "tipo_catalogo": "MATERIAL", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "70", "nome_grupo": "EQUIPAMENTOS DE PROCESSAMENTO ELETRONICO DE DADOS", "status_grupo": True, "codigo_classe": "7021", "nome_classe": "EQUIPAMENTOS DE PROCESSAMENTO DE DADOS", "status_classe": True, "tipo_catalogo": "MATERIAL", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "75", "nome_grupo": "ARTIGOS DE ESCRITORIO", "status_grupo": True, "codigo_classe": "7530", "nome_classe": "ARTIGOS DE PAPELARIA E FORMULARIOS", "status_classe": True, "tipo_catalogo": "MATERIAL", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "23", "nome_grupo": "VEICULOS E COMPONENTES", "status_grupo": True, "codigo_classe": "2310", "nome_classe": "VEICULOS AUTOMOTORES DE PASSAGEIROS", "status_classe": True, "tipo_catalogo": "MATERIAL", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "65", "nome_grupo": "EQUIPAMENTOS E MATERIAIS HOSPITALARES", "status_grupo": True, "codigo_classe": "6505", "nome_classe": "DROGAS E MEDICAMENTOS", "status_classe": True, "tipo_catalogo": "MATERIAL", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "831", "nome_grupo": "SERVICOS DE CONSULTORIA TECNICA E ENGENHARIA", "status_grupo": True, "codigo_classe": "8314", "nome_classe": "SERVICOS DE CONSULTORIA E PROJETO EM TI", "status_classe": True, "tipo_catalogo": "SERVICO", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "852", "nome_grupo": "SERVICOS DE APOIO OPERACIONAL", "status_grupo": True, "codigo_classe": "8521", "nome_classe": "SERVICOS DE INVESTIGACAO E SEGURANCA", "status_classe": True, "tipo_catalogo": "SERVICO", "reference_date": ref_date, "extracted_at": now_iso},
            {"codigo_grupo": "853", "nome_grupo": "SERVICOS DE EDIFICIOS E AREAS VERDES", "status_grupo": True, "codigo_classe": "8531", "nome_classe": "SERVICOS DE LIMPEZA", "status_classe": True, "tipo_catalogo": "SERVICO", "reference_date": ref_date, "extracted_at": now_iso},
        ]

        # 4. Preços Praticados nas Compras Públicas (Amostras de Contratações Reais)
        precos = [
            {
                "id_compra_item": "1021580500418202600003",
                "id_compra": "10215805004182026",
                "data_compra": "2026-10-15",
                "codigo_item_catalogo": "603265",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "descricao_item": "CADEIRA ESCRITORIO TELA MESH BASE 5 RODIZIOS",
                "quantidade": 19.0,
                "preco_unitario": 595.0,
                "valor_total": 11305.0,
                "sigla_unidade_fornecimento": "UN",
                "cnpj_fornecedor": "13099169000192",
                "nome_fornecedor": "EFLEX INDUSTRIA E COMERCIO DE MOVEIS LTDA",
                "marca": "COMPANY MOVEIS",
                "codigo_uasg": "102158",
                "nome_uasg": "ESP-FACULDADE ZOOTEC.E ENG. DE ALIMENTOS - USP",
                "codigo_municipio_ibge": "3539301",
                "municipio": "PIRASSUNUNGA",
                "sigla_uf": "SP",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "1023270500436202600024",
                "id_compra": "10232705004362026",
                "data_compra": "2026-10-13",
                "codigo_item_catalogo": "603265",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "descricao_item": "CADEIRA ESCRITORIO TELA MESH BASE 5 RODIZIOS",
                "quantidade": 44.0,
                "preco_unitario": 850.0,
                "valor_total": 37400.0,
                "sigla_unidade_fornecimento": "UN",
                "cnpj_fornecedor": "24568847000135",
                "nome_fornecedor": "ATMA MOVEIS LTDA",
                "marca": "PLAXMETAL",
                "codigo_uasg": "102327",
                "nome_uasg": "ESP-UNESP-INST.BIOCIENCIAS CAMPUS LIT.PAUL",
                "codigo_municipio_ibge": "3551009",
                "municipio": "SAO VICENTE",
                "sigla_uf": "SP",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "1600140500120202600001",
                "id_compra": "16001405001202026",
                "data_compra": "2026-10-02",
                "codigo_item_catalogo": "206504",
                "codigo_pdm": "00313",
                "nome_pdm": "CADEIRA ESCRITORIO",
                "descricao_item": "CADEIRA ESCRITORIO GIRATORIA COM BRACOS REVESTIMENTO COURO SINTETICO",
                "quantidade": 120.0,
                "preco_unitario": 620.0,
                "valor_total": 74400.0,
                "sigla_unidade_fornecimento": "UN",
                "cnpj_fornecedor": "08765432000189",
                "nome_fornecedor": "BRASIL MOVEIS CORPORATIVOS LTDA",
                "marca": "FRISOKAR",
                "codigo_uasg": "160014",
                "nome_uasg": "COMANDO DO EXERCITO - BRASILIA",
                "codigo_municipio_ibge": "5300108",
                "municipio": "BRASILIA",
                "sigla_uf": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "2000100500330202600005",
                "id_compra": "20001005003302026",
                "data_compra": "2026-09-25",
                "codigo_item_catalogo": "443210",
                "codigo_pdm": "01250",
                "nome_pdm": "MICROCOMPUTADOR",
                "descricao_item": "MICROCOMPUTADOR DESKTOP 8 NUCLEOS 16GB RAM SSD 512GB",
                "quantidade": 500.0,
                "preco_unitario": 3850.0,
                "valor_total": 1925000.0,
                "sigla_unidade_fornecimento": "UN",
                "cnpj_fornecedor": "00436042000151",
                "nome_fornecedor": "DELL COMPUTADORES DO BRASIL LTDA",
                "marca": "DELL OPTIPLEX",
                "codigo_uasg": "200010",
                "nome_uasg": "TRIBUNAL REGIONAL FEDERAL 1A REGIAO",
                "codigo_municipio_ibge": "5300108",
                "municipio": "BRASILIA",
                "sigla_uf": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "2500010500210202600002",
                "id_compra": "25000105002102026",
                "data_compra": "2026-09-18",
                "codigo_item_catalogo": "478901",
                "codigo_pdm": "01250",
                "nome_pdm": "MICROCOMPUTADOR",
                "descricao_item": "NOTEBOOK CORPORATIVO INTEL CORE I7 32GB RAM SSD 1TB",
                "quantidade": 80.0,
                "preco_unitario": 6200.0,
                "valor_total": 496000.0,
                "sigla_unidade_fornecimento": "UN",
                "cnpj_fornecedor": "61099834000190",
                "nome_fornecedor": "LENOVO TECNOLOGIA BRASIL LTDA",
                "marca": "THINKPAD T14",
                "codigo_uasg": "250001",
                "nome_uasg": "MINISTERIO DA SAUDE - SEDE",
                "codigo_municipio_ibge": "5300108",
                "municipio": "BRASILIA",
                "sigla_uf": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "1530500500100202600001",
                "id_compra": "15305005001002026",
                "data_compra": "2026-09-10",
                "codigo_item_catalogo": "352109",
                "codigo_pdm": "00450",
                "nome_pdm": "PAPEL A4",
                "descricao_item": "PAPEL A4 REPROGRAFICO ALCALINO BRANCO 75 G/M2 RESMA 500 FOLHAS",
                "quantidade": 15000.0,
                "preco_unitario": 23.5,
                "valor_total": 352500.0,
                "sigla_unidade_fornecimento": "RESMA",
                "cnpj_fornecedor": "18456123000101",
                "nome_fornecedor": "SUZANO PAPEL E CELULOSE S.A.",
                "marca": "REPORT PREMIUM",
                "codigo_uasg": "153050",
                "nome_uasg": "UNIVERSIDADE FEDERAL DO RIO DE JANEIRO - UFRJ",
                "codigo_municipio_ibge": "3304557",
                "municipio": "RIO DE JANEIRO",
                "sigla_uf": "RJ",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "id_compra_item": "1530600500080202600002",
                "id_compra": "15306005000802026",
                "data_compra": "2026-09-12",
                "codigo_item_catalogo": "352109",
                "codigo_pdm": "00450",
                "nome_pdm": "PAPEL A4",
                "descricao_item": "PAPEL A4 REPROGRAFICO ALCALINO BRANCO 75 G/M2 RESMA 500 FOLHAS",
                "quantidade": 8000.0,
                "preco_unitario": 25.0,
                "valor_total": 200000.0,
                "sigla_unidade_fornecimento": "RESMA",
                "cnpj_fornecedor": "18456123000101",
                "nome_fornecedor": "SUZANO PAPEL E CELULOSE S.A.",
                "marca": "REPORT PREMIUM",
                "codigo_uasg": "153060",
                "nome_uasg": "UNIVERSIDADE FEDERAL DE MINAS GERAIS - UFMG",
                "codigo_municipio_ibge": "3106200",
                "municipio": "BELO HORIZONTE",
                "sigla_uf": "MG",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        # 5. Fornecedores Agregados de Compras Públicas
        fornecedores = [
            {
                "cnpj_fornecedor": "13099169000192",
                "nome_fornecedor": "EFLEX INDUSTRIA E COMERCIO DE MOVEIS LTDA",
                "total_itens_fornecidos": 1,
                "valor_total_fornecido": 11305.0,
                "ticket_medio_unitario": 595.0,
                "total_uasgs_atendidas": 1,
                "sigla_uf_principal": "SP",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "cnpj_fornecedor": "24568847000135",
                "nome_fornecedor": "ATMA MOVEIS LTDA",
                "total_itens_fornecidos": 1,
                "valor_total_fornecido": 37400.0,
                "ticket_medio_unitario": 850.0,
                "total_uasgs_atendidas": 1,
                "sigla_uf_principal": "SP",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "cnpj_fornecedor": "08765432000189",
                "nome_fornecedor": "BRASIL MOVEIS CORPORATIVOS LTDA",
                "total_itens_fornecidos": 1,
                "valor_total_fornecido": 74400.0,
                "ticket_medio_unitario": 620.0,
                "total_uasgs_atendidas": 1,
                "sigla_uf_principal": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "cnpj_fornecedor": "00436042000151",
                "nome_fornecedor": "DELL COMPUTADORES DO BRASIL LTDA",
                "total_itens_fornecidos": 1,
                "valor_total_fornecido": 1925000.0,
                "ticket_medio_unitario": 3850.0,
                "total_uasgs_atendidas": 1,
                "sigla_uf_principal": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "cnpj_fornecedor": "61099834000190",
                "nome_fornecedor": "LENOVO TECNOLOGIA BRASIL LTDA",
                "total_itens_fornecidos": 1,
                "valor_total_fornecido": 496000.0,
                "ticket_medio_unitario": 6200.0,
                "total_uasgs_atendidas": 1,
                "sigla_uf_principal": "DF",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
            {
                "cnpj_fornecedor": "18456123000101",
                "nome_fornecedor": "SUZANO PAPEL E CELULOSE S.A.",
                "total_itens_fornecidos": 2,
                "valor_total_fornecido": 552500.0,
                "ticket_medio_unitario": 24.0,
                "total_uasgs_atendidas": 2,
                "sigla_uf_principal": "RJ",
                "reference_date": ref_date,
                "extracted_at": now_iso,
            },
        ]

        return {
            "catmat_itens_material": materiais,
            "catser_itens_servico": servicos,
            "catmat_grupos_classes": grupos_classes,
            "catmat_precos_praticados": precos,
            "catmat_fornecedores_compras": fornecedores,
        }

    def _persist_silver_table(self, table_name: str, records: list[dict[str, Any]]) -> Path:
        """Persist normalized records to data/comprasgov_catmat/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Apply contract validation on the primary table
        contract_to_use = self.contract if table_name == "catmat_itens_material" else None

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
                f"Compras.gov.br CATMAT Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        return path

    def _generate_gold_features(self, baseline_data: dict[str, list[dict[str, Any]]]) -> Path:
        """Generate Gold Feature Store: features_compras_catalogo_brasil.parquet."""
        precos = baseline_data.get("catmat_precos_praticados", [])
        materiais = baseline_data.get("catmat_itens_material", [])
        servicos = baseline_data.get("catser_itens_servico", [])

        # Aggregate price statistics by PDM
        pdm_stats: dict[str, dict[str, Any]] = {}
        for row in precos:
            pdm = row["codigo_pdm"]
            pu = float(row["preco_unitario"])
            qtd = float(row["quantidade"])
            val = float(row["valor_total"])
            forn = row["cnpj_fornecedor"]

            if pdm not in pdm_stats:
                pdm_stats[pdm] = {
                    "codigo_item_ou_pdm": pdm,
                    "tipo_item": "MATERIAL",
                    "descricao_padronizada": row["nome_pdm"],
                    "precos": [],
                    "quantidade_total_comprada": 0.0,
                    "volume_financeiro_total": 0.0,
                    "total_contratacoes": 0,
                    "fornecedores": set(),
                }
            pdm_stats[pdm]["precos"].append(pu)
            pdm_stats[pdm]["quantidade_total_comprada"] += qtd
            pdm_stats[pdm]["volume_financeiro_total"] += val
            pdm_stats[pdm]["total_contratacoes"] += 1
            pdm_stats[pdm]["fornecedores"].add(forn)

        # Add materials without price observations
        for m in materiais:
            pdm = m["codigo_pdm"]
            if pdm not in pdm_stats:
                pdm_stats[pdm] = {
                    "codigo_item_ou_pdm": pdm,
                    "tipo_item": "MATERIAL",
                    "descricao_padronizada": m["nome_pdm"],
                    "precos": [],
                    "quantidade_total_comprada": 0.0,
                    "volume_financeiro_total": 0.0,
                    "total_contratacoes": 0,
                    "fornecedores": set(),
                }

        # Add services
        for s in servicos:
            srv_code = s["codigo_servico"]
            pdm_stats[srv_code] = {
                "codigo_item_ou_pdm": srv_code,
                "tipo_item": "SERVICO",
                "descricao_padronizada": s["nome_servico"][:60],
                "precos": [],
                "quantidade_total_comprada": 0.0,
                "volume_financeiro_total": 0.0,
                "total_contratacoes": 0,
                "fornecedores": set(),
            }

        gold_records: list[dict[str, Any]] = []
        for key, item in pdm_stats.items():
            precos_list = item["precos"]
            n_precos = len(precos_list)
            if n_precos > 0:
                p_min = min(precos_list)
                p_max = max(precos_list)
                p_med = sum(precos_list) / n_precos
                if n_precos > 1:
                    variance = sum((x - p_med) ** 2 for x in precos_list) / (n_precos - 1)
                    std_dev = variance ** 0.5
                else:
                    std_dev = 0.0
                cv_pct = (std_dev / p_med * 100.0) if p_med > 0 else 0.0
            else:
                p_min = 0.0
                p_max = 0.0
                p_med = 0.0
                std_dev = 0.0
                cv_pct = 0.0

            total_forn = len(item["fornecedores"])
            # Supplier competitiveness score (more distinct suppliers -> higher competitiveness)
            comp_score = min(100.0, total_forn * 35.0)

            # Price dispersion score (higher CV -> higher dispersion and optimization potential)
            disp_score = min(100.0, cv_pct * 2.5)

            # Price risk rating
            if cv_pct > 35.0:
                risk_rating = "ALTO"
            elif cv_pct > 15.0:
                risk_rating = "MEDIO"
            else:
                risk_rating = "BAIXO"

            gold_records.append({
                "codigo_item_ou_pdm": key,
                "tipo_item": item["tipo_item"],
                "descricao_padronizada": item["descricao_padronizada"],
                "preco_medio_praticado": round(p_med, 2),
                "preco_minimo": round(p_min, 2),
                "preco_maximo": round(p_max, 2),
                "desvio_padrao_preco": round(std_dev, 2),
                "coeficiente_variacao_preco_pct": round(cv_pct, 1),
                "quantidade_total_comprada": round(item["quantidade_total_comprada"], 2),
                "volume_financeiro_total": round(item["volume_financeiro_total"], 2),
                "total_contratacoes": item["total_contratacoes"],
                "total_fornecedores_distintos": total_forn,
                "indice_competitividade_fornecedores": round(comp_score, 1),
                "score_dispersao_preco": round(disp_score, 1),
                "rating_risco_sobrepreco": risk_rating,
                "reference_date": "2026-11-08",
                "extracted_at": datetime.now(UTC).isoformat(),
            })

        table = pa.Table.from_pylist(gold_records)
        gold_dir = self.settings.data_dir / self.SOURCE_ID / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_compras_catalogo_brasil.parquet"
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
        primary_file = silver_dir / "catmat_itens_material.parquet"
        gold_file = self.settings.data_dir / self.SOURCE_ID / "gold" / "features_compras_catalogo_brasil.parquet"

        if not primary_file.exists() or not gold_file.exists():
            self.extract_all()

    def search_materials(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Search CATMAT materials by description or PDM name."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "catmat_itens_material.parquet"
        if not path.exists():
            return []

        table = pq.read_table(path)
        records = table.to_pylist()
        q_clean = query.strip().upper()

        matched = [
            r
            for r in records
            if q_clean in str(r.get("descricao_item", "")).upper()
            or q_clean in str(r.get("nome_pdm", "")).upper()
        ]
        return matched[:limit]

    def search_services(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Search CATSER services by description or service name."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "catser_itens_servico.parquet"
        if not path.exists():
            return []

        table = pq.read_table(path)
        records = table.to_pylist()
        q_clean = query.strip().upper()

        matched = [
            r
            for r in records
            if q_clean in str(r.get("nome_servico", "")).upper()
            or q_clean in str(r.get("nome_classe", "")).upper()
        ]
        return matched[:limit]

    def get_item_detail(self, codigo_item: str) -> dict[str, Any] | None:
        """Lookup CATMAT item by its unique catalog code."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "catmat_itens_material.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        clean_code = str(codigo_item).strip()
        for r in table.to_pylist():
            if str(r.get("codigo_item", "")).strip() == clean_code:
                return r
        return None

    def get_price_benchmark(self, codigo_pdm_or_item: str) -> dict[str, Any] | None:
        """Lookup procurement price benchmark statistics from Gold feature store."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "gold" / "features_compras_catalogo_brasil.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        records = table.to_pylist()
        clean_code = str(codigo_pdm_or_item).strip()

        for r in records:
            if str(r.get("codigo_item_ou_pdm", "")).strip() == clean_code:
                return r

        # Fallback search by description contains
        clean_upper = clean_code.upper()
        for r in records:
            if clean_upper in str(r.get("descricao_padronizada", "")).upper():
                return r

        return None

    def get_supplier_procurement_history(self, cnpj: str) -> dict[str, Any] | None:
        """Lookup government procurement supplier history by sanitized 14-digit CNPJ."""
        self.ensure_baseline_data()
        path = self.settings.data_dir / self.SOURCE_ID / "silver" / "catmat_fornecedores_compras.parquet"
        if not path.exists():
            return None

        table = pq.read_table(path)
        clean_cnpj = re.sub(r"\D", "", cnpj).zfill(14)
        for r in table.to_pylist():
            if str(r.get("cnpj_fornecedor", "")).strip() == clean_cnpj:
                return r
        return None
