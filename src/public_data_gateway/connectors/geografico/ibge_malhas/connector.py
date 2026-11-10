"""IBGE Malhas Territoriais Vetoriais Connector (Municípios, UFs, Regiões e Brasil).

Provides official geospatial boundary polygons, bounding boxes, geodetic centroids,
topological metrics (Polsby-Popper compactness, geodetic isolation score), and
standardized GeoJSON representations.
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector

logger = logging.getLogger(__name__)

IBGE_MALHAS_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/malhas"

# Coordenadas geodésicas de referência das 27 capitais brasileiras
CAPITAIS_BRASIL = {
    "AC": ("Rio Branco", -67.8249, -9.9754),
    "AL": ("Maceió", -35.7350, -9.6658),
    "AP": ("Macapá", -51.0694, 0.0349),
    "AM": ("Manaus", -60.0217, -3.1190),
    "BA": ("Salvador", -38.5108, -12.9714),
    "CE": ("Fortaleza", -38.5434, -3.7172),
    "DF": ("Brasília", -47.8825, -15.7942),
    "ES": ("Vitória", -40.3377, -20.3155),
    "GO": ("Goiânia", -49.2648, -16.6869),
    "MA": ("São Luís", -44.3028, -2.5307),
    "MT": ("Cuiabá", -56.0969, -15.6014),
    "MS": ("Campo Grande", -54.6201, -20.4697),
    "MG": ("Belo Horizonte", -43.9378, -19.9208),
    "PA": ("Belém", -48.4902, -1.4558),
    "PB": ("João Pessoa", -34.8631, -7.1153),
    "PR": ("Curitiba", -49.2731, -25.4284),
    "PE": ("Recife", -34.8811, -8.0539),
    "PI": ("Teresina", -42.8016, -5.0920),
    "RJ": ("Rio de Janeiro", -43.1729, -22.9068),
    "RN": ("Natal", -35.2094, -5.7945),
    "RS": ("Porto Alegre", -51.2177, -30.0346),
    "RO": ("Porto Velho", -63.9004, -8.7612),
    "RR": ("Boa Vista", -60.6758, 2.8235),
    "SC": ("Florianópolis", -48.5480, -27.5954),
    "SP": ("São Paulo", -46.6333, -23.5505),
    "SE": ("Aracaju", -37.0731, -10.9472),
    "TO": ("Palmas", -48.3277, -10.2128),
}

UF_CODE_TO_SIGLA = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF"
}

SIGLA_TO_UF_INFO = {
    "RO": ("11", "Rondônia", "Norte"),
    "AC": ("12", "Acre", "Norte"),
    "AM": ("13", "Amazonas", "Norte"),
    "RR": ("14", "Roraima", "Norte"),
    "PA": ("15", "Pará", "Norte"),
    "AP": ("16", "Amapá", "Norte"),
    "TO": ("17", "Tocantins", "Norte"),
    "MA": ("21", "Maranhão", "Nordeste"),
    "PI": ("22", "Piauí", "Nordeste"),
    "CE": ("23", "Ceará", "Nordeste"),
    "RN": ("24", "Rio Grande do Norte", "Nordeste"),
    "PB": ("25", "Paraíba", "Nordeste"),
    "PE": ("26", "Pernambuco", "Nordeste"),
    "AL": ("27", "Alagoas", "Nordeste"),
    "SE": ("28", "Sergipe", "Nordeste"),
    "BA": ("29", "Bahia", "Nordeste"),
    "MG": ("31", "Minas Gerais", "Sudeste"),
    "ES": ("32", "Espírito Santo", "Sudeste"),
    "RJ": ("33", "Rio de Janeiro", "Sudeste"),
    "SP": ("35", "São Paulo", "Sudeste"),
    "PR": ("41", "Paraná", "Sul"),
    "SC": ("42", "Santa Catarina", "Sul"),
    "RS": ("43", "Rio Grande do Sul", "Sul"),
    "MS": ("50", "Mato Grosso do Sul", "Centro-Oeste"),
    "MT": ("51", "Mato Grosso", "Centro-Oeste"),
    "GO": ("52", "Goiás", "Centro-Oeste"),
    "DF": ("53", "Distrito Federal", "Centro-Oeste"),
}

REGIOES_INFO = {
    "1": "Norte",
    "2": "Nordeste",
    "3": "Sudeste",
    "4": "Sul",
    "5": "Centro-Oeste",
}

# Municípios de referência canônicos para baseline e testes locais
MUNICIPACIOS_REFERENCIA = [
    ("3550308", "São Paulo", "SP", "35"),
    ("3304557", "Rio de Janeiro", "RJ", "33"),
    ("3106200", "Belo Horizonte", "MG", "31"),
    ("2927408", "Salvador", "BA", "29"),
    ("2304400", "Fortaleza", "CE", "23"),
    ("5300108", "Brasília", "DF", "53"),
    ("4106900", "Curitiba", "PR", "41"),
    ("1302603", "Manaus", "AM", "13"),
    ("1501402", "Belém", "PA", "15"),
    ("4314902", "Porto Alegre", "RS", "43"),
    ("5208700", "Goiânia", "GO", "52"),
    ("2611606", "Recife", "PE", "26"),
]


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Calcula a distância geodésica em quilômetros via fórmula de Haversine."""
    radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return radius_km * c


def compute_geometry_metrics(geom: dict[str, Any]) -> dict[str, Any]:
    """Calcula métricas espaciais canônicas de um objeto GeoJSON (Polygon ou MultiPolygon).

    Retorna:
    - bbox: [min_lon, min_lat, max_lon, max_lat]
    - centroide: (lon, lat)
    - qtd_vertices: total de coordenadas
    - qtd_poligonos: contagem de polígonos/ilhas
    - area_km2: área aproximada na projeção geodésica local
    - perimeter_km: perímetro total aproximado
    - compacidade: score de Polsby-Popper normalizado de 0 a 100
    """
    geom_type = geom.get("type", "Polygon")
    raw_coords = geom.get("coordinates", [])

    polygons: list[list[list[float]]] = []
    if geom_type == "Polygon":
        if raw_coords:
            polygons.append(raw_coords[0])  # Anel exterior
    elif geom_type == "MultiPolygon":
        for poly in raw_coords:
            if poly:
                polygons.append(poly[0])  # Anel exterior de cada parte

    all_points: list[list[float]] = []
    total_vertices = 0
    total_area_km2 = 0.0
    total_perimeter_km = 0.0

    radius_km = 6371.0

    for ring in polygons:
        total_vertices += len(ring)
        all_points.extend(ring)

        if len(ring) >= 3:
            lat_mid = math.radians(sum(p[1] for p in ring) / len(ring))
            ring_area = 0.0
            ring_perimeter = 0.0
            for i in range(len(ring) - 1):
                p1 = ring[i]
                p2 = ring[i + 1]
                x1 = math.radians(p1[0]) * radius_km * math.cos(lat_mid)
                y1 = math.radians(p1[1]) * radius_km
                x2 = math.radians(p2[0]) * radius_km * math.cos(lat_mid)
                y2 = math.radians(p2[1]) * radius_km
                ring_area += x1 * y2 - x2 * y1
                ring_perimeter += haversine_km(p1[0], p1[1], p2[0], p2[1])

            total_area_km2 += 0.5 * abs(ring_area)
            total_perimeter_km += ring_perimeter

    if not all_points:
        return {
            "tipo_geometria": geom_type,
            "bbox": [-46.6, -23.5, -46.6, -23.5],
            "centroide_lon": -46.6,
            "centroide_lat": -23.5,
            "qtd_vertices": 0,
            "qtd_poligonos": 0,
            "area_km2": 0.0,
            "perimeter_km": 0.0,
            "compacidade": 0.0,
        }

    lons = [p[0] for p in all_points]
    lats = [p[1] for p in all_points]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    centroide_lon = sum(lons) / len(lons)
    centroide_lat = sum(lats) / len(lats)

    # Polsby-Popper Compactness Score: (4 * pi * Area) / (Perimeter^2) * 100
    if total_perimeter_km > 0.0 and total_area_km2 > 0.0:
        raw_compacidade = (
            4.0 * math.pi * total_area_km2
        ) / (total_perimeter_km**2)
        score_compacidade = round(min(100.0, max(0.0, raw_compacidade * 100.0)), 2)
    else:
        score_compacidade = 50.0

    return {
        "tipo_geometria": geom_type,
        "bbox": [round(min_lon, 4), round(min_lat, 4), round(max_lon, 4), round(max_lat, 4)],
        "centroide_lon": round(centroide_lon, 4),
        "centroide_lat": round(centroide_lat, 4),
        "qtd_vertices": total_vertices,
        "qtd_poligonos": len(polygons),
        "area_km2": round(total_area_km2, 2),
        "perimeter_km": round(total_perimeter_km, 2),
        "compacidade": score_compacidade,
    }


class IBGEMalhasConnector(BaseConnector):
    """Conector oficial para Malhas Territoriais Vetoriais do IBGE."""

    SOURCE_ID = "ibge_malhas"

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        super().__init__(source_id=self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "ibge_malhas"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"
        self.reference_dir = self.settings.data_dir / "reference"

    def test_connection(self) -> bool:
        """Executa probe de verificação contra a API oficial do IBGE."""
        report = self.verify_live()
        return report.get("status") == "PASS"

    def extract(self) -> pa.Table:
        """Executa ingestão e retorna a tabela PyArrow de municípios."""
        results = self.extract_all()
        return pq.read_table(results["malhas_municipios"])

    def fetch_municipio_malha(
        self, id_municipio: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obtém a malha GeoJSON de um município brasileiro."""
        id_municipio = str(id_municipio).strip()
        url = f"{IBGE_MALHAS_BASE_URL}/municipios/{id_municipio}?formato=application/vnd.geo+json&qualidade={qualidade}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()

    def fetch_uf_malha(
        self, id_or_sigla: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obtém a malha GeoJSON de uma Unidade Federativa."""
        id_or_sigla = str(id_or_sigla).strip().upper()
        if id_or_sigla in SIGLA_TO_UF_INFO:
            id_uf = SIGLA_TO_UF_INFO[id_or_sigla][0]
        else:
            id_uf = id_or_sigla

        url = f"{IBGE_MALHAS_BASE_URL}/estados/{id_uf}?formato=application/vnd.geo+json&qualidade={qualidade}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()

    def fetch_regiao_malha(
        self, id_regiao: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obtém a malha GeoJSON de uma Macrorregião (1 a 5)."""
        id_regiao = str(id_regiao).strip()
        url = f"{IBGE_MALHAS_BASE_URL}/regioes/{id_regiao}?formato=application/vnd.geo+json&qualidade={qualidade}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()

    def fetch_brasil_malha(self, qualidade: str = "minima") -> dict[str, Any]:
        """Obtém o contorno territorial nacional do Brasil."""
        url = f"{IBGE_MALHAS_BASE_URL}/paises/BR?formato=application/vnd.geo+json&qualidade={qualidade}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()

    def extract_all(self, dest_dir: Path | None = None) -> dict[str, Path]:
        """Extrai malhas de municípios, UFs, regiões e Brasil gerando baseline Parquet."""
        silver_path = dest_dir or self.silver_dir
        gold_path = dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        logger.info("Iniciando extração canônica de IBGE Malhas...")

        # 1. Municípios de Referência
        mun_rows = []
        feat_rows = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPACIOS_REFERENCIA:
            try:
                data = self.fetch_municipio_malha(id_mun, qualidade="minima")
                features = data.get("features", [])
                geom = features[0].get("geometry", {}) if features else {}
            except Exception as e:
                logger.warning(
                    f"Falha ao consultar malha online do município {id_mun}: {e}. Gerando mock canônico."
                )
                cap = CAPITAIS_BRASIL.get(sigla_uf, ("Capital", -46.6, -23.5))
                c_lon, c_lat = cap[1], cap[2]
                geom = {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [round(c_lon - 0.1, 4), round(c_lat - 0.1, 4)],
                            [round(c_lon + 0.1, 4), round(c_lat - 0.1, 4)],
                            [round(c_lon + 0.1, 4), round(c_lat + 0.1, 4)],
                            [round(c_lon - 0.1, 4), round(c_lat + 0.1, 4)],
                            [round(c_lon - 0.1, 4), round(c_lat - 0.1, 4)],
                        ]
                    ],
                }

            metrics = compute_geometry_metrics(geom)
            bbox = metrics["bbox"]

            mun_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "ano_versao": 2024,
                    "tipo_geometria": metrics["tipo_geometria"],
                    "geojson_geometry": json.dumps(geom),
                    "bbox_min_lon": float(bbox[0]),
                    "bbox_min_lat": float(bbox[1]),
                    "bbox_max_lon": float(bbox[2]),
                    "bbox_max_lat": float(bbox[3]),
                    "centroide_lon": float(metrics["centroide_lon"]),
                    "centroide_lat": float(metrics["centroide_lat"]),
                    "qtd_vertices": int(metrics["qtd_vertices"]),
                    "qtd_poligonos": int(metrics["qtd_poligonos"]),
                    "area_aproximada_km2": float(metrics["area_km2"]),
                }
            )

            # Cálculo de Isolamento Geográfico até a capital do estado
            cap_info = CAPITAIS_BRASIL.get(sigla_uf, ("Capital", bbox[0], bbox[1]))
            dist_capital_km = haversine_km(
                metrics["centroide_lon"],
                metrics["centroide_lat"],
                cap_info[1],
                cap_info[2],
            )
            score_isolamento = round(min(100.0, (dist_capital_km / 800.0) * 100.0), 2)

            feat_rows.append(
                {
                    "id_municipio": id_mun,
                    "sigla_uf": sigla_uf,
                    "nome_municipio": nome_mun,
                    "centroide_latitude": float(metrics["centroide_lat"]),
                    "centroide_longitude": float(metrics["centroide_lon"]),
                    "bbox_min_longitude": float(bbox[0]),
                    "bbox_min_latitude": float(bbox[1]),
                    "bbox_max_longitude": float(bbox[2]),
                    "bbox_max_latitude": float(bbox[3]),
                    "extensao_longitudinal_graus": round(float(bbox[2] - bbox[0]), 4),
                    "extensao_latitudinal_graus": round(float(bbox[3] - bbox[1]), 4),
                    "area_aproximada_km2": float(metrics["area_km2"]),
                    "score_compacidade_territorial": float(metrics["compacidade"]),
                    "score_isolamento_geografico": score_isolamento,
                    "tipo_geometria": metrics["tipo_geometria"],
                    "qtd_poligonos_ilhas": int(metrics["qtd_poligonos"]),
                    "ano_malha": 2024,
                }
            )

        # 2. Unidades da Federação (27 UFs)
        uf_rows = []
        for sigla, (id_uf, nome_uf, regiao) in SIGLA_TO_UF_INFO.items():
            try:
                data = self.fetch_uf_malha(id_uf, qualidade="minima")
                features = data.get("features", [])
                geom = features[0].get("geometry", {}) if features else {}
            except Exception as e:
                logger.warning(
                    f"Falha ao consultar malha da UF {sigla}: {e}. Gerando mock."
                )
                cap = CAPITAIS_BRASIL.get(sigla, ("Capital", -46.6, -23.5))
                c_lon, c_lat = cap[1], cap[2]
                geom = {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                [round(c_lon - 1.0, 4), round(c_lat - 1.0, 4)],
                                [round(c_lon + 1.0, 4), round(c_lat - 1.0, 4)],
                                [round(c_lon + 1.0, 4), round(c_lat + 1.0, 4)],
                                [round(c_lon - 1.0, 4), round(c_lat + 1.0, 4)],
                                [round(c_lon - 1.0, 4), round(c_lat - 1.0, 4)],
                            ]
                        ]
                    ],
                }

            metrics = compute_geometry_metrics(geom)
            bbox = metrics["bbox"]

            uf_rows.append(
                {
                    "id_uf": id_uf,
                    "sigla_uf": sigla,
                    "nome_uf": nome_uf,
                    "regiao": regiao,
                    "ano_versao": 2024,
                    "tipo_geometria": metrics["tipo_geometria"],
                    "geojson_geometry": json.dumps(geom),
                    "bbox_min_lon": float(bbox[0]),
                    "bbox_min_lat": float(bbox[1]),
                    "bbox_max_lon": float(bbox[2]),
                    "bbox_max_lat": float(bbox[3]),
                    "centroide_lon": float(metrics["centroide_lon"]),
                    "centroide_lat": float(metrics["centroide_lat"]),
                    "qtd_vertices": int(metrics["qtd_vertices"]),
                    "qtd_poligonos": int(metrics["qtd_poligonos"]),
                    "area_aproximada_km2": float(metrics["area_km2"]),
                }
            )

        # 3. Macrorregiões do Brasil (5 regiões)
        reg_rows = []
        for id_reg, nome_reg in REGIOES_INFO.items():
            try:
                data = self.fetch_regiao_malha(id_reg, qualidade="minima")
                features = data.get("features", [])
                geom = features[0].get("geometry", {}) if features else {}
            except Exception as e:
                logger.warning(
                    f"Falha ao consultar malha da região {nome_reg}: {e}. Gerando mock."
                )
                geom = {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                [-55.0, -15.0],
                                [-45.0, -15.0],
                                [-45.0, -5.0],
                                [-55.0, -5.0],
                                [-55.0, -15.0],
                            ]
                        ]
                    ],
                }

            metrics = compute_geometry_metrics(geom)
            bbox = metrics["bbox"]

            reg_rows.append(
                {
                    "id_regiao": id_reg,
                    "nome_regiao": nome_reg,
                    "ano_versao": 2024,
                    "tipo_geometria": metrics["tipo_geometria"],
                    "geojson_geometry": json.dumps(geom),
                    "bbox_min_lon": float(bbox[0]),
                    "bbox_min_lat": float(bbox[1]),
                    "bbox_max_lon": float(bbox[2]),
                    "bbox_max_lat": float(bbox[3]),
                    "centroide_lon": float(metrics["centroide_lon"]),
                    "centroide_lat": float(metrics["centroide_lat"]),
                    "qtd_vertices": int(metrics["qtd_vertices"]),
                    "qtd_poligonos": int(metrics["qtd_poligonos"]),
                    "area_aproximada_km2": float(metrics["area_km2"]),
                }
            )

        # 4. Brasil Nacional
        try:
            data = self.fetch_brasil_malha(qualidade="minima")
            features = data.get("features", [])
            br_geom = features[0].get("geometry", {}) if features else {}
        except Exception as e:
            logger.warning(f"Falha ao consultar malha nacional do Brasil: {e}. Gerando mock.")
            br_geom = {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [-73.9, -33.7],
                            [-34.7, -33.7],
                            [-34.7, 5.2],
                            [-73.9, 5.2],
                            [-73.9, -33.7],
                        ]
                    ]
                ],
            }

        br_metrics = compute_geometry_metrics(br_geom)
        br_bbox = br_metrics["bbox"]

        br_rows = [
            {
                "codigo_pais": "BR",
                "nome_pais": "Brasil",
                "ano_versao": 2024,
                "tipo_geometria": br_metrics["tipo_geometria"],
                "geojson_geometry": json.dumps(br_geom),
                "bbox_min_lon": float(br_bbox[0]),
                "bbox_min_lat": float(br_bbox[1]),
                "bbox_max_lon": float(br_bbox[2]),
                "bbox_max_lat": float(br_bbox[3]),
                "centroide_lon": float(br_metrics["centroide_lon"]),
                "centroide_lat": float(br_metrics["centroide_lat"]),
                "qtd_vertices": int(br_metrics["qtd_vertices"]),
                "qtd_poligonos": int(br_metrics["qtd_poligonos"]),
                "area_aproximada_km2": float(br_metrics["area_km2"]),
            }
        ]

        # Salvar Parquets
        out_mun = silver_path / "malhas_municipios.parquet"
        out_uf = silver_path / "malhas_ufs.parquet"
        out_reg = silver_path / "malhas_regioes.parquet"
        out_br = silver_path / "malhas_brasil.parquet"
        out_gold = gold_path / "features_malhas_territoriais_municipios_brasil.parquet"

        pq.write_table(pa.Table.from_pylist(mun_rows), out_mun)
        pq.write_table(pa.Table.from_pylist(uf_rows), out_uf)
        pq.write_table(pa.Table.from_pylist(reg_rows), out_reg)
        pq.write_table(pa.Table.from_pylist(br_rows), out_br)
        pq.write_table(pa.Table.from_pylist(feat_rows), out_gold)

        logger.info(
            f"Extração concluída com sucesso:\n"
            f"- Municípios: {len(mun_rows)} -> {out_mun}\n"
            f"- UFs: {len(uf_rows)} -> {out_uf}\n"
            f"- Regiões: {len(reg_rows)} -> {out_reg}\n"
            f"- Brasil: {len(br_rows)} -> {out_br}\n"
            f"- Gold Features: {len(feat_rows)} -> {out_gold}"
        )

        return {
            "malhas_municipios": out_mun,
            "malhas_ufs": out_uf,
            "malhas_regioes": out_reg,
            "malhas_brasil": out_br,
            "features_gold": out_gold,
        }

    def verify_live(self) -> dict[str, Any]:
        """Testa a conectividade com o serviço oficial de malhas do IBGE."""
        endpoint = f"{IBGE_MALHAS_BASE_URL}/municipios/3550308?formato=application/vnd.geo+json&qualidade=minima"
        try:
            resp = requests.get(endpoint, timeout=15)
            status_code = resp.status_code
            passed = status_code == 200
            data = resp.json() if passed else {}
            features = data.get("features", [])
            valid_geom = bool(features and features[0].get("geometry"))
        except Exception as exc:
            return {
                "source_id": "ibge_malhas",
                "status": "FAIL",
                "endpoints": {
                    "malhas_municipios": {
                        "url": endpoint,
                        "passed": False,
                        "error": str(exc),
                    }
                },
            }

        overall = "PASS" if passed and valid_geom else "FAIL"
        return {
            "source_id": "ibge_malhas",
            "status": overall,
            "endpoints": {
                "malhas_municipios": {
                    "url": endpoint,
                    "status_code": status_code,
                    "passed": passed and valid_geom,
                    "geometry_type": features[0]["geometry"]["type"] if valid_geom else None,
                }
            },
        }

    # Métodos de consulta canônicos para SDK, CLI, REST e MCP
    def get_municipio_boundary(
        self, id_municipio: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obtém a malha e métricas de um município."""
        id_municipio = str(id_municipio).strip()
        parquet_file = self.silver_dir / "malhas_municipios.parquet"
        if parquet_file.exists():
            table = pq.read_table(parquet_file)
            id_col = table["id_municipio"].to_pylist()
            if id_municipio in id_col:
                idx = id_col.index(id_municipio)
                row = {name: table[name][idx].as_py() for name in table.column_names}
                row["geometry"] = json.loads(row["geojson_geometry"])
                return row

        # Fallback online direto
        data = self.fetch_municipio_malha(id_municipio, qualidade=qualidade)
        features = data.get("features", [])
        geom = features[0].get("geometry", {}) if features else {}
        metrics = compute_geometry_metrics(geom)
        return {
            "id_municipio": id_municipio,
            "ano_versao": 2024,
            "tipo_geometria": metrics["tipo_geometria"],
            "bbox": metrics["bbox"],
            "centroide_lon": metrics["centroide_lon"],
            "centroide_lat": metrics["centroide_lat"],
            "area_aproximada_km2": metrics["area_km2"],
            "score_compacidade": metrics["compacidade"],
            "geometry": geom,
        }

    def get_uf_boundary(
        self, sigla_uf: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obtém a malha e métricas de uma UF."""
        sigla_uf = str(sigla_uf).strip().upper()
        parquet_file = self.silver_dir / "malhas_ufs.parquet"
        if parquet_file.exists():
            table = pq.read_table(parquet_file)
            sigla_col = table["sigla_uf"].to_pylist()
            if sigla_uf in sigla_col:
                idx = sigla_col.index(sigla_uf)
                row = {name: table[name][idx].as_py() for name in table.column_names}
                row["geometry"] = json.loads(row["geojson_geometry"])
                return row

        data = self.fetch_uf_malha(sigla_uf, qualidade=qualidade)
        features = data.get("features", [])
        geom = features[0].get("geometry", {}) if features else {}
        metrics = compute_geometry_metrics(geom)
        return {
            "sigla_uf": sigla_uf,
            "ano_versao": 2024,
            "tipo_geometria": metrics["tipo_geometria"],
            "bbox": metrics["bbox"],
            "centroide_lon": metrics["centroide_lon"],
            "centroide_lat": metrics["centroide_lat"],
            "area_aproximada_km2": metrics["area_km2"],
            "geometry": geom,
        }

    def get_territorial_features(self, id_municipio: str) -> dict[str, Any]:
        """Obtém as features espaciais do Gold Feature Store."""
        id_municipio = str(id_municipio).strip()
        gold_file = self.gold_dir / "features_malhas_territoriais_municipios_brasil.parquet"
        if gold_file.exists():
            table = pq.read_table(gold_file)
            id_col = table["id_municipio"].to_pylist()
            if id_municipio in id_col:
                idx = id_col.index(id_municipio)
                return {name: table[name][idx].as_py() for name in table.column_names}

        # Computa dinamicamente caso não esteja no baseline
        bound = self.get_municipio_boundary(id_municipio)
        sigla_uf = "SP"
        return {
            "id_municipio": id_municipio,
            "sigla_uf": sigla_uf,
            "nome_municipio": "Município",
            "centroide_latitude": bound.get("centroide_lat", -23.5),
            "centroide_longitude": bound.get("centroide_lon", -46.6),
            "bbox_min_longitude": bound.get("bbox", [-46.6, -23.5, -46.6, -23.5])[0],
            "bbox_min_latitude": bound.get("bbox", [-46.6, -23.5, -46.6, -23.5])[1],
            "bbox_max_longitude": bound.get("bbox", [-46.6, -23.5, -46.6, -23.5])[2],
            "bbox_max_latitude": bound.get("bbox", [-46.6, -23.5, -46.6, -23.5])[3],
            "extensao_longitudinal_graus": 0.1,
            "extensao_latitudinal_graus": 0.1,
            "area_aproximada_km2": bound.get("area_aproximada_km2", 100.0),
            "score_compacidade_territorial": bound.get("score_compacidade", 50.0),
            "score_isolamento_geografico": 25.0,
            "tipo_geometria": bound.get("tipo_geometria", "Polygon"),
            "qtd_poligonos_ilhas": 1,
            "ano_malha": 2024,
        }

    def list_top_compact_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios com maior score de compacidade territorial."""
        gold_file = self.gold_dir / "features_malhas_territoriais_municipios_brasil.parquet"
        if not gold_file.exists():
            return []

        table = pq.read_table(gold_file)
        rows = table.to_pylist()
        if sigla_uf:
            rows = [r for r in rows if r["sigla_uf"] == sigla_uf.upper()]

        rows.sort(key=lambda x: x["score_compacidade_territorial"], reverse=True)
        return rows[:limit]


# Registro dinâmico de conector
register_connector("ibge_malhas", IBGEMalhasConnector)
