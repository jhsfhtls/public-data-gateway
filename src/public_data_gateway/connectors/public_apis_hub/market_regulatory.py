"""Block 2 — Inteligencia de Mercado, Regulatorias, Economia & Governo."""

from __future__ import annotations

import os
from typing import Any

from public_data_gateway.connectors.public_apis_hub._base import PublicApiHubBase


class MarketRegulatoryClient(PublicApiHubBase):
    """Clients for B3 (brapi), AwesomeAPI, IPEA, ANEEL/ANP CKAN, Camara, World Bank & WHO."""

    def fetch_brapi_quote(self, tickers: str) -> list[dict[str, Any]]:
        """Fetch B3 stock/FII quotes from brapi.dev (uses BRAPI_TOKEN if set)."""
        clean = tickers.strip().upper()
        if not clean:
            raise ValueError("Tickers cannot be empty.")
        params: dict[str, Any] = {}
        token = os.environ.get("BRAPI_TOKEN")
        if token:
            params["token"] = token
        data = self._get_json(f"https://brapi.dev/api/quote/{clean}", params=params, cache_ttl=300)
        return [
            {
                "symbol": item.get("symbol"),
                "short_name": item.get("shortName"),
                "price": item.get("regularMarketPrice"),
                "change_pct": item.get("regularMarketChangePercent"),
                "market_cap": item.get("marketCap"),
            }
            for item in data.get("results") or []
        ]

    def fetch_awesomeapi_rates(self, pairs: str = "USD-BRL,EUR-BRL,BTC-BRL") -> dict[str, Any]:
        """Fetch real-time FX & crypto quotes from AwesomeAPI."""
        clean = pairs.strip().upper()
        if not clean:
            raise ValueError("Currency pairs cannot be empty.")
        return self._get_json(
            f"https://economia.awesomeapi.com.br/json/last/{clean}",
            cache_ttl=120,
        )

    def fetch_ipeadata_series(self, sercodigo: str, limit: int = 24) -> list[dict[str, Any]]:
        """Fetch historical macroeconomic/regional series from IPEA Data OData v4."""
        clean = sercodigo.strip()
        if not clean:
            raise ValueError("IPEA series code (sercodigo) cannot be empty.")
        url = f"https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{clean}')"
        data = self._get_json(url, params={"$top": max(1, min(limit, 500))})
        return data.get("value") or []

    def query_ckan_datastore(
        self,
        resource_id: str,
        *,
        portal: str = "aneel",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Query CKAN DataStore SQL/JSON endpoints for ANEEL or ANP open data."""
        base_urls = {
            "aneel": "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search",
            "anp": "https://dados.gov.br/api/3/action/datastore_search",
        }
        key = portal.strip().lower()
        if key not in base_urls:
            raise ValueError(f"Unsupported CKAN portal '{portal}'. Expected one of {sorted(base_urls)}.")
        data = self._get_json(
            base_urls[key],
            params={"resource_id": resource_id.strip(), "limit": max(1, min(limit, 100))},
        )
        return (data.get("result") or {}).get("records") or []

    def fetch_camara_deputados(self, pagina: int = 1, itens: int = 20) -> list[dict[str, Any]]:
        """Fetch active federal deputies from Camara dos Deputados Open Data API."""
        data = self._get_json(
            "https://dadosabertos.camara.leg.br/api/v2/deputados",
            params={"pagina": max(1, pagina), "itens": max(1, min(itens, 100))},
        )
        return data.get("dados") or []

    def fetch_world_bank_indicator(
        self,
        country: str = "BRA",
        indicator: str = "NY.GDP.MKTP.KD.ZG",
        per_page: int = 10,
    ) -> list[dict[str, Any]]:
        """Fetch World Bank v2 indicator series for a country (ISO-3)."""
        data = self._get_json(
            f"https://api.worldbank.org/v2/country/{country.strip().upper()}/indicator/{indicator.strip()}",
            params={"format": "json", "per_page": max(1, min(per_page, 100))},
        )
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
            return [
                {
                    "country": (row.get("country") or {}).get("id"),
                    "indicator": (row.get("indicator") or {}).get("id"),
                    "year": row.get("date"),
                    "value": row.get("value"),
                }
                for row in data[1]
            ]
        return []

    def fetch_who_gho_indicator(self, indicator: str = "WHOSIS_000001", top: int = 10) -> list[dict[str, Any]]:
        """Fetch global health observatory indicator records from WHO GHO OData API."""
        data = self._get_json(
            f"https://ghoapi.azureedge.net/api/{indicator.strip()}",
            params={"$top": max(1, min(top, 100))},
        )
        return data.get("value") or []
