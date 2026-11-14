"""Block 4 — Dados Globais, Demografia, Pesquisa & Sentimento."""

from __future__ import annotations

from typing import Any

from public_data_gateway.connectors.public_apis_hub._base import PublicApiHubBase


class GlobalResearchClient(PublicApiHubBase):
    """Clients for REST Countries, OpenAlex scientific graph, and GDELT Project."""

    def lookup_country(self, code: str = "BRA") -> dict[str, Any]:
        """Normalize country ISO-3166 metadata via REST Countries v3.1."""
        clean = code.strip().upper()
        if not clean or len(clean) not in (2, 3):
            raise ValueError(f"Invalid country ISO code '{code}'. Expected 2 or 3 letters.")
        rows = self._get_json(f"https://restcountries.com/v3.1/alpha/{clean}")
        item = rows[0] if isinstance(rows, list) and rows else rows
        return {
            "cca2": item.get("cca2"),
            "cca3": item.get("cca3"),
            "name": (item.get("name") or {}).get("common"),
            "region": item.get("region"),
            "subregion": item.get("subregion"),
            "population": item.get("population"),
            "currencies": list((item.get("currencies") or {}).keys()),
            "borders": item.get("borders") or [],
        }

    def search_openalex_works(self, query: str, per_page: int = 10) -> list[dict[str, Any]]:
        """Search open scientific works & citations via OpenAlex API."""
        clean = query.strip()
        if not clean:
            raise ValueError("OpenAlex search query cannot be empty.")
        data = self._get_json(
            "https://api.openalex.org/works",
            params={"search": clean, "per-page": max(1, min(per_page, 50))},
        )
        return [
            {
                "id": w.get("id"),
                "title": w.get("title"),
                "publication_year": w.get("publication_year"),
                "cited_by_count": w.get("cited_by_count"),
                "doi": w.get("doi"),
            }
            for w in data.get("results") or []
        ]

    def query_gdelt_doc(self, query: str, maxrecords: int = 10) -> list[dict[str, Any]]:
        """Query global news articles and sentiment streams via GDELT Project Doc 2.0 API."""
        clean = query.strip()
        if not clean:
            raise ValueError("GDELT query cannot be empty.")
        data = self._get_json(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query": clean,
                "mode": "ArtList",
                "format": "json",
                "maxrecords": max(1, min(maxrecords, 50)),
            },
        )
        return data.get("articles") or []
