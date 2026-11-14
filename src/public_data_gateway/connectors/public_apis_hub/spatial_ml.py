"""Block 3 — Feature Engineering Espacial, Mobilidade, Clima & ML."""

from __future__ import annotations

import os
from typing import Any

from public_data_gateway.connectors.public_apis_hub._base import PublicApiHubBase


class SpatialMlClient(PublicApiHubBase):
    """Spatial POIs (Overpass), routing (OSRM), weather/climate, elevation & ML inference."""

    def count_osm_pois_radius(
        self,
        lat: float,
        lon: float,
        *,
        amenity: str = "school",
        radius_m: int = 1000,
    ) -> dict[str, Any]:
        """Count OpenStreetMap POIs within radius_m around (lat, lon) via Overpass API."""
        safe_radius = max(50, min(int(radius_m), 10000))
        clean_amenity = amenity.strip().lower()
        if not clean_amenity:
            raise ValueError("Amenity tag cannot be empty.")
        ql = (
            f'[out:json][timeout:25];'
            f'(node["amenity"="{clean_amenity}"](around:{safe_radius},{lat},{lon});'
            f'way["amenity"="{clean_amenity}"](around:{safe_radius},{lat},{lon}););'
            f'out count;'
        )
        data = self._post_json("https://overpass-api.de/api/interpreter", data={"data": ql})
        elements = data.get("elements") or []
        tags = (elements[0].get("tags") or {}) if elements else {}
        total = int(tags.get("total", len(elements)))
        return {
            "lat": lat,
            "lon": lon,
            "amenity": clean_amenity,
            "radius_m": safe_radius,
            "total_pois": total,
            "nodes": int(tags.get("nodes", 0)),
            "ways": int(tags.get("ways", 0)),
        }

    def compute_osrm_route(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        *,
        profile: str = "driving",
    ) -> dict[str, Any]:
        """Compute real road distance (km) and duration (min) via OSRM."""
        url = f"https://router.project-osrm.org/route/v1/{profile}/{lon1},{lat1};{lon2},{lat2}"
        data = self._get_json(url, params={"overview": "false"})
        routes = data.get("routes") or []
        if not routes:
            raise ValueError("No route returned by OSRM.")
        best = routes[0]
        return {
            "profile": profile,
            "distance_km": round(float(best["distance"]) / 1000.0, 3),
            "duration_min": round(float(best["duration"]) / 60.0, 2),
        }

    def fetch_open_meteo_forecast(
        self,
        lat: float,
        lon: float,
        *,
        daily: str = "temperature_2m_max,temperature_2m_min,precipitation_sum",
    ) -> dict[str, Any]:
        """Fetch hourly/daily weather variables from Open-Meteo for exogenous ML features."""
        return self._get_json(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "daily": daily, "timezone": "America/Sao_Paulo"},
        )

    def fetch_nasa_power_daily(
        self,
        lat: float,
        lon: float,
        *,
        start: str,
        end: str,
        parameters: str = "ALLSKY_SFC_SW_DWN,PRECTOTCORR,T2M",
    ) -> dict[str, Any]:
        """Fetch solar radiation, precipitation, and temperature from NASA POWER API."""
        return self._get_json(
            "https://power.larc.nasa.gov/api/temporal/daily/point",
            params={
                "parameters": parameters,
                "community": "AG",
                "longitude": lon,
                "latitude": lat,
                "start": start,
                "end": end,
                "format": "JSON",
            },
        )

    def fetch_elevation(self, lat: float, lon: float) -> dict[str, Any]:
        """Fetch terrain elevation (meters) from Open-Elevation SRTM."""
        data = self._get_json(
            "https://api.open-elevation.com/api/v1/lookup",
            params={"locations": f"{lat},{lon}"},
        )
        results = data.get("results") or []
        if not results:
            raise ValueError("No elevation result returned.")
        return {
            "lat": lat,
            "lon": lon,
            "elevation_m": float(results[0]["elevation"]),
        }

    def run_hf_inference(self, model: str, inputs: str) -> Any:
        """Run Hugging Face HTTP inference (requires HF_TOKEN env var)."""
        token = os.environ.get("HF_TOKEN")
        if not token:
            raise ValueError("HF_TOKEN environment variable is required for Hugging Face Inference.")
        return self._post_json(
            f"https://router.huggingface.co/hf-inference/models/{model.strip()}",
            json_body={"inputs": inputs},
            headers={"Authorization": f"Bearer {token}"},
        )

    def fetch_alpha_vantage_daily(self, symbol: str) -> dict[str, Any]:
        """Fetch Alpha Vantage daily time series (requires ALPHA_VANTAGE_API_KEY env var)."""
        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required.")
        return self._get_json(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY", "symbol": symbol.strip().upper(), "apikey": api_key},
        )

    def fetch_nasa_apod(self) -> dict[str, Any]:
        """Fetch NASA Astronomy Picture of the Day (uses NASA_API_KEY or DEMO_KEY)."""
        api_key = os.environ.get("NASA_API_KEY", "DEMO_KEY")
        return self._get_json("https://api.nasa.gov/planetary/apod", params={"api_key": api_key})

    @staticmethod
    def describe_upgini_features() -> dict[str, Any]:
        """Describe optional Upgini public feature enrichment compatibility."""
        return {
            "package": "upgini",
            "install_command": "pip install upgini",
            "supported_search_keys": ["date", "postal_code", "country", "ip"],
            "gateway_synergy": "Combine local Brazil Gold Feature Store with Upgini external ML features.",
        }
