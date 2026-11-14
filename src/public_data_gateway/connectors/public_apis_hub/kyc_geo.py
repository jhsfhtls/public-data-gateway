"""Block 1 — Enriquecimento Cadastral, Geolocalizacao, KYC/KYB & Antifraude."""

from __future__ import annotations

from typing import Any

from public_data_gateway.connectors.public_apis_hub._base import PublicApiHubBase


class KycGeoClient(PublicApiHubBase):
    """Cascade KYC/KYB, CEP, Geocoding, RDAP, GLEIF LEI, and IP/ASN enrichment."""

    def enrich_cnpj_cascade(self, cnpj: str) -> dict[str, Any]:
        """Enrich CNPJ using cascade fallback: Minha Receita -> CNPJa -> BrasilAPI -> ReceitaWS."""
        clean = self._digits_only(cnpj)
        if len(clean) != 14:
            raise ValueError(f"Invalid CNPJ '{cnpj}'. Expected 14 digits.")

        def _from_minha_receita() -> dict[str, Any]:
            data = self._get_json(f"https://minhareceita.org/api/v1/cnpj/{clean}")
            return {
                "cnpj": clean,
                "razao_social": data.get("razao_social"),
                "situacao_cadastral": data.get("descricao_situacao_cadastral"),
                "cnae_principal": str(data.get("cnae_fiscal") or ""),
                "uf": data.get("uf"),
                "municipio": data.get("municipio"),
                "qsa_count": len(data.get("qsa") or []),
                "raw": data,
            }

        def _from_cnpja() -> dict[str, Any]:
            data = self._get_json(f"https://open.cnpja.com/office/{clean}")
            company = data.get("company") or {}
            status = data.get("status") or {}
            address = data.get("address") or {}
            main_act = data.get("mainActivity") or {}
            return {
                "cnpj": clean,
                "razao_social": company.get("name"),
                "situacao_cadastral": status.get("text"),
                "cnae_principal": str(main_act.get("id") or ""),
                "uf": address.get("state"),
                "municipio": address.get("city"),
                "qsa_count": len(company.get("members") or []),
                "raw": data,
            }

        def _from_brasilapi() -> dict[str, Any]:
            data = self._get_json(f"https://brasilapi.com.br/api/cnpj/v1/{clean}")
            return {
                "cnpj": clean,
                "razao_social": data.get("razao_social"),
                "situacao_cadastral": data.get("descricao_situacao_cadastral"),
                "cnae_principal": str(data.get("cnae_fiscal") or ""),
                "uf": data.get("uf"),
                "municipio": data.get("municipio"),
                "qsa_count": len(data.get("qsa") or []),
                "raw": data,
            }

        def _from_receitaws() -> dict[str, Any]:
            data = self._get_json(f"https://receitaws.com.br/v1/cnpj/{clean}")
            if data.get("status") == "ERROR":
                raise ValueError(data.get("message", "ReceitaWS error"))
            act = (data.get("atividade_principal") or [{}])[0]
            return {
                "cnpj": clean,
                "razao_social": data.get("nome"),
                "situacao_cadastral": data.get("situacao"),
                "cnae_principal": str(act.get("code") or ""),
                "uf": data.get("uf"),
                "municipio": data.get("municipio"),
                "qsa_count": len(data.get("qsa") or []),
                "raw": data,
            }

        provider, payload = self._first_success(
            [
                ("minha_receita", _from_minha_receita),
                ("cnpja_open", _from_cnpja),
                ("brasilapi", _from_brasilapi),
                ("receitaws", _from_receitaws),
            ],
            f"CNPJ({clean})",
        )
        payload["provider_used"] = provider
        return payload

    def lookup_cep_cascade(self, cep: str) -> dict[str, Any]:
        """Lookup CEP using cascade fallback: ViaCEP -> OpenCEP -> BrasilAPI."""
        clean = self._digits_only(cep)
        if len(clean) != 8:
            raise ValueError(f"Invalid CEP '{cep}'. Expected 8 digits.")

        def _from_viacep() -> dict[str, Any]:
            data = self._get_json(f"https://viacep.com.br/ws/{clean}/json/")
            if data.get("erro"):
                raise ValueError("CEP not found in ViaCEP")
            return {
                "cep": clean,
                "logradouro": data.get("logradouro"),
                "bairro": data.get("bairro"),
                "localidade": data.get("localidade"),
                "uf": data.get("uf"),
                "ibge": data.get("ibge"),
                "ddd": data.get("ddd"),
            }

        def _from_opencep() -> dict[str, Any]:
            data = self._get_json(f"https://opencep.com/v1/{clean}")
            return {
                "cep": clean,
                "logradouro": data.get("logradouro"),
                "bairro": data.get("bairro"),
                "localidade": data.get("localidade"),
                "uf": data.get("uf"),
                "ibge": data.get("ibge"),
                "ddd": None,
            }

        def _from_brasilapi() -> dict[str, Any]:
            data = self._get_json(f"https://brasilapi.com.br/api/cep/v2/{clean}")
            return {
                "cep": clean,
                "logradouro": data.get("street"),
                "bairro": data.get("neighborhood"),
                "localidade": data.get("city"),
                "uf": data.get("state"),
                "ibge": None,
                "ddd": None,
            }

        provider, payload = self._first_success(
            [("viacep", _from_viacep), ("opencep", _from_opencep), ("brasilapi", _from_brasilapi)],
            f"CEP({clean})",
        )
        payload["provider_used"] = provider
        return payload

    def geocode_nominatim(self, query: str, limit: int = 3) -> list[dict[str, Any]]:
        """Forward geocode an address using OpenStreetMap Nominatim."""
        clean = query.strip()
        if not clean:
            raise ValueError("Geocode query cannot be empty.")
        rows = self._get_json(
            "https://nominatim.openstreetmap.org/search",
            params={"q": clean, "format": "jsonv2", "limit": max(1, min(limit, 10))},
        )
        return [
            {
                "display_name": r.get("display_name"),
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "type": r.get("type"),
            }
            for r in rows
        ]

    def lookup_rdap_registro_br(self, domain: str) -> dict[str, Any]:
        """Lookup .br domain registration & age metadata via RDAP Registro.br."""
        clean = domain.strip().lower()
        if not clean or "." not in clean:
            raise ValueError(f"Invalid domain '{domain}'.")
        data = self._get_json(f"https://rdap.registro.br/domain/{clean}")
        events = {e.get("eventAction"): e.get("eventDate") for e in data.get("events") or []}
        nameservers = [ns.get("ldhName") for ns in data.get("nameservers") or []]
        return {
            "domain": data.get("ldhName", clean),
            "status": data.get("status", []),
            "registered_at": events.get("registration"),
            "last_changed_at": events.get("last changed"),
            "nameservers": nameservers,
        }

    def lookup_gleif_lei(self, query: str) -> list[dict[str, Any]]:
        """Search global Legal Entity Identifiers (LEI) in GLEIF API."""
        clean = query.strip()
        if not clean:
            raise ValueError("GLEIF query cannot be empty.")
        data = self._get_json(
            "https://api.gleif.org/api/v1/lei-records",
            params={"filter[entity.legalName]": clean, "page[size]": 5},
        )
        results: list[dict[str, Any]] = []
        for item in data.get("data") or []:
            attrs = item.get("attributes") or {}
            entity = attrs.get("entity") or {}
            results.append(
                {
                    "lei": item.get("id"),
                    "legal_name": (entity.get("legalName") or {}).get("name"),
                    "country": (entity.get("legalAddress") or {}).get("country"),
                    "status": entity.get("status"),
                }
            )
        return results

    def lookup_ip_intel(self, ip: str) -> dict[str, Any]:
        """Lookup IP ASN, prefix, and geolocation via BGPView."""
        clean = ip.strip()
        if not clean:
            raise ValueError("IP address cannot be empty.")
        payload = self._get_json(f"https://api.bgpview.io/ip/{clean}")
        data = payload.get("data") or {}
        prefixes = data.get("prefixes") or []
        first_asn = (prefixes[0].get("asn") or {}) if prefixes else {}
        return {
            "ip": data.get("ip", clean),
            "ptr_record": data.get("ptr_record"),
            "asn": first_asn.get("asn"),
            "asn_name": first_asn.get("name"),
            "country_code": first_asn.get("country_code"),
        }
