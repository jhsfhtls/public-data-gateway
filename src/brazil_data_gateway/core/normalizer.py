"""Deterministic normalizers and Mod-11 validators for Brazilian canonical identifiers."""

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

from brazil_data_gateway.core.config import GatewaySettings

# Mapping of IBGE UF code prefix (first 2 digits) to state metadata
_UF_PREFIX_MAP: dict[str, tuple[str, str, str]] = {
    "11": ("RO", "Rondônia", "Norte"),
    "12": ("AC", "Acre", "Norte"),
    "13": ("AM", "Amazonas", "Norte"),
    "14": ("RR", "Roraima", "Norte"),
    "15": ("PA", "Pará", "Norte"),
    "16": ("AP", "Amapá", "Norte"),
    "17": ("TO", "Tocantins", "Norte"),
    "21": ("MA", "Maranhão", "Nordeste"),
    "22": ("PI", "Piauí", "Nordeste"),
    "23": ("CE", "Ceará", "Nordeste"),
    "24": ("RN", "Rio Grande do Norte", "Nordeste"),
    "25": ("PB", "Paraíba", "Nordeste"),
    "26": ("PE", "Pernambuco", "Nordeste"),
    "27": ("AL", "Alagoas", "Nordeste"),
    "28": ("SE", "Sergipe", "Nordeste"),
    "29": ("BA", "Bahia", "Nordeste"),
    "31": ("MG", "Minas Gerais", "Sudeste"),
    "32": ("ES", "Espírito Santo", "Sudeste"),
    "33": ("RJ", "Rio de Janeiro", "Sudeste"),
    "35": ("SP", "São Paulo", "Sudeste"),
    "41": ("PR", "Paraná", "Sul"),
    "42": ("SC", "Santa Catarina", "Sul"),
    "43": ("RS", "Rio Grande do Sul", "Sul"),
    "50": ("MS", "Mato Grosso do Sul", "Centro-Oeste"),
    "51": ("MT", "Mato Grosso", "Centro-Oeste"),
    "52": ("GO", "Goiás", "Centro-Oeste"),
    "53": ("DF", "Distrito Federal", "Centro-Oeste"),
}

_SENTINEL_STRINGS = {
    "",
    "-",
    "--",
    "null",
    "none",
    "sem informacao",
    "sem informação",
    "na",
    "n/a",
    "nao informado",
    "não informado",
    "ignorado",
    "99",
    "999",
    "9999",
    "-1",
}


def _strip_accents(text: str) -> str:
    """Normalize accented characters to ASCII equivalents for robust matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


class CheckDigitValidator:
    """Deterministic Mod-11 and IBGE check-digit validators."""

    @staticmethod
    def is_valid_cnpj(val: str | int | None) -> bool:
        """Validate a 14-digit CNPJ using official Mod-11 check digit rules."""
        if val is None:
            return False
        digits = re.sub(r"\D", "", str(val))
        if len(digits) != 14:
            return False
        # Reject known invalid uniform sequences
        if len(set(digits)) == 1:
            return False

        # First check digit
        weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sm1 = sum(int(d) * w for d, w in zip(digits[:12], weights_1, strict=True))
        rem1 = sm1 % 11
        cd1 = 0 if rem1 < 2 else 11 - rem1
        if int(digits[12]) != cd1:
            return False

        # Second check digit
        weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sm2 = sum(int(d) * w for d, w in zip(digits[:13], weights_2, strict=True))
        rem2 = sm2 % 11
        cd2 = 0 if rem2 < 2 else 11 - rem2
        return int(digits[13]) == cd2

    @staticmethod
    def is_valid_cpf(val: str | int | None) -> bool:
        """Validate an 11-digit CPF using official Mod-11 check digit rules."""
        if val is None:
            return False
        digits = re.sub(r"\D", "", str(val))
        if len(digits) != 11 or len(set(digits)) == 1:
            return False

        # First check digit
        sm1 = sum(int(digits[i]) * (10 - i) for i in range(9))
        rem1 = sm1 % 11
        cd1 = 0 if rem1 < 2 else 11 - rem1
        if int(digits[9]) != cd1:
            return False

        # Second check digit
        sm2 = sum(int(digits[i]) * (11 - i) for i in range(10))
        rem2 = sm2 % 11
        cd2 = 0 if rem2 < 2 else 11 - rem2
        return int(digits[10]) == cd2

    @staticmethod
    def is_valid_ibge_7(val: str | int | None) -> bool:
        """Validate 7-digit IBGE municipality code using Modulo 10 (Luhn weights [1, 2, 1, 2, 1, 2])."""
        if val is None:
            return False
        digits = re.sub(r"\D", "", str(val))
        if len(digits) != 7:
            return False

        # Standard IBGE Luhn check-digit calculation
        weights = [1, 2, 1, 2, 1, 2]
        sm = 0
        for d, w in zip(digits[:6], weights, strict=True):
            p = int(d) * w
            sm += p if p < 10 else (p // 10 + p % 10)
        rem = sm % 10
        expected = 0 if rem == 0 else 10 - rem
        if int(digits[6]) == expected:
            return True

        # Historical exception list from IBGE catalog anomalies
        known_exceptions = {
            "5203939", "5203962", "3117836", "3152131",
            "2611533", "2201919", "2201988", "2202251", "4305871"
        }
        return digits in known_exceptions


class CanonicalNormalizer:
    """Canonical data normalizer for Brazilian geographic, tax, and economic codes."""

    UF_MAP: dict[str, dict[str, str]] = {
        code: {"sigla": sigla, "nome": nome, "regiao": regiao}
        for code, (sigla, nome, regiao) in _UF_PREFIX_MAP.items()
    }

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self._loaded = False
        self._by_id_7: dict[str, dict[str, Any]] = {}
        self._by_id_6: dict[str, dict[str, Any]] = {}
        self._by_name_uf: dict[tuple[str, str], dict[str, Any]] = {}
        self._by_name: dict[str, list[dict[str, Any]]] = {}

    def _ensure_loaded(self) -> None:
        """Lazily load and index official municipality dataset."""
        if self._loaded:
            return

        candidates = [
            self.settings.reference_dir / "municipios_brasil.json",
            self.settings.repo_root / "data" / "reference" / "municipios_brasil.json",
            Path(__file__).resolve().parents[3] / "data" / "reference" / "municipios_brasil.json",
            self.settings.repo_root.parent / "core" / "data" / "referencia" / "municipios_brasil.json",
        ]

        loaded_data: list[dict[str, Any]] = []
        for path in candidates:
            if path.is_file():
                try:
                    with open(path, encoding="utf-8") as f:
                        loaded_data = json.load(f)
                    break
                except (json.JSONDecodeError, OSError):
                    continue

        for item in loaded_data:
            id_7 = str(item.get("id_municipio", "")).strip()
            if not id_7:
                continue

            id_6 = str(item.get("id_municipio_6", "")).strip() or id_7[:6]
            nome = str(item.get("nome", "")).strip()
            sigla_uf = str(item.get("sigla_uf", "")).strip().upper()
            id_uf = str(item.get("id_uf", "")).strip()

            # Enrich UF if missing based on IBGE prefix
            if not sigla_uf and len(id_7) >= 2:
                prefix = id_7[:2]
                if prefix in _UF_PREFIX_MAP:
                    sigla_uf, nome_uf, regiao = _UF_PREFIX_MAP[prefix]
                    item["sigla_uf"] = sigla_uf
                    item.setdefault("nome_uf", nome_uf)
                    item.setdefault("regiao", regiao)
                    item.setdefault("id_uf", prefix)

            record = {
                "id_municipio": id_7,
                "id_municipio_6": id_6,
                "nome": nome,
                "nome_municipio": nome,
                "nome_municipio_oficial": nome,
                "sigla_uf": sigla_uf,
                "id_uf": id_uf or id_7[:2],
                "nome_uf": item.get("nome_uf", ""),
                "regiao": item.get("regiao", ""),
            }

            self._by_id_7[id_7] = record
            self._by_id_6[id_6] = record

            norm_name = _strip_accents(nome)
            if sigla_uf:
                self._by_name_uf[(norm_name, sigla_uf)] = record
            self._by_name.setdefault(norm_name, []).append(record)

        self._loaded = True

    @property
    def municipios(self) -> list[dict[str, Any]]:
        """Return all loaded municipality records."""
        self._ensure_loaded()
        return list(self._by_id_7.values())

    def get_municipio(
        self, code_or_name: str | int | None, uf: str | None = None
    ) -> dict[str, Any] | None:
        """Alias for get_municipio_record."""
        return self.get_municipio_record(code_or_name, uf=uf)

    def get_municipio_record(
        self, code_or_name: str | int | None, uf: str | None = None
    ) -> dict[str, Any] | None:
        """Resolve full municipality metadata by 7-digit ID, 6-digit legacy ID, or fuzzy name + UF."""
        if code_or_name is None:
            return None
        self._ensure_loaded()

        raw_str = str(code_or_name).strip()
        digits = re.sub(r"\D", "", raw_str)

        # 7-digit code lookup
        if len(digits) == 7:
            if digits in self._by_id_7:
                return self._by_id_7[digits]
            # Fallback if not in index but has valid prefix
            prefix = digits[:2]
            if prefix in _UF_PREFIX_MAP:
                sigla_uf, nome_uf, regiao = _UF_PREFIX_MAP[prefix]
                return {
                    "id_municipio": digits,
                    "id_municipio_6": digits[:6],
                    "nome_municipio_oficial": raw_str,
                    "sigla_uf": sigla_uf,
                    "id_uf": prefix,
                    "nome_uf": nome_uf,
                    "regiao": regiao,
                }

        # 6-digit legacy code lookup
        if len(digits) == 6:
            if digits in self._by_id_6:
                return self._by_id_6[digits]

        # Name-based lookup
        norm_name = _strip_accents(raw_str)
        norm_uf = uf.strip().upper() if uf else None

        if norm_uf and (norm_name, norm_uf) in self._by_name_uf:
            return self._by_name_uf[(norm_name, norm_uf)]

        matches = self._by_name.get(norm_name, [])
        if norm_uf:
            for m in matches:
                if m.get("sigla_uf") == norm_uf:
                    return m

        if len(matches) == 1:
            return matches[0]

        return None

    def to_ibge_7(self, code_or_name: str | int | None, uf: str | None = None) -> str | None:
        """Resolve any municipality identifier into its official 7-digit IBGE code."""
        record = self.get_municipio_record(code_or_name, uf=uf)
        if record:
            return record["id_municipio"]

        # If digits only and len 6, compute 7th digit
        if code_or_name is not None:
            digits = re.sub(r"\D", "", str(code_or_name))
            if len(digits) == 6:
                weights = [1, 2, 1, 2, 1, 2]
                sm = 0
                for d, w in zip(digits, weights, strict=True):
                    p = int(d) * w
                    sm += p if p < 10 else (p // 10 + p % 10)
                rem = sm % 10
                cd = 0 if rem == 0 else 10 - rem
                return f"{digits}{cd}"
            if len(digits) == 7:
                return digits

        return None

    @staticmethod
    def to_cnpj_14(val: str | int | None) -> str | None:
        """Zero-pad and format CNPJ to strict 14 numeric characters."""
        if val is None:
            return None
        digits = re.sub(r"\D", "", str(val))
        if not digits or len(digits) > 14:
            return None
        return digits.zfill(14)

    @staticmethod
    def to_cep_8(val: str | int | None) -> str | None:
        """Zero-pad and format CEP to strict 8 numeric characters."""
        if val is None:
            return None
        digits = re.sub(r"\D", "", str(val))
        if not digits or len(digits) > 8:
            return None
        return digits.zfill(8)

    @staticmethod
    def to_cnae_7(val: str | int | None) -> str | None:
        """Zero-pad and format CNAE code to strict 7 numeric characters."""
        if val is None:
            return None
        digits = re.sub(r"\D", "", str(val))
        if not digits or len(digits) > 7:
            return None
        return digits.zfill(7)

    @staticmethod
    def to_cbo_6(val: str | int | None) -> str | None:
        """Zero-pad and format CBO code to strict 6 numeric characters."""
        if val is None:
            return None
        digits = re.sub(r"\D", "", str(val))
        if not digits or len(digits) > 6:
            return None
        return digits.zfill(6)

    @staticmethod
    def to_ncm_8(val: str | int | None) -> str | None:
        """Zero-pad and format Mercosur NCM code to strict 8 numeric characters."""
        if val is None:
            return None
        digits = re.sub(r"\D", "", str(val))
        if not digits or len(digits) > 8:
            return None
        return digits.zfill(8)

    @staticmethod
    def preserve_cpf_mask(val: str) -> str:
        """Strictly mask CPF to '***.123.456-**' format. Never expose unmasked digits."""
        if not val:
            return ""

        # Match existing masked format like ***.123.456-**
        masked_match = re.search(r"\*{3}\.?(\d{3})\.?(\d{3})-?\*{2}", val)
        if masked_match:
            d1, d2 = masked_match.group(1), masked_match.group(2)
            return f"***.{d1}.{d2}-**"

        # Match raw 11-digit CPF and mask immediately
        digits = re.sub(r"\D", "", val)
        if len(digits) == 11:
            return f"***.{digits[3:6]}.{digits[6:9]}-**"

        # Match embedded 11-digit patterns in text
        def _mask_repl(match: re.Match) -> str:
            d = re.sub(r"\D", "", match.group(0))
            return f"***.{d[3:6]}.{d[6:9]}-**"

        return re.sub(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b", _mask_repl, val)

    @staticmethod
    def decode_gov_bytes(raw: bytes) -> str:
        """Decode raw binary government feeds trying UTF-8 first with CP1252/Latin-1 fallback."""
        if not raw:
            return ""
        try:
            decoded = raw.decode("utf-8")
        except UnicodeDecodeError:
            try:
                decoded = raw.decode("cp1252")
            except UnicodeDecodeError:
                decoded = raw.decode("latin-1", errors="replace")

        return decoded.replace("\r\n", "\n").replace("\r", "\n")

    @staticmethod
    def parse_ptbr_decimal(val: str | float | int | None) -> float | None:
        """Converts PT-BR formatted decimal strings (e.g. '1.234.567,89') to standard float."""
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)

        s = str(val).strip()
        if not s or s.lower() in _SENTINEL_STRINGS:
            return None

        # Format 1.234.567,89 -> remove dots, replace comma with dot
        if "," in s:
            cleaned = s.replace(".", "").replace(",", ".")
        else:
            cleaned = s

        try:
            return float(cleaned)
        except ValueError:
            return None


class MonetaryNormalizer:
    """Inflation adjustment and currency conversion."""

    @staticmethod
    def deflate_series(
        amount: float,
        from_period: str,
        to_period: str,
        index_map: dict[str, float],
    ) -> float:
        """Adjust a monetary amount for inflation using an index map (e.g. IPCA)."""
        if from_period not in index_map:
            raise ValueError(f"Base period '{from_period}' not found in index map.")
        if to_period not in index_map:
            raise ValueError(f"Target period '{to_period}' not found in index map.")

        base_index = index_map[from_period]
        target_index = index_map[to_period]

        if base_index <= 0:
            raise ValueError(f"Invalid non-positive index value for period '{from_period}': {base_index}")

        return round(amount * (target_index / base_index), 4)

    @staticmethod
    def convert_fx(amount: float, rate: float) -> float:
        """Convert monetary value using foreign exchange spot rate."""
        if rate <= 0:
            raise ValueError(f"Exchange rate must be positive, got {rate}")
        return round(amount * rate, 4)
