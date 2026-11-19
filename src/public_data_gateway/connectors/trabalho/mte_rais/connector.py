"""Official RAIS annual microdata to municipal Silver aggregates and Gold features.

Raw public microdata are aggregated immediately. The original 7z archives remain in
the local Bronze directory; worker-level rows are never written to Silver or Gold.
"""

from __future__ import annotations

import csv
import ftplib
import json
import logging
import math
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import py7zr
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.models import DataContract

logger = logging.getLogger(__name__)


@lru_cache(maxsize=256)
def _key(value: str) -> str:
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return " ".join(text.casefold().split())


def _value(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(_key(name))
        if value is not None:
            return value.strip()
    return ""


def _number(value: str) -> float:
    value = value.strip().replace(" ", "")
    if not value:
        return 0.0
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return 0.0


def _reported_number(value: str) -> float | None:
    if not value.strip():
        return None
    try:
        parsed = float(value.strip().replace(",", "."))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


class MTERaisConnector(BaseConnector):
    """Aggregate published RAIS microdata, with explicit source coverage."""

    SOURCE_ID = "mte_rais"
    FTP_HOST = "ftp.mtps.gov.br"
    FTP_BASE = "pdet/microdados/RAIS"
    VINC_ARCHIVES = (
        "RAIS_VINC_PUB_CENTRO_OESTE.7z",
        "RAIS_VINC_PUB_MG_ES_RJ.7z",
        "RAIS_VINC_PUB_NI.7z",
        "RAIS_VINC_PUB_NORDESTE.7z",
        "RAIS_VINC_PUB_NORTE.7z",
        "RAIS_VINC_PUB_SP.7z",
        "RAIS_VINC_PUB_SUL.7z",
    )
    ESTAB_ARCHIVE = "RAIS_ESTAB_PUB.7z"
    SILVER_TABLES = (
        "rais_vinculos_ativos",
        "rais_remuneracao_media",
        "rais_estabelecimentos_porte",
        "rais_massa_salarial_municipio",
        "rais_rotatividade_demografia",
    )
    GOLD_FILE = "features_rais_mercado_formal_municipios_brasil.parquet"
    COMMON = ("ano", "id_municipio", "nome_municipio", "sigla_uf")
    MONTHS = (
        "Janeiro",
        "Fevereiro",
        "Março",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
    )

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        super().__init__(self.SOURCE_ID, settings)
        with (Path(__file__).parent / "contract.yaml").open(encoding="utf-8") as stream:
            self.contract_raw = yaml.safe_load(stream)
        self.contract = DataContract(
            contract_id=self.contract_raw["contract_id"],
            source_id=self.SOURCE_ID,
            version=self.contract_raw["schema_version"],
            grain="municipality-year and declared analytical dimensions",
            primary_keys=["ano", "id_municipio"],
            min_expected_rows=1,
        )

    def _ftp(self) -> ftplib.FTP:
        ftp = ftplib.FTP(self.FTP_HOST, timeout=30)
        ftp.encoding = "latin-1"
        ftp.login()
        return ftp

    def verify_live(self) -> dict[str, Any]:
        """Probe the RAIS FTP directory and real annual archive metadata."""
        report: dict[str, Any] = {
            "source_id": self.SOURCE_ID,
            "timestamp": datetime.now(UTC).isoformat(),
            "status": "FAIL",
            "endpoints": {},
        }
        ftp: ftplib.FTP | None = None
        try:
            ftp = self._ftp()
            years = ftp.nlst(self.FTP_BASE)
            annual = [int(x.rsplit("/", 1)[-1]) for x in years if x.rsplit("/", 1)[-1].isdigit()]
            if not annual:
                raise ValueError("Official RAIS FTP has no annual directories")
            year = max(annual)
            names = {x.rsplit("/", 1)[-1] for x in ftp.nlst(f"{self.FTP_BASE}/{year}")}
            required = {self.ESTAB_ARCHIVE, *self.VINC_ARCHIVES}
            missing = sorted(required - names)
            archive_bytes = (
                ftp.size(f"{self.FTP_BASE}/{year}/{self.ESTAB_ARCHIVE}")
                if self.ESTAB_ARCHIVE in names
                else 0
            )
            passed = not missing and bool(archive_bytes and archive_bytes > 0)
            report["endpoints"]["ftp_rais"] = {
                "host": self.FTP_HOST,
                "directory": f"{self.FTP_BASE}/{year}",
                "available_archives": len(required - set(missing)),
                "missing_archives": missing,
                "establishment_archive_bytes": archive_bytes,
                "passed": passed,
            }
            report["status"] = "PASS" if passed else "FAIL"
        except (
            OSError,
            EOFError,
            ValueError,
            ftplib.error_reply,
            ftplib.error_perm,
            ftplib.error_proto,
            ftplib.error_temp,
        ) as exc:
            report["endpoints"]["ftp_rais"] = {"passed": False, "error": str(exc)}
            logger.warning("RAIS live probe failed: %s", exc)
        finally:
            if ftp is not None:
                try:
                    ftp.quit()
                except (
                    OSError,
                    EOFError,
                    ftplib.error_reply,
                    ftplib.error_perm,
                    ftplib.error_proto,
                    ftplib.error_temp,
                ):
                    ftp.close()
        return report

    def test_connection(self) -> bool:
        return self.verify_live()["status"] == "PASS"

    def _download(self, ftp: ftplib.FTP, year: int, name: str) -> Path:
        """Resume a partial archive and reject files with unexpected source size."""
        path = self.settings.data_dir / self.SOURCE_ID / "bronze" / str(year) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        remote = f"{self.FTP_BASE}/{year}/{name}"
        expected = ftp.size(remote)
        if expected is None or expected <= 0:
            raise ValueError(f"RAIS archive has invalid FTP size: {remote}")
        current = path.stat().st_size if path.exists() else 0
        if current > expected:
            raise ValueError(f"Local RAIS archive exceeds FTP size: {path}")
        if current < expected:
            with path.open("ab") as stream:
                ftp.retrbinary(
                    f"RETR {remote}", stream.write, blocksize=262144, rest=current or None
                )
        if path.stat().st_size != expected:
            raise OSError(f"Incomplete RAIS archive: {path}")
        return path

    def _download_archive(self, year: int, name: str) -> Path:
        for attempt in range(3):
            try:
                with self._ftp() as ftp:
                    return self._download(ftp, year, name)
            except (
                OSError,
                EOFError,
                ftplib.error_reply,
                ftplib.error_perm,
                ftplib.error_proto,
                ftplib.error_temp,
            ) as exc:
                logger.warning("RAIS download attempt %s failed for %s: %s", attempt + 1, name, exc)
                if attempt == 2:
                    raise
                time.sleep(2**attempt)
        raise RuntimeError("Unreachable RAIS download retry state")

    def _extract_archive(self, path: Path) -> Path:
        """Extract exactly one official COMT file; reject oversized archives."""
        with py7zr.SevenZipFile(path) as archive:
            entries = [entry for entry in archive.list() if not entry.is_directory]
            if len(entries) != 1 or entries[0].uncompressed > 15_000_000_000:
                raise ValueError(f"Unexpected RAIS archive contents: {path}")
            member = entries[0].filename
            if Path(member).name != member or Path(member).suffix.upper() not in {
                ".COMT",
                ".TXT",
                ".CSV",
            }:
                raise ValueError(f"Unsafe RAIS archive member: {member}")
            output = path.parent / member
            if not output.exists() or output.stat().st_size != entries[0].uncompressed:
                archive.extractall(path=path.parent)
            return output

    def _municipalities(self) -> dict[str, tuple[str, str, str]]:
        path = self.settings.data_dir / "reference" / "municipios_brasil.json"
        with path.open(encoding="utf-8") as stream:
            records = json.load(stream)
        return {
            str(item["id_municipio_6"]): (
                str(item["id_municipio"]),
                str(item["nome"]),
                str(item["sigla_uf"]),
            )
            for item in records
        }

    def _rows(self, path: Path, required: tuple[str, ...]):
        with path.open(encoding="latin-1", newline="") as stream:
            reader = csv.DictReader(stream, delimiter=",")
            reader.fieldnames = [_key(name) for name in reader.fieldnames or []]
            missing = [name for name in required if _key(name) not in reader.fieldnames]
            if missing:
                raise ValueError(f"RAIS CSV missing required headers {missing}: {path}")
            yield from reader

    def _municipality(
        self,
        row: dict[str, str],
        municipalities: dict[str, tuple[str, str, str]],
        *,
        workplace: bool = True,
    ) -> tuple[str, str, str] | None:
        if workplace:
            workplace_code = _value(row, "Município Trab - Código")
            mapped = municipalities.get(workplace_code.zfill(6))
            if mapped is not None:
                return mapped
        establishment_code = _value(row, "Município - Código")
        return municipalities.get(establishment_code.zfill(6))

    def extract_all(
        self,
        *,
        ano: int = 2025,
        vinculos_csv_paths: list[Path] | None = None,
        estabelecimentos_csv_path: Path | None = None,
        allow_partial: bool = False,
    ) -> dict[str, pa.Table]:
        """Build annual aggregates from source archives or caller-provided fixture CSVs.

        A nationwide baseline is written only when every official archive is consumed.
        Partial fixture processing is permitted solely with ``allow_partial=True``.
        """
        if (vinculos_csv_paths is None) != (estabelecimentos_csv_path is None):
            raise ValueError("Both vínculo and establishment CSV inputs are required")
        official = vinculos_csv_paths is None
        if not official and not allow_partial:
            raise ValueError("Caller-provided CSVs require allow_partial=True")
        if official:
            names = [*self.VINC_ARCHIVES, self.ESTAB_ARCHIVE]
            # The public FTP server stalls with seven parallel RETR commands.
            # Two connections have been verified to transfer concurrently.
            with ThreadPoolExecutor(max_workers=2) as pool:
                downloaded = list(pool.map(lambda name: self._download_archive(ano, name), names))
            paths, est_path = downloaded[:-1], downloaded[-1]
            vinculos_csv_paths = [self._extract_archive(path) for path in paths]
            estabelecimentos_csv_path = self._extract_archive(est_path)
        assert vinculos_csv_paths is not None and estabelecimentos_csv_path is not None
        municipalities = self._municipalities()
        stock: dict[tuple[str, str], int] = defaultdict(int)
        wage: dict[tuple[str, str, str], list[float]] = defaultdict(
            lambda: [0.0, 0.0, 0.0, 0.0, 0.0]
        )
        demography: dict[tuple[str, str, str], list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
        mass: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
        establishments: dict[tuple[str, str], int] = defaultdict(int)
        skipped = 0
        workplace_fallback_rows = 0
        vinculo_rows = 0
        active_rows = 0
        establishment_rows = 0
        vinc_required = (
            "Município Trab - Código",
            "Ind Vínculo Ativo 31/12 - Código",
            "CNAE 2.0 Subclasse - Codigo",
            "Escolaridade Após 2005 - Código",
            "Sexo - Código",
            "Vl Rem Média Nom",
            "Vl Rem Média (SM)",
            "Idade",
            "Tempo Emprego",
            "Vl Rem Dezembro Nom",
            *(f"Vl Rem {month} SC" for month in self.MONTHS),
        )
        for path in vinculos_csv_paths:
            for row in self._rows(Path(path), vinc_required):
                vinculo_rows += 1
                municipality = self._municipality(row, municipalities)
                if municipality is None:
                    skipped += 1
                    continue
                if _value(row, "Ind Vínculo Ativo 31/12 - Código") != "1":
                    continue
                active_rows += 1
                workplace_code = _value(row, "Município Trab - Código").zfill(6)
                if workplace_code not in municipalities:
                    workplace_fallback_rows += 1
                municipal_id = municipality[0]
                cnae = _value(row, "CNAE 2.0 Subclasse - Codigo", "CNAE 2.0 Classe - Código").strip(
                    '" '
                )
                sector = cnae[:2].zfill(2) if cnae else "00"
                stock[(municipal_id, sector)] += 1
                school = _value(row, "Escolaridade Após 2005 - Código") or "IGNORADO"
                sex = _value(row, "Sexo - Código") or "IGNORADO"
                wage_real = _reported_number(_value(row, "Vl Rem Média Nom"))
                wage_sm = _reported_number(_value(row, "Vl Rem Média (SM)"))
                group = wage[(municipal_id, school, sex)]
                group[0] += 1
                if wage_real is not None:
                    group[1] += wage_real
                    group[3] += 1
                if wage_sm is not None:
                    group[2] += wage_sm
                    group[4] += 1
                age = _number(_value(row, "Idade"))
                age_band = "ATE_24" if age < 25 else "25_A_49" if age < 50 else "50_OU_MAIS"
                tenure = _number(_value(row, "Tempo Emprego"))
                demo = demography[(municipal_id, age_band, school)]
                demo[0] += 1
                demo[1] += tenure
                demo[2] += int(tenure <= 12)
                annual = sum(
                    max(_number(_value(row, f"Vl Rem {month} SC")), 0) for month in self.MONTHS
                )
                annual += max(_number(_value(row, "Vl Rem Dezembro Nom")), 0)
                mass[municipal_id][0] += 1
                mass[municipal_id][1] += annual
        if not stock:
            raise ValueError("No active RAIS vínculo rows with mapped municipalities")
        estab_required = (
            "Município - Código",
            "Tamanho Estabelecimento - Código",
            "Qtd Vínculos Ativos",
        )
        for row in self._rows(Path(estabelecimentos_csv_path), estab_required):
            establishment_rows += 1
            municipality = self._municipality(row, municipalities, workplace=False)
            if municipality is None:
                skipped += 1
                continue
            size = (
                _value(row, "Tamanho Estabelecimento - Código", "Tamanho Estabelecimento")
                or "IGNORADO"
            )
            establishments[(municipality[0], size)] += 1
        if not establishments:
            raise ValueError("No RAIS establishment rows with mapped municipalities")
        now = datetime.now(UTC).isoformat()
        source_url = f"ftp://{self.FTP_HOST}/{self.FTP_BASE}/{ano}/"
        reference_date = f"{ano}-12-31"

        def common(municipal_id: str) -> dict[str, Any]:
            name, uf = municipality_by_7[municipal_id]
            return {
                "ano": ano,
                "id_municipio": municipal_id,
                "nome_municipio": name,
                "sigla_uf": uf,
                "source_url": source_url,
                "reference_date": reference_date,
                "extracted_at": now,
            }

        municipality_by_7 = {v[0]: (v[1], v[2]) for v in municipalities.values()}
        tables: dict[str, list[dict[str, Any]]] = {name: [] for name in self.SILVER_TABLES}
        for (municipal_id, sector), count in sorted(stock.items()):
            tables["rais_vinculos_ativos"].append(
                {
                    **common(municipal_id),
                    "grande_setor_cnae": sector,
                    "total_vinculos_ativos": count,
                }
            )
        for (municipal_id, school, sex), values in sorted(wage.items()):
            count, real, sm, real_count, sm_count = values
            tables["rais_remuneracao_media"].append(
                {
                    **common(municipal_id),
                    "nivel_escolaridade": school,
                    "sexo_trabalhador": sex,
                    "total_vinculos": int(count),
                    "remuneracao_media_reais": real / real_count if real_count else 0.0,
                    "remuneracao_media_sm": sm / sm_count if sm_count else 0.0,
                }
            )
        for (municipal_id, size), count in sorted(establishments.items()):
            tables["rais_estabelecimentos_porte"].append(
                {**common(municipal_id), "faixa_porte": size, "total_estabelecimentos": count}
            )
        for municipal_id, (count, amount) in sorted(mass.items()):
            tables["rais_massa_salarial_municipio"].append(
                {
                    **common(municipal_id),
                    "total_vinculos_ativos": int(count),
                    "massa_salarial_anual_reais": round(amount, 2),
                }
            )
        for (municipal_id, age_band, school), values in sorted(demography.items()):
            count, tenure_sum, recent = values
            tables["rais_rotatividade_demografia"].append(
                {
                    **common(municipal_id),
                    "faixa_etaria": age_band,
                    "nivel_escolaridade": school,
                    "total_vinculos_ativos": int(count),
                    "tempo_medio_emprego_meses": tenure_sum / count,
                    "taxa_vinculos_ate_12_meses_pct": 100 * recent / count,
                }
            )
        silver: dict[str, pa.Table] = {}
        silver_dir = self.settings.data_dir / self.SOURCE_ID / "silver"
        silver_dir.mkdir(parents=True, exist_ok=True)
        for name, records in tables.items():
            table = pa.Table.from_pylist(records)
            pq.write_table(table, silver_dir / f"{name}.parquet", compression="zstd")
            silver[name] = table
        self._write_gold(
            ano, common, mass, wage, demography, establishments, source_url, reference_date, now
        )
        source_dir = (self.settings.data_dir / self.SOURCE_ID / "bronze" / str(ano)).resolve()
        official_partial = not official and all(
            Path(path).resolve().parent == source_dir
            for path in [*vinculos_csv_paths, estabelecimentos_csv_path]
        )
        coverage = {
            "source_id": self.SOURCE_ID,
            "ano": ano,
            "coverage": (
                "NATIONAL"
                if official
                else "PARTIAL_OFFICIAL"
                if official_partial
                else "PARTIAL_FIXTURE"
            ),
            "official_archives": list(self.VINC_ARCHIVES) + [self.ESTAB_ARCHIVE]
            if official
            else [],
            "archive_sizes_bytes": (
                {path.name: path.stat().st_size for path in [*paths, est_path]} if official else {}
            ),
            "input_files": [str(path) for path in vinculos_csv_paths]
            + [str(estabelecimentos_csv_path)],
            "unmapped_rows": skipped,
            "workplace_fallback_rows": workplace_fallback_rows,
            "vinculo_rows": vinculo_rows,
            "active_rows": active_rows,
            "establishment_rows": establishment_rows,
            "extracted_at": now,
        }
        with (self.settings.data_dir / self.SOURCE_ID / "coverage.json").open(
            "w", encoding="utf-8", newline="\n"
        ) as stream:
            json.dump(coverage, stream, ensure_ascii=False, indent=2)
        return silver

    def _write_gold(
        self,
        ano: int,
        common: Any,
        mass: dict,
        wage: dict,
        demography: dict,
        establishments: dict,
        source_url: str,
        reference_date: str,
        now: str,
    ) -> None:
        counts = {key: int(value[0]) for key, value in mass.items()}
        maximum = max(counts.values())
        establishment_totals: dict[str, int] = defaultdict(int)
        wage_totals: dict[str, float] = defaultdict(float)
        wage_reported_counts: dict[str, int] = defaultdict(int)
        tenure_totals: dict[str, float] = defaultdict(float)
        for (municipal_id, _), count in establishments.items():
            establishment_totals[municipal_id] += count
        for (municipal_id, _, _), values in wage.items():
            wage_totals[municipal_id] += values[1]
            wage_reported_counts[municipal_id] += int(values[3])
        for (municipal_id, _, _), values in demography.items():
            tenure_totals[municipal_id] += values[1]
        gold = []
        for municipal_id, (count, amount) in sorted(mass.items()):
            score = 100 * math.log1p(count) / math.log1p(maximum)
            classification = "ALTA" if score >= 75 else "MEDIA" if score >= 40 else "BAIXA"
            gold.append(
                {
                    **common(municipal_id),
                    "total_vinculos_ativos": int(count),
                    "total_estabelecimentos": establishment_totals[municipal_id],
                    "remuneracao_media_reais": (
                        wage_totals[municipal_id] / wage_reported_counts[municipal_id]
                        if wage_reported_counts[municipal_id]
                        else 0.0
                    ),
                    "massa_salarial_anual_reais": round(amount, 2),
                    "tempo_medio_emprego_meses": tenure_totals[municipal_id] / count,
                    "score_formalizacao_trabalho_municipio": round(score, 4),
                    "classificacao_formalizacao_trabalho_municipio": classification,
                }
            )
        path = self.settings.data_dir / self.SOURCE_ID / "gold" / self.GOLD_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(gold), path, compression="zstd")

    def extract(self) -> pa.Table:
        return self.extract_all()["rais_vinculos_ativos"]

    def _read(self, layer: str, filename: str) -> list[dict[str, Any]]:
        path = self.settings.data_dir / self.SOURCE_ID / layer / filename
        if not path.exists():
            return []
        return pq.read_table(path).to_pylist()

    def get_formal_employment_summary(
        self, id_municipio: str, ano: int | None = None
    ) -> dict[str, Any] | None:
        rows = self._read("silver", "rais_massa_salarial_municipio.parquet")
        return next(
            (
                row
                for row in rows
                if row["id_municipio"] == id_municipio.strip()
                and (ano is None or row["ano"] == ano)
            ),
            None,
        )

    def get_rais_market_profile(self, id_municipio: str) -> dict[str, Any] | None:
        rows = self._read("gold", self.GOLD_FILE)
        return next((row for row in rows if row["id_municipio"] == id_municipio.strip()), None)

    def get_sectoral_employment(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        rows = self._read("silver", "rais_vinculos_ativos.parquet")
        return [
            row
            for row in rows
            if row["id_municipio"] == id_municipio.strip() and (ano is None or row["ano"] == ano)
        ]

    def list_top_formalized_municipalities(self, limit: int = 10) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self._read("gold", self.GOLD_FILE)
        return sorted(
            rows, key=lambda row: row["score_formalizacao_trabalho_municipio"], reverse=True
        )[:limit]


register_connector('mte_rais', MTERaisConnector)
