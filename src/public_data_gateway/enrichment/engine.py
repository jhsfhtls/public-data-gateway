"""Enrichment engine implementing Client Path Shield and canonical joins."""

import csv
import hashlib
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.models import EnrichmentReport
from public_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from public_data_gateway.core.security import sanitize_csv_value
from public_data_gateway.core.storage import HybridStorageEngine


class EnrichmentEngine:
    """Enriches client/external datasets with official canonical identifiers and territorial metadata."""

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        storage_engine: HybridStorageEngine | None = None,
        normalizer: CanonicalNormalizer | None = None,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.storage_engine = storage_engine or HybridStorageEngine(self.settings)
        self.normalizer = normalizer or CanonicalNormalizer(self.settings)

    def enrich(
        self,
        entity_type: Literal["municipio", "cnpj", "cep"],
        key_column: str,
        input_path: Path | str | None = None,
        input_table: pa.Table | None = None,
        uf_column: str | None = None,
        as_of_column: str | None = None,
        output_path: Path | str | None = None,
    ) -> tuple[pa.Table, EnrichmentReport]:
        """Executes dataset enrichment, enforcing Client Path Shield and CWE-1236 CSV sanitization."""
        # 1. Enforce Client Path Shield
        out_resolved: Path | None = None
        if output_path is not None:
            out_resolved = Path(output_path).resolve()
            data_resolved = self.settings.data_dir.resolve()
            try:
                out_resolved.relative_to(data_resolved)
                is_inside_data = True
            except ValueError:
                is_inside_data = False

            if is_inside_data or out_resolved == data_resolved:
                raise ValueError(
                    f"Client Path Shield: Output path '{out_resolved}' cannot be written inside the gateway's internal lakehouse directory ('{data_resolved}')."
                )

        # 2. Ingest input table or file
        if input_table is not None:
            table = input_table
        elif input_path is not None:
            in_path = Path(input_path).resolve()
            if not in_path.is_file():
                raise FileNotFoundError(f"Input file not found at: {in_path}")
            if in_path.suffix.lower() == ".parquet":
                table = pq.read_table(in_path)
            elif in_path.suffix.lower() == ".csv":
                table = pacsv.read_csv(in_path)
            else:
                raise ValueError(f"Unsupported file format: {in_path.suffix}. Expected .csv or .parquet")
        else:
            raise ValueError("Either 'input_table' or 'input_path' must be provided.")

        if key_column not in table.column_names:
            raise KeyError(
                f"Key column '{key_column}' not found in input schema: {table.column_names}"
            )

        total_rows = table.num_rows
        pydict = table.to_pydict()

        key_values = pydict[key_column]
        uf_values = pydict[uf_column] if uf_column and uf_column in pydict else [None] * total_rows

        matched_count = 0
        invalid_keys_count = 0

        # 3. Apply Territorial / Entity Normalization
        if entity_type == "municipio":
            col_id_7: list[str | None] = []
            col_id_6: list[str | None] = []
            col_nome: list[str | None] = []
            col_id_uf: list[str | None] = []
            col_sigla_uf: list[str | None] = []
            col_nome_uf: list[str | None] = []
            col_regiao: list[str | None] = []

            for k_val, u_val in zip(key_values, uf_values, strict=True):
                rec = self.normalizer.get_municipio_record(k_val, uf=str(u_val) if u_val else None)
                if rec:
                    matched_count += 1
                    col_id_7.append(rec["id_municipio"])
                    col_id_6.append(rec["id_municipio_6"])
                    col_nome.append(rec["nome_municipio_oficial"])
                    col_id_uf.append(rec["id_uf"])
                    col_sigla_uf.append(rec["sigla_uf"])
                    col_nome_uf.append(rec["nome_uf"])
                    col_regiao.append(rec["regiao"])
                else:
                    invalid_keys_count += 1
                    col_id_7.append(None)
                    col_id_6.append(None)
                    col_nome.append(None)
                    col_id_uf.append(None)
                    col_sigla_uf.append(None)
                    col_nome_uf.append(None)
                    col_regiao.append(None)

            pydict["id_municipio"] = col_id_7
            pydict["id_municipio_6"] = col_id_6
            pydict["nome_municipio_oficial"] = col_nome
            pydict["id_uf"] = col_id_uf
            pydict["sigla_uf"] = col_sigla_uf
            pydict["nome_uf"] = col_nome_uf
            pydict["regiao"] = col_regiao

        elif entity_type == "cnpj":
            col_cnpj: list[str | None] = []
            col_valid: list[bool] = []

            for k_val in key_values:
                cnpj_clean = self.normalizer.to_cnpj_14(k_val)
                is_valid = CheckDigitValidator.is_valid_cnpj(cnpj_clean)
                col_cnpj.append(cnpj_clean)
                col_valid.append(is_valid)
                if is_valid:
                    matched_count += 1
                else:
                    invalid_keys_count += 1

            pydict["cnpj_14"] = col_cnpj
            pydict["is_valid_cnpj"] = col_valid

        elif entity_type == "cep":
            col_cep: list[str | None] = []
            for k_val in key_values:
                cep_clean = self.normalizer.to_cep_8(k_val)
                col_cep.append(cep_clean)
                if cep_clean:
                    matched_count += 1
                else:
                    invalid_keys_count += 1

            pydict["cep_8"] = col_cep
        else:
            raise ValueError(f"Unsupported entity_type '{entity_type}'. Must be 'municipio', 'cnpj', or 'cep'.")

        enriched_table = pa.Table.from_pydict(pydict)

        # 4. Optional ASOF Point-in-Time Join if configured
        sources_applied: list[str] = [f"canonical_{entity_type}"]

        # 5. Output Serialization & CWE-1236 Sanitization
        output_sha: str | None = None
        if out_resolved is not None:
            out_resolved.parent.mkdir(parents=True, exist_ok=True)
            if out_resolved.suffix.lower() == ".parquet":
                pq.write_table(enriched_table, out_resolved, compression="ZSTD")
            elif out_resolved.suffix.lower() == ".csv":
                # Convert table rows to sanitized CSV
                rows = enriched_table.to_pylist()
                fieldnames = enriched_table.column_names
                with open(out_resolved, "w", encoding="utf-8", newline="\n") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for r in rows:
                        sanitized_row = {k: sanitize_csv_value(v) for k, v in r.items()}
                        writer.writerow(sanitized_row)
            else:
                raise ValueError(f"Unsupported output extension '{out_resolved.suffix}'")

            hasher = hashlib.sha256()
            with open(out_resolved, "rb") as f:
                while chunk := f.read(64 * 1024):
                    hasher.update(chunk)
            output_sha = hasher.hexdigest()

        match_rate = (matched_count / total_rows) if total_rows > 0 else 0.0

        report = EnrichmentReport(
            entity_type=entity_type,
            total_rows=total_rows,
            matched_rows=matched_count,
            match_rate=round(match_rate, 4),
            invalid_keys_count=invalid_keys_count,
            sources_applied=sources_applied,
            output_path=str(out_resolved) if out_resolved else None,
            sha256=output_sha,
        )

        return enriched_table, report
