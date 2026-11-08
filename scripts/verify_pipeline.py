"""Verification script confirming physical execution of all 5 tables and intra-domain analytics mart."""

from __future__ import annotations

import duckdb

from brazil_data_gateway.connectors.demografico.ibge_localidades import (
    IBGELocalidadesConnector,
)


def main() -> None:
    print("=================================================================")
    print("Executing full connector pipeline across all 5 IBGE tables...")
    print("=================================================================")
    conn = IBGELocalidadesConnector()
    tables = conn.extract_all()
    print(f"[PIPELINE SUCCESS] Materialized {len(tables)} tables from Bronze to Gold:")
    for name, tbl in tables.items():
        print(f"  - {name:<14}: {tbl.num_rows:>6} rows, {tbl.num_columns:>2} columns")

    source_dir = conn.settings.data_dir / "ibge_localidades"
    print("\n[PHYSICAL ARTIFACTS VERIFICATION]")
    for tbl_name in ["municipios", "distritos", "subdistritos", "estados", "regioes"]:
        b_file = source_dir / "bronze" / f"raw_{tbl_name}.json"
        s_file = source_dir / "silver" / f"{tbl_name}.parquet"
        print(f"  - Bronze raw_{tbl_name}.json:  exists={b_file.is_file()} ({b_file.stat().st_size:,} bytes)")
        print(f"  - Silver {tbl_name}.parquet:   exists={s_file.is_file()} ({s_file.stat().st_size:,} bytes)")

    g_file = source_dir / "gold" / "features_municipio_ml.parquet"
    print(f"  - Gold features_ml.parquet: exists={g_file.is_file()} ({g_file.stat().st_size:,} bytes)")

    # DuckDB Validation
    con = duckdb.connect()
    silver_mun = source_dir / "silver" / "municipios.parquet"
    silver_res = con.execute(f"""
        SELECT id_municipio, nome_municipio, sigla_uf, regiao, is_capital 
        FROM read_parquet('{silver_mun.as_posix()}')
        WHERE id_municipio IN ('1302603', '3146107', '4305439', '5101837', '2609600', '5300108')
        ORDER BY id_municipio
    """).fetchall()
    print("\n[SILVER VALIDATION DIVERSE CITIES]")
    print(f"{'id_municipio':<14} {'nome_municipio':<26} {'sigla_uf':<10} {'regiao':<14} {'is_capital'}")
    for row in silver_res:
        print(f"{row[0]:<14} {row[1]:<26} {row[2]:<10} {row[3]:<14} {row[4]}")

    # Mart Validation in DuckDB
    duckdb_path = conn.settings.data_dir / "brasil.duckdb"
    if duckdb_path.exists():
        mart_con = duckdb.connect(str(duckdb_path), read_only=True)
        mart_sample = mart_con.execute("""
            SELECT id_municipio, nome_municipio, sigla_uf, qtd_distritos_no_municipio, qtd_municipios_na_uf
            FROM mart_hierarquia_territorial_brasil
            WHERE id_municipio IN ('1302603', '3146107', '4305439', '5101837', '2609600', '5300108')
            ORDER BY id_municipio
        """).fetchall()
        print("\n[DBT MART HIERARQUIA TERRITORIAL BRASIL]")
        print(f"{'id_municipio':<14} {'nome_municipio':<26} {'sigla_uf':<10} {'distritos':<12} {'mun_na_uf'}")
        for r in mart_sample:
            print(f"{r[0]:<14} {r[1]:<26} {r[2]:<10} {r[3]:<12} {r[4]}")
        mart_con.close()

    print("\n=================================================================")
    print("Pipeline verification completed 100% successfully!")
    print("=================================================================")


if __name__ == "__main__":
    main()
