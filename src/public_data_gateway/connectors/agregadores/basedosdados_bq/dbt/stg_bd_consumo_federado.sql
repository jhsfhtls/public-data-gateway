select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(consultas_bigquery_mensais as integer) as consultas_bigquery_mensais,
    cast(eficiencia_particionamento_pct as double) as eficiencia_particionamento_pct,
    cast(custo_medio_scan_mb as double) as custo_medio_scan_mb,
    cast(taxa_reuso_cross_domain_pct as double) as taxa_reuso_cross_domain_pct,
    cast(indice_eficiencia_federada_bd as double) as indice_eficiencia_federada_bd,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/basedosdados_bq/silver/bd_consumo_federado_sql.parquet')
