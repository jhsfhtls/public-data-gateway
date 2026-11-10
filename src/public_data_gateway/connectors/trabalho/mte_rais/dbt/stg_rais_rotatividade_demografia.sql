{{ config(materialized='view') }}

select
    cast(ano as integer) as ano,
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(faixa_etaria as varchar) as faixa_etaria,
    cast(nivel_escolaridade as varchar) as nivel_escolaridade,
    cast(total_vinculos_ativos as integer) as total_vinculos_ativos,
    cast(tempo_medio_emprego_meses as double) as tempo_medio_emprego_meses,
    cast(taxa_vinculos_ate_12_meses_pct as double) as taxa_vinculos_ate_12_meses_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_rais/silver/rais_rotatividade_demografia.parquet')
