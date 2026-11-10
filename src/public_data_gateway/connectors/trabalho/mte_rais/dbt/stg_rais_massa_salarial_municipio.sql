{{ config(materialized='view') }}

select
    cast(ano as integer) as ano,
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(total_vinculos_ativos as integer) as total_vinculos_ativos,
    cast(massa_salarial_anual_reais as double) as massa_salarial_anual_reais,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_rais/silver/rais_massa_salarial_municipio.parquet')
