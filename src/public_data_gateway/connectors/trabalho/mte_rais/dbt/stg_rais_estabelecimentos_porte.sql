{{ config(materialized='view') }}

select
    cast(ano as integer) as ano,
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(faixa_porte as varchar) as faixa_porte,
    cast(total_estabelecimentos as integer) as total_estabelecimentos,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_rais/silver/rais_estabelecimentos_porte.parquet')
