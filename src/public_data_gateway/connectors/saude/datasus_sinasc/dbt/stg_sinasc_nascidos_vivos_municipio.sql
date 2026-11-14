{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(total_nascidos_vivos as integer) as total_nascidos_vivos,
    cast(taxa_bruta_natalidade_1000_hab as double) as taxa_bruta_natalidade_1000_hab,
    cast(nascidos_vivos_hospitalares as integer) as nascidos_vivos_hospitalares,
    cast(proporcao_partos_hospitalares_pct as double) as proporcao_partos_hospitalares_pct,
    cast(nascidos_vivos_parto_cesareo as integer) as nascidos_vivos_parto_cesareo,
    cast(proporcao_partos_cesareos_pct as double) as proporcao_partos_cesareos_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sinasc_silver', 'sinasc_nascidos_vivos_municipio') }}
