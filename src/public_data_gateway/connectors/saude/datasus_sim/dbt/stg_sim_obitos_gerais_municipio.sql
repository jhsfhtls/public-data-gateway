{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(total_obitos_registrados as integer) as total_obitos_registrados,
    cast(taxa_bruta_mortalidade_1000_hab as double) as taxa_bruta_mortalidade_1000_hab,
    cast(obitos_hospitalares as integer) as obitos_hospitalares,
    cast(proporcao_obitos_hospitalares_pct as double) as proporcao_obitos_hospitalares_pct,
    cast(obitos_causas_mal_definidas as integer) as obitos_causas_mal_definidas,
    cast(proporcao_causas_mal_definidas_pct as double) as proporcao_causas_mal_definidas_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sim_silver', 'sim_obitos_gerais_municipio') }}
