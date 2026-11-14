{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(obitos_prematuros_dcnt_30_69_anos as integer) as obitos_prematuros_dcnt_30_69_anos,
    cast(proporcao_mortalidade_prematura_dcnt_pct as double) as proporcao_mortalidade_prematura_dcnt_pct,
    cast(taxa_mortalidade_causas_externas_100k_hab as double) as taxa_mortalidade_causas_externas_100k_hab,
    cast(taxa_mortalidade_acidentes_transporte_100k_hab as double) as taxa_mortalidade_acidentes_transporte_100k_hab,
    cast(anos_potenciais_vida_perdidos_1000_hab as double) as anos_potenciais_vida_perdidos_1000_hab,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sim_silver', 'sim_mortalidade_prematura_dcnt_externas') }}
