{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(capitulo_cid10 as varchar) as capitulo_cid10,
    cast(obitos_capitulo as integer) as obitos_capitulo,
    cast(participacao_capitulo_pct as double) as participacao_capitulo_pct,
    cast(taxa_mortalidade_especifica_100k_hab as double) as taxa_mortalidade_especifica_100k_hab,
    cast(idade_media_obito_capitulo_anos as double) as idade_media_obito_capitulo_anos,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sim_silver', 'sim_causas_capitulos_cid10') }}
