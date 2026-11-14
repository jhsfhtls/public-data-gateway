{{ config(materialized='table') }}

with causas as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        capitulo_cid10,
        obitos_capitulo,
        participacao_capitulo_pct,
        taxa_mortalidade_especifica_100k_hab,
        idade_media_obito_capitulo_anos,
        reference_date,
        extracted_at
    from {{ ref('stg_sim_causas_capitulos_cid10') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    capitulo_cid10,
    obitos_capitulo,
    participacao_capitulo_pct,
    taxa_mortalidade_especifica_100k_hab,
    idade_media_obito_capitulo_anos,
    reference_date,
    extracted_at
from causas
