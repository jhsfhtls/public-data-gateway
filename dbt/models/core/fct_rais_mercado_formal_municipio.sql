{{ config(materialized='table') }}

with salarios as (
    select
        ano,
        id_municipio,
        sum(total_vinculos) as vinculos_com_remuneracao,
        round(sum(remuneracao_media_reais * total_vinculos) / nullif(sum(total_vinculos), 0), 2) as remuneracao_media_reais,
        round(sum(remuneracao_media_sm * total_vinculos) / nullif(sum(total_vinculos), 0), 3) as remuneracao_media_sm
    from {{ ref('stg_rais_remuneracao_media') }}
    group by ano, id_municipio
)

select
    m.ano,
    m.id_municipio,
    m.nome_municipio,
    m.sigla_uf,
    m.total_vinculos_ativos,
    s.vinculos_com_remuneracao,
    s.remuneracao_media_reais,
    s.remuneracao_media_sm,
    m.massa_salarial_anual_reais,
    m.source_url,
    m.reference_date,
    m.extracted_at
from {{ ref('stg_rais_massa_salarial_municipio') }} m
left join salarios s
    on m.ano = s.ano and m.id_municipio = s.id_municipio
