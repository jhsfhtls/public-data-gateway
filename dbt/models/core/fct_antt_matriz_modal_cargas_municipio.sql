{{ config(materialized='table') }}

with modais as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        modal_transporte,
        participacao_modal_cargas_pct,
        custo_tonelada_km_brl,
        velocidade_media_comercial_kmh,
        indice_confiabilidade_corredor,
        reference_date,
        extracted_at
    from {{ ref('stg_antt_fluxo_logistico_modal') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    modal_transporte,
    participacao_modal_cargas_pct,
    custo_tonelada_km_brl,
    velocidade_media_comercial_kmh,
    indice_confiabilidade_corredor,
    round(
        sum(case when modal_transporte != 'Rodoviario' then participacao_modal_cargas_pct else 0.0 end)
        over (partition by id_municipio),
        2
    ) as participacao_nao_rodoviaria_pct,
    reference_date,
    extracted_at
from modais
