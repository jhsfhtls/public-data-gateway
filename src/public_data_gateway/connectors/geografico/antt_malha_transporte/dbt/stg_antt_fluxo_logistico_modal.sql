{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(modal_transporte as varchar) as modal_transporte,
    cast(participacao_modal_cargas_pct as double) as participacao_modal_cargas_pct,
    cast(custo_tonelada_km_brl as double) as custo_tonelada_km_brl,
    cast(velocidade_media_comercial_kmh as double) as velocidade_media_comercial_kmh,
    cast(indice_confiabilidade_corredor as double) as indice_confiabilidade_corredor,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('antt_malha_transporte_silver', 'antt_fluxo_logistico_modal') }}
