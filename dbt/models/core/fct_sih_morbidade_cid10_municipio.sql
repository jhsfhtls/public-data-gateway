{{ config(materialized='table') }}

with cid10 as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        capitulo_cid10,
        internacoes_capitulo_aih,
        participacao_capitulo_pct,
        valor_total_capitulo_brl,
        tempo_medio_permanencia_dias,
        taxa_letalidade_capitulo_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_sih_morbidade_capitulos_cid10') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    capitulo_cid10,
    internacoes_capitulo_aih,
    participacao_capitulo_pct,
    valor_total_capitulo_brl,
    round(valor_total_capitulo_brl / nullif(internacoes_capitulo_aih, 0), 2) as custo_medio_internacao_cid10_brl,
    tempo_medio_permanencia_dias,
    taxa_letalidade_capitulo_pct,
    reference_date,
    extracted_at
from cid10
