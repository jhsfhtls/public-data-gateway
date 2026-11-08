{{ config(materialized='table') }}

with gini as (
    select * from {{ ref('stg_sidra_gini_rendimento_uf') }}
)

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano,
    indice_gini_rendimento,
    rendimento_medio_domiciliar_reais,
    case
        when indice_gini_rendimento >= 0.530 then 'MUITO_ALTA_DESIGUALDADE'
        when indice_gini_rendimento >= 0.490 then 'ALTA_DESIGUALDADE'
        when indice_gini_rendimento >= 0.450 then 'MEDIA_DESIGUALDADE'
        else 'MODERADA_DESIGUALDADE'
    end as faixa_desigualdade_renda,
    reference_date,
    extracted_at
from gini
