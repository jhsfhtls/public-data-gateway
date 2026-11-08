{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_pncp_modalidades') }}
)

select
    modalidade_id,
    modalidade_nome,
    amparo_legal,
    descricao,
    is_exige_disputa,
    case
        when is_exige_disputa then 'COMPETITIVA'
        else 'DIRETA'
    end as regime_competicao
from stg
