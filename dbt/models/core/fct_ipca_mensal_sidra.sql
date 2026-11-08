{{ config(materialized='table') }}

with ipca as (
    select * from {{ ref('stg_sidra_ipca_mensal') }}
)

select
    ano_mes,
    ano,
    mes,
    mes_ano_extenso,
    ipca_variacao_mensal_pct,
    ipca_acumulado_ano_pct,
    ipca_acumulado_12m_pct,
    indice_geral,
    case
        when ipca_variacao_mensal_pct > 1.0 then 'PRESSAO_INFLACIONARIA_ALTA'
        when ipca_variacao_mensal_pct > 0.4 then 'INFLACAO_MODERADA'
        when ipca_variacao_mensal_pct >= 0.0 then 'ESTABILIDADE_PRECOS'
        else 'DEFLACAO'
    end as regime_inflacionario_mensal,
    reference_date,
    extracted_at
from ipca
