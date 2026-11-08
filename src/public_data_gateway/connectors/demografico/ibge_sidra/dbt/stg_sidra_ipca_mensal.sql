{{ config(materialized='view') }}

select
    cast(ano_mes as varchar) as ano_mes,
    cast(ano as integer) as ano,
    cast(mes as integer) as mes,
    cast(mes_ano_extenso as varchar) as mes_ano_extenso,
    cast(ipca_variacao_mensal_pct as double) as ipca_variacao_mensal_pct,
    cast(ipca_acumulado_ano_pct as double) as ipca_acumulado_ano_pct,
    cast(ipca_acumulado_12m_pct as double) as ipca_acumulado_12m_pct,
    cast(indice_geral as varchar) as indice_geral,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_sidra/silver/sidra_ipca_mensal.parquet')
