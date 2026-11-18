{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_siconfi/silver/siconfi_rgf_limites.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(quadrimestre as bigint) as quadrimestre,
    cast(receita_corrente_liquida_brl as double) as receita_corrente_liquida_brl,
    cast(despesa_total_pessoal_brl as double) as despesa_total_pessoal_brl,
    cast(despesa_pessoal_rcl_pct as double) as despesa_pessoal_rcl_pct,
    cast(limite_prudencial_pessoal_pct as double) as limite_prudencial_pessoal_pct,
    cast(limite_maximo_pessoal_pct as double) as limite_maximo_pessoal_pct,
    cast(divida_consolidada_liquida_brl as double) as divida_consolidada_liquida_brl,
    cast(status_alerta_lrf as varchar) as status_alerta_lrf
from source
