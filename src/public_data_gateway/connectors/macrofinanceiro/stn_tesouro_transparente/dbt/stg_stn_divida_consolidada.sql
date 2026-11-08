{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_tesouro_transparente/silver/stn_divida_consolidada.parquet')
)

select
    cast(id_municipio_ibge_7d as varchar) as id_municipio_ibge_7d,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(receita_corrente_liquida_rcl as double) as receita_corrente_liquida_rcl,
    cast(divida_consolidada_bruta as double) as divida_consolidada_bruta,
    cast(disponibilidade_caixa_bruta as double) as disponibilidade_caixa_bruta,
    cast(divida_consolidada_liquida_dcl as double) as divida_consolidada_liquida_dcl,
    cast(razao_dcl_rcl_pct as double) as razao_dcl_rcl_pct,
    cast(limite_senado_rcl_pct as double) as limite_senado_rcl_pct
from source
