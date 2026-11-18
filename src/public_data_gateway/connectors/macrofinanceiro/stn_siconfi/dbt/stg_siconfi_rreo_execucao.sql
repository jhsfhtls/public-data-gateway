{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_siconfi/silver/siconfi_rreo_execucao.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(bimestre as bigint) as bimestre,
    cast(receita_corrente_liquida_brl as double) as receita_corrente_liquida_brl,
    cast(receita_tributaria_propria_brl as double) as receita_tributaria_propria_brl,
    cast(transferencias_correntes_brl as double) as transferencias_correntes_brl,
    cast(despesa_capital_investimento_brl as double) as despesa_capital_investimento_brl,
    cast(resultado_primario_brl as double) as resultado_primario_brl
from source
