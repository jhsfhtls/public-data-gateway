{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_capag/silver/capag_avaliacao_estadual.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_uf as varchar) as nome_uf,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(nota_capag_uf as varchar) as nota_capag_uf,
    cast(indicador_1_endividamento_uf_pct as double) as indicador_1_endividamento_uf_pct,
    cast(indicador_2_poupanca_corrente_uf_pct as double) as indicador_2_poupanca_corrente_uf_pct,
    cast(indicador_3_liquidez_uf_pct as double) as indicador_3_liquidez_uf_pct,
    cast(elegivel_garantia_uniao_uf as boolean) as elegivel_garantia_uniao_uf,
    cast(espaco_fiscal_estimado_brl as double) as espaco_fiscal_estimado_brl
from source
