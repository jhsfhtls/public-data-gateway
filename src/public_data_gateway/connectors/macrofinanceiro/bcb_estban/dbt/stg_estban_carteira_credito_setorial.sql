{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_estban/silver/estban_carteira_credito_setorial.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_base_yyyymm as varchar) as data_base_yyyymm,
    cast(emprestimos_titulos_descontados_brl as double) as emprestimos_titulos_descontados_brl,
    cast(financiamentos_gerais_brl as double) as financiamentos_gerais_brl,
    cast(financiamento_agropecuario_rural_brl as double) as financiamento_agropecuario_rural_brl,
    cast(financiamento_imobiliario_habitacional_brl as double) as financiamento_imobiliario_habitacional_brl,
    cast(provisao_credito_liquidacao_duvidosa_brl as double) as provisao_credito_liquidacao_duvidosa_brl,
    cast(taxa_provisao_inadimplencia_pct as double) as taxa_provisao_inadimplencia_pct
from source
