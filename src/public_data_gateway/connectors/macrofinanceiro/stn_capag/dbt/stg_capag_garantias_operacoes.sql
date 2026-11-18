{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_capag/silver/capag_garantias_operacoes.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(estoque_garantias_uniao_brl as double) as estoque_garantias_uniao_brl,
    cast(operacoes_contratadas_com_garantia as bigint) as operacoes_contratadas_com_garantia,
    cast(custo_medio_ponderado_aa_pct as double) as custo_medio_ponderado_aa_pct,
    cast(honras_aval_acumuladas_brl as double) as honras_aval_acumuladas_brl,
    cast(status_adimplencia_garantias as varchar) as status_adimplencia_garantias
from source
