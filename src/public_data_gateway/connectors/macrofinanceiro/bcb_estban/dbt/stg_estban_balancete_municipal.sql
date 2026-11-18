{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_estban/silver/estban_balancete_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_base_yyyymm as varchar) as data_base_yyyymm,
    cast(qtd_instituicoes_atuantes as bigint) as qtd_instituicoes_atuantes,
    cast(qtd_agencias_ativas as bigint) as qtd_agencias_ativas,
    cast(ativo_total_agencias_brl as double) as ativo_total_agencias_brl,
    cast(operacoes_credito_total_brl as double) as operacoes_credito_total_brl,
    cast(depositos_totais_brl as double) as depositos_totais_brl,
    cast(relacao_credito_deposito_pct as double) as relacao_credito_deposito_pct,
    cast(indice_concentracao_hhi_local as double) as indice_concentracao_hhi_local
from source
