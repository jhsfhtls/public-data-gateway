{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_sicor/silver/sicor_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_contratos_sicor as bigint) as qtd_contratos_sicor,
    cast(valor_custeio_sicor_brl as double) as valor_custeio_sicor_brl,
    cast(valor_investimento_sicor_brl as double) as valor_investimento_sicor_brl,
    cast(valor_comercializacao_sicor_brl as double) as valor_comercializacao_sicor_brl,
    cast(valor_total_sicor_brl as double) as valor_total_sicor_brl,
    cast(area_financiada_total_ha as double) as area_financiada_total_ha,
    cast(ticket_medio_contrato_brl as double) as ticket_medio_contrato_brl
from source
