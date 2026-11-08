{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_tesouro_transparente/silver/stn_sadipem_pvl.parquet')
)

select
    cast(id_pleito as bigint) as id_pleito,
    cast(tipo_interessado as varchar) as tipo_interessado,
    cast(interessado as varchar) as interessado,
    cast(id_municipio_ibge_7d as varchar) as id_municipio_ibge_7d,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(num_pvl as varchar) as num_pvl,
    cast(status_pleito as varchar) as status_pleito,
    cast(tipo_operacao as varchar) as tipo_operacao,
    cast(finalidade as varchar) as finalidade,
    cast(tipo_credor as varchar) as tipo_credor,
    cast(credor as varchar) as credor,
    cast(moeda as varchar) as moeda,
    cast(valor_operacao as double) as valor_operacao,
    cast(data_status as varchar) as data_status
from source
