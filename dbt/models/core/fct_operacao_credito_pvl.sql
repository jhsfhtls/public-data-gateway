{{ config(materialized='table') }}

with pvl as (
    select * from {{ ref('stg_stn_sadipem_pvl') }}
)

select
    id_pleito,
    tipo_interessado,
    interessado,
    id_municipio_ibge_7d,
    sigla_uf,
    num_pvl,
    status_pleito,
    tipo_operacao,
    finalidade,
    tipo_credor,
    credor,
    moeda,
    valor_operacao,
    data_status
from pvl
