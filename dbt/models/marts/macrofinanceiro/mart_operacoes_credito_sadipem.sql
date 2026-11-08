{{ config(materialized='table') }}

with pvl as (
    select * from {{ ref('fct_operacao_credito_pvl') }}
),

capag as (
    select distinct id_municipio_ibge_7d, nota_capag_final from {{ ref('fct_capacidade_pagamento_capag') }}
)

select
    p.id_pleito,
    p.tipo_interessado,
    p.interessado,
    p.id_municipio_ibge_7d,
    p.sigla_uf,
    p.num_pvl,
    p.status_pleito,
    p.tipo_operacao,
    p.finalidade,
    p.tipo_credor,
    p.credor,
    p.moeda,
    p.valor_operacao,
    p.data_status,
    coalesce(c.nota_capag_final, 'N/A') as nota_capag_proponente,
    case
        when p.valor_operacao >= 100000000.0 then 'GRANDE_PORTE'
        when p.valor_operacao >= 10000000.0 then 'MEDIO_PORTE'
        else 'PEQUENO_PORTE'
    end as faixa_valor_credito
from pvl p
left join capag c on p.id_municipio_ibge_7d = c.id_municipio_ibge_7d
