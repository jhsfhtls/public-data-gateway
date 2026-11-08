{{ config(materialized='table') }}

with capag as (
    select * from {{ ref('stg_stn_capag_municipios') }}
),

divida as (
    select * from {{ ref('stg_stn_divida_consolidada') }}
)

select
    c.id_municipio_ibge_7d,
    c.sigla_uf,
    c.nome_ente,
    c.ano_exercicio,
    c.indicador_1_endividamento_pct,
    c.indicador_2_poupanca_corrente_pct,
    c.indicador_3_liquidez_relativa_pct,
    c.nota_indicador_1,
    c.nota_indicador_2,
    c.nota_indicador_3,
    c.nota_capag_final,
    d.receita_corrente_liquida_rcl,
    d.divida_consolidada_bruta,
    d.disponibilidade_caixa_bruta,
    d.divida_consolidada_liquida_dcl,
    d.limite_senado_rcl_pct
from capag c
left join divida d on c.id_municipio_ibge_7d = d.id_municipio_ibge_7d and c.ano_exercicio = d.ano_exercicio
