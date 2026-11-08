{{ config(materialized='table') }}

with capag as (
    select * from {{ ref('fct_capacidade_pagamento_capag') }}
),

fpm_anual as (
    select
        id_municipio_ibge_7d,
        ano_exercicio,
        sum(valor_bruto_fpm_mes) as total_fpm_bruto_anual,
        sum(valor_liquido_fpm_mes) as total_fpm_liquido_anual
    from {{ ref('fct_transferencia_constitucional_fpm') }}
    group by id_municipio_ibge_7d, ano_exercicio
),

pvl_stats as (
    select
        id_municipio_ibge_7d,
        count(distinct id_pleito) as total_pleitos_pvl,
        sum(valor_operacao) as volume_credito_solicitado_pvl,
        sum(case when status_pleito = 'Deferido' then valor_operacao else 0.0 end) as volume_credito_deferido_pvl
    from {{ ref('fct_operacao_credito_pvl') }}
    group by id_municipio_ibge_7d
)

select
    c.id_municipio_ibge_7d,
    c.sigla_uf,
    c.nome_ente as nome_municipio,
    c.ano_exercicio,
    c.nota_capag_final,
    c.receita_corrente_liquida_rcl,
    c.divida_consolidada_liquida_dcl,
    c.indicador_1_endividamento_pct as razao_dcl_rcl_pct,
    c.indicador_2_poupanca_corrente_pct,
    c.indicador_3_liquidez_relativa_pct,
    coalesce(f.total_fpm_bruto_anual, 0.0) as total_fpm_bruto_anual,
    coalesce(f.total_fpm_liquido_anual, 0.0) as total_fpm_liquido_anual,
    coalesce(p.total_pleitos_pvl, 0) as total_pleitos_pvl,
    coalesce(p.volume_credito_solicitado_pvl, 0.0) as volume_credito_solicitado_pvl,
    coalesce(p.volume_credito_deferido_pvl, 0.0) as volume_credito_deferido_pvl,
    case
        when c.nota_capag_final = 'A' then 'EXCELENTE_CAPAG_A'
        when c.nota_capag_final = 'B' then 'REGULAR_CAPAG_B'
        else 'CRITICO_CAPAG_C'
    end as classificacao_saude_fiscal
from capag c
left join fpm_anual f on c.id_municipio_ibge_7d = f.id_municipio_ibge_7d and c.ano_exercicio = f.ano_exercicio
left join pvl_stats p on c.id_municipio_ibge_7d = p.id_municipio_ibge_7d
