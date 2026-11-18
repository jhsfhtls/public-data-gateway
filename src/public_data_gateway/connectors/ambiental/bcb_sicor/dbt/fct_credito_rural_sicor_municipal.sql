{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_bcb_sicor_resumo_municipal') }}
),

pp as (
    select * from {{ ref('stg_bcb_sicor_pronaf_proagro') }}
),

custeio_agg as (
    select
        id_municipio,
        sum(qtd_contratos_custeio) as qtd_contratos_custeio_amostra,
        round(sum(valor_custeio_brl), 2) as valor_custeio_amostra_brl
    from {{ ref('stg_bcb_sicor_custeio') }}
    group by id_municipio
),

invest_agg as (
    select
        id_municipio,
        sum(qtd_contratos_investimento) as qtd_contratos_investimento_amostra,
        round(sum(valor_investimento_brl), 2) as valor_investimento_amostra_brl
    from {{ ref('stg_bcb_sicor_investimento') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.bioma_predominante,
    r.qtd_contratos_sicor,
    r.valor_custeio_sicor_brl,
    r.valor_investimento_sicor_brl,
    r.valor_comercializacao_sicor_brl,
    r.valor_total_sicor_brl,
    r.area_financiada_total_ha,
    r.ticket_medio_contrato_brl,
    pp.participacao_pronaf_pct,
    pp.cobertura_proagro_sicor_pct,
    pp.participacao_abc_renovagro_pct,
    pp.taxa_glosa_res_bcb_140_pct,
    coalesce(c.qtd_contratos_custeio_amostra, 0) as qtd_contratos_custeio_amostra,
    coalesce(c.valor_custeio_amostra_brl, 0.0) as valor_custeio_amostra_brl,
    coalesce(i.qtd_contratos_investimento_amostra, 0) as qtd_contratos_investimento_amostra,
    coalesce(i.valor_investimento_amostra_brl, 0.0) as valor_investimento_amostra_brl
from res r
left join pp on r.id_municipio = pp.id_municipio
left join custeio_agg c on r.id_municipio = c.id_municipio
left join invest_agg i on r.id_municipio = i.id_municipio
