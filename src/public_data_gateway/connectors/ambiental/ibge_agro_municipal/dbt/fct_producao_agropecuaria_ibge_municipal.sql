{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_ibge_agro_resumo_municipal') }}
),

pevs as (
    select * from {{ ref('stg_ibge_agro_pevs_silvicultura') }}
),

pam_agg as (
    select
        id_municipio,
        count(*) as qtd_culturas_pam_amostra,
        round(sum(quantidade_produzida_t), 2) as quantidade_produzida_amostra_t
    from {{ ref('stg_ibge_agro_pam_lavouras') }}
    group by id_municipio
),

ppm_agg as (
    select
        id_municipio,
        sum(producao_leite_mil_litros) as producao_leite_mil_litros
    from {{ ref('stg_ibge_agro_ppm_pecuaria') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.bioma_predominante,
    r.area_colhida_pam_ha,
    r.valor_producao_pam_mil_brl,
    r.efetivo_rebanho_bovino_cabecas,
    r.valor_producao_pecuaria_mil_brl,
    r.valor_producao_silvicultura_pevs_mil_brl,
    r.valor_bruto_agro_total_mil_brl,
    r.produtividade_media_pam_kg_ha,
    pv.area_floresta_plantada_eucalipto_pinus_ha,
    pv.producao_madeira_tora_m3,
    coalesce(pa.qtd_culturas_pam_amostra, 0) as qtd_culturas_pam_amostra,
    coalesce(pa.quantidade_produzida_amostra_t, 0.0) as quantidade_produzida_amostra_t,
    coalesce(pp.producao_leite_mil_litros, 0.0) as producao_leite_mil_litros
from res r
left join pevs pv on r.id_municipio = pv.id_municipio
left join pam_agg pa on r.id_municipio = pa.id_municipio
left join ppm_agg pp on r.id_municipio = pp.id_municipio
