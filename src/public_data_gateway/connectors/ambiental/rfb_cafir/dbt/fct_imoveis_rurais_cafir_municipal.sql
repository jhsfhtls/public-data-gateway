{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_rfb_cafir_resumo_municipal') }}
),

cnir as (
    select * from {{ ref('stg_rfb_cafir_vinculos_cnir') }}
),

conc as (
    select * from {{ ref('stg_rfb_cafir_concentracao_fundiaria') }}
),

imv_agg as (
    select
        id_municipio,
        count(*) as qtd_imoveis_amostra_cafir,
        round(avg(area_total_ha), 2) as area_media_amostra_ha
    from {{ ref('stg_rfb_cafir_imoveis_rurais') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.bioma_predominante,
    r.qtd_imoveis_cafir,
    r.area_total_cafir_ha,
    r.area_media_imovel_cafir_ha,
    r.vtn_medio_estimado_brl_ha,
    cn.taxa_vinculacao_cnir_pct,
    cn.taxa_georreferenciamento_sigef_pct,
    cn.paridade_cib_car_pct,
    cn.imoveis_pendentes_vinculacao_cnt,
    cc.indice_gini_fundiario_cafir,
    cc.area_pequenos_imoveis_ate_4mf_pct,
    cc.area_medios_imoveis_4a15mf_pct,
    cc.area_grandes_imoveis_acima_15mf_pct,
    coalesce(ia.qtd_imoveis_amostra_cafir, 0) as qtd_imoveis_amostra_cafir,
    coalesce(ia.area_media_amostra_ha, 0.0) as area_media_amostra_ha
from res r
left join cnir cn on r.id_municipio = cn.id_municipio
left join conc cc on r.id_municipio = cc.id_municipio
left join imv_agg ia on r.id_municipio = ia.id_municipio
