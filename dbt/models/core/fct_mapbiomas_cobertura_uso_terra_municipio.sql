{{ config(materialized='table') }}

with cobertura as (
    select * from {{ ref('stg_mapbiomas_cobertura_anual') }}
),

agro as (
    select * from {{ ref('stg_mapbiomas_agropecuaria_pastagem') }}
),

alertas as (
    select * from {{ ref('stg_mapbiomas_alertas_validados') }}
),

trans_agg as (
    select
        id_municipio,
        cast(round(cast(sum(area_transicao_ha) as decimal(12, 4)), 2) as double) as area_total_transicionada_ha,
        cast(round(cast(avg(taxa_conversao_liquida_pct) as decimal(10, 4)), 2) as double) as taxa_conversao_media_pct
    from {{ ref('stg_mapbiomas_transicoes_uso_terra') }}
    group by id_municipio
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.id_uf,
    c.regiao,
    c.bioma_predominante,
    c.ano_colecao,
    c.area_total_municipio_ha,
    c.area_floresta_nativa_ha,
    c.area_formacao_campestre_savana_ha,
    c.area_agropecuaria_total_ha,
    c.area_nao_vegetada_urbana_ha,
    c.area_corpos_dagua_ha,
    c.cobertura_nativa_pct,
    c.indice_preservacao_nativa,
    a.area_pastagem_ha,
    a.area_agricultura_temporaria_ha,
    a.area_agricultura_perene_silvicultura_ha,
    a.pastagem_degradada_pct,
    a.area_irrigada_ha,
    a.indice_sustentabilidade_agro,
    al.qtd_alertas_mapbiomas_ano,
    al.area_desmatada_validada_ha,
    al.alertas_com_embargo_ou_autuacao_pct,
    al.alertas_em_area_protegida_pct,
    al.velocidade_regeneracao_liquida_pct,
    al.indice_integridade_territorial,
    t.area_total_transicionada_ha,
    t.taxa_conversao_media_pct,
    c.reference_date
from cobertura as c
inner join agro as a
    on c.id_municipio = a.id_municipio
inner join alertas as al
    on c.id_municipio = al.id_municipio
inner join trans_agg as t
    on c.id_municipio = t.id_municipio
