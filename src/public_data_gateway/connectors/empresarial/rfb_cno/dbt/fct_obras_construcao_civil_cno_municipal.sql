{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_rfb_cno_resumo_municipal') }}
),

areas as (
    select * from {{ ref('stg_rfb_cno_areas_destinacao') }}
),

obras_agg as (
    select
        id_municipio,
        count(*) as qtd_obras_amostra_cadastral,
        round(avg(area_total_m2), 2) as area_media_obra_amostra_m2
    from {{ ref('stg_rfb_cno_obras_cadastradas') }}
    group by id_municipio
),

vinc_agg as (
    select
        id_municipio,
        count(distinct documento_responsavel) as qtd_responsaveis_amostra
    from {{ ref('stg_rfb_cno_vinculos_responsaveis') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.total_obras_cno,
    r.obras_ativas_em_andamento,
    r.obras_paralisadas_suspensas,
    r.obras_encerradas_habitese,
    r.area_total_registrada_m2,
    a.area_residencial_m2,
    a.area_comercial_industrial_m2,
    a.area_infraestrutura_publica_m2,
    a.area_total_construida_m2,
    a.percentual_obras_ativas_pct,
    coalesce(o.qtd_obras_amostra_cadastral, 0) as qtd_obras_amostra_cadastral,
    coalesce(o.area_media_obra_amostra_m2, 0.0) as area_media_obra_amostra_m2,
    coalesce(v.qtd_responsaveis_amostra, 0) as qtd_responsaveis_amostra
from res r
left join areas a on r.id_municipio = a.id_municipio
left join obras_agg o on r.id_municipio = o.id_municipio
left join vinc_agg v on r.id_municipio = v.id_municipio
