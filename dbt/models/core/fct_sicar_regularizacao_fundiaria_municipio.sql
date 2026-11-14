{{ config(materialized='table') }}

with imoveis as (
    select * from {{ ref('stg_sfb_sicar_imoveis_rurais') }}
),

pra as (
    select * from {{ ref('stg_sfb_sicar_regularizacao_pra') }}
),

geo as (
    select * from {{ ref('stg_sfb_sicar_geometria_sigef') }}
),

rl_agg as (
    select
        id_municipio,
        cast(round(cast(sum(area_reserva_legal_declarada_ha) as decimal(12, 4)), 2) as double) as area_reserva_legal_total_ha,
        cast(round(cast(sum(area_app_preservada_ha) as decimal(12, 4)), 2) as double) as area_app_total_ha,
        cast(round(cast(avg(taxa_passivo_ambiental_pct) as decimal(10, 4)), 2) as double) as taxa_passivo_media_pct,
        cast(round(cast(avg(conformidade_reserva_legal_pct) as decimal(10, 4)), 2) as double) as conformidade_reserva_legal_media_pct
    from {{ ref('stg_sfb_sicar_reserva_legal_app') }}
    group by id_municipio
)

select
    i.id_municipio,
    i.nome_municipio,
    i.sigla_uf,
    i.id_uf,
    i.regiao,
    i.bioma_predominante,
    i.ano_referencia,
    i.qtd_imoveis_car_ativos,
    i.area_cadastrada_car_ha,
    i.area_media_imovel_ha,
    i.imoveis_analise_concluida_pct,
    i.indice_cobertura_cadastral_car,
    p.qtd_imoveis_aderidos_pra,
    p.imoveis_sem_sobreposicao_ti_uc_pct,
    p.imoveis_livres_embargo_ibama_pct,
    p.elegibilidade_credito_rural_car_pct,
    p.velocidade_homologacao_car_pct,
    p.indice_governanca_pra,
    g.poligonos_sem_sobreposicao_pct,
    g.integracao_car_sigef_incra_pct,
    g.precisao_georreferenciamento_pct,
    g.atualizacao_retificacao_anual_pct,
    g.indice_qualidade_geoespacial_car,
    r.area_reserva_legal_total_ha,
    r.area_app_total_ha,
    r.taxa_passivo_media_pct,
    r.conformidade_reserva_legal_media_pct,
    i.reference_date
from imoveis as i
inner join pra as p
    on i.id_municipio = p.id_municipio
inner join geo as g
    on i.id_municipio = g.id_municipio
inner join rl_agg as r
    on i.id_municipio = r.id_municipio
