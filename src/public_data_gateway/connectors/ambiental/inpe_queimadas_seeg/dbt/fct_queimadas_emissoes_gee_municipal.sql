{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_queimadas_seeg_resumo_municipal') }}
),

rf as (
    select * from {{ ref('stg_inpe_risco_fogo_mensal') }}
),

focos_agg as (
    select
        id_municipio,
        count(*) as qtd_focos_amostra_inpe,
        round(avg(frp_mw), 2) as frp_medio_amostra_mw
    from {{ ref('stg_inpe_focos_queimadas') }}
    group by id_municipio
),

seeg_agg as (
    select
        id_municipio,
        round(sum(emissao_liquida_tco2e), 2) as emissoes_amostra_tco2e,
        round(sum(remocao_tco2e), 2) as remocao_florestal_tco2e
    from {{ ref('stg_seeg_emissoes_gee_setor') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.bioma_predominante,
    r.qtd_focos_calor_inpe,
    r.area_cicatriz_queimada_ha,
    r.risco_fogo_medio_inpe,
    r.emissoes_liquidas_tco2e_seeg,
    r.participacao_agro_lulucf_pct,
    rf.frp_medio_mw,
    rf.dias_seco_consecutivos_max,
    rf.concentracao_estacao_seca_pct,
    coalesce(f.qtd_focos_amostra_inpe, 0) as qtd_focos_amostra_inpe,
    coalesce(f.frp_medio_amostra_mw, 0.0) as frp_medio_amostra_mw,
    coalesce(s.emissoes_amostra_tco2e, 0.0) as emissoes_amostra_tco2e,
    coalesce(s.remocao_florestal_tco2e, 0.0) as remocao_florestal_tco2e
from res r
left join rf on r.id_municipio = rf.id_municipio
left join focos_agg f on r.id_municipio = f.id_municipio
left join seeg_agg s on r.id_municipio = s.id_municipio
