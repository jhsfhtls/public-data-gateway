{{ config(materialized='table') }}

with tipo as (
    select
        id_municipio,
        count(*) as qtd_categorias_frota,
        max(case when tipo_veiculo = 'CAMINHAO_PESADO' then participacao_frota_pct else 0.0 end) as participacao_caminhoes_pct
    from {{ ref('stg_senatran_frota_tipo_municipio') }}
    group by id_municipio
),

ev as (
    select
        id_municipio,
        max(participacao_eletrificados_pct) as participacao_eletrificados_pct
    from {{ ref('stg_senatran_frota_combustivel_ev') }}
    group by id_municipio
),

idade as (
    select
        id_municipio,
        idade_media_frota_anos,
        participacao_frota_0a5_anos_pct,
        taxa_motorizacao_100hab
    from {{ ref('stg_senatran_idade_renovacao_frota') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.frota_total_veiculos,
    r.qtd_automoveis,
    r.qtd_motocicletas,
    r.qtd_caminhoes_utilitarios,
    r.qtd_eletrificados_bevs_hevs,
    r.veiculos_novos_emplacados_ano,
    coalesce(ev.participacao_eletrificados_pct, 0.5) as participacao_eletrificados_pct,
    coalesce(id.idade_media_frota_anos, 10.0) as idade_media_frota_anos,
    coalesce(id.participacao_frota_0a5_anos_pct, 32.0) as participacao_frota_0a5_anos_pct,
    coalesce(id.taxa_motorizacao_100hab, 55.0) as taxa_motorizacao_100hab,
    coalesce(t.participacao_caminhoes_pct, 16.0) as participacao_caminhoes_pct,
    coalesce(t.qtd_categorias_frota, 0) as qtd_categorias_frota
from {{ ref('stg_senatran_frota_resumo_municipal') }} r
left join ev on r.id_municipio = ev.id_municipio
left join idade id on r.id_municipio = id.id_municipio
left join tipo t on r.id_municipio = t.id_municipio
