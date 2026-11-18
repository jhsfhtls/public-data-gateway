{{ config(materialized='table') }}

with ocor as (
    select
        id_municipio,
        count(*) as qtd_tipos_crime_amostra,
        sum(qtd_ocorrencias) as qtd_ocorrencias_amostra_sinesp
    from {{ ref('stg_sinesp_ocorrencias_municipais') }}
    group by id_municipio
),

mvi as (
    select
        id_municipio,
        total_vitimas_mvi,
        taxa_elucidacao_inqueritos_pct
    from {{ ref('stg_sinesp_vitimizacao_mvi_uf') }}
),

cap as (
    select
        id_municipio,
        qtd_delegacias_unidades,
        possui_guarda_municipal_ativa,
        efetivo_seguranca_100k_hab
    from {{ ref('stg_sinesp_capacidade_seguranca') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.taxa_mvi_por_100k_hab,
    r.taxa_roubo_carga_100k_hab,
    r.taxa_furto_roubo_veiculo_100k_hab,
    r.taxa_estelionato_100k_hab,
    r.investimento_seguranca_per_capita_brl,
    coalesce(m.total_vitimas_mvi, 0) as total_vitimas_mvi,
    coalesce(m.taxa_elucidacao_inqueritos_pct, 45.0) as taxa_elucidacao_inqueritos_pct,
    coalesce(c.qtd_delegacias_unidades, 10) as qtd_delegacias_unidades,
    coalesce(c.possui_guarda_municipal_ativa, true) as possui_guarda_municipal_ativa,
    coalesce(c.efetivo_seguranca_100k_hab, 280.0) as efetivo_seguranca_100k_hab,
    coalesce(o.qtd_tipos_crime_amostra, 0) as qtd_tipos_crime_amostra,
    coalesce(o.qtd_ocorrencias_amostra_sinesp, 0) as qtd_ocorrencias_amostra_sinesp
from {{ ref('stg_sinesp_seguranca_resumo_municipal') }} r
left join mvi m on r.id_municipio = m.id_municipio
left join cap c on r.id_municipio = c.id_municipio
left join ocor o on r.id_municipio = o.id_municipio
