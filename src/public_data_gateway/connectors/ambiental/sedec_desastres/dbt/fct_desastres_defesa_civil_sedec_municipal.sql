{{ config(materialized='table') }}

with ocor as (
    select
        id_municipio,
        count(*) as qtd_ocorrencias_s2id_registradas
    from {{ ref('stg_sedec_ocorrencias_desastres') }}
    group by id_municipio
),

dec as (
    select
        id_municipio,
        qtd_decretos_se_5_anos,
        qtd_decretos_calamidade_5_anos,
        recursos_federais_liberados_mil_brl
    from {{ ref('stg_sedec_decretos_reconhecimento') }}
),

cem as (
    select
        id_municipio,
        qtd_pluviometros_cemaden,
        populacao_area_risco_pct,
        possui_plano_contingencia_ativo
    from {{ ref('stg_sedec_cemaden_monitoramento') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.qtd_desastres_s2id_5a,
    coalesce(o.qtd_ocorrencias_s2id_registradas, 0) as qtd_ocorrencias_s2id_registradas,
    r.qtd_decretos_emergencia_5a,
    coalesce(d.qtd_decretos_se_5_anos, 0) as qtd_decretos_se_5_anos,
    coalesce(d.qtd_decretos_calamidade_5_anos, 0) as qtd_decretos_calamidade_5_anos,
    r.total_afetados_5a,
    r.prejuizos_economicos_mil_brl,
    coalesce(d.recursos_federais_liberados_mil_brl, 0.0) as recursos_federais_liberados_mil_brl,
    coalesce(c.qtd_pluviometros_cemaden, 0) as qtd_pluviometros_cemaden,
    coalesce(c.populacao_area_risco_pct, 0.0) as populacao_area_risco_pct,
    coalesce(c.possui_plano_contingencia_ativo, true) as possui_plano_contingencia_ativo,
    r.alertas_cemaden_12m
from {{ ref('stg_sedec_desastres_resumo_municipal') }} r
left join ocor o on r.id_municipio = o.id_municipio
left join dec d on r.id_municipio = d.id_municipio
left join cem c on r.id_municipio = c.id_municipio
