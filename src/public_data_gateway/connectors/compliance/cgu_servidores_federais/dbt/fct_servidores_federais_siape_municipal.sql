{{ config(materialized='table') }}

with lot as (
    select
        id_municipio,
        count(*) as qtd_regimes_lotacao_siape
    from {{ ref('stg_siape_servidores_ativos_municipio') }}
    group by id_municipio
),

fol as (
    select
        id_municipio,
        remuneracao_teto_pct,
        adicional_funcao_das_fce_mil_brl
    from {{ ref('stg_siape_remuneracao_folha_orgao') }}
),

comp as (
    select
        id_municipio,
        servidores_dedicacao_exclusiva_pct,
        alertas_acumulo_ilicito,
        servidores_cedidos_count,
        indice_integridade_siape_pct
    from {{ ref('stg_cgu_acumulo_cargos_conflitos') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.total_servidores_federais_siape,
    r.folha_mensal_bruta_siape_mil_brl,
    r.remuneracao_media_bruta_brl,
    r.pct_cargos_comissionados_das,
    coalesce(l.qtd_regimes_lotacao_siape, 0) as qtd_regimes_lotacao_siape,
    coalesce(f.remuneracao_teto_pct, 1.5) as remuneracao_teto_pct,
    coalesce(f.adicional_funcao_das_fce_mil_brl, 0.0) as adicional_funcao_das_fce_mil_brl,
    coalesce(c.servidores_dedicacao_exclusiva_pct, 90.0) as servidores_dedicacao_exclusiva_pct,
    coalesce(c.alertas_acumulo_ilicito, 0) as alertas_acumulo_ilicito,
    coalesce(c.servidores_cedidos_count, 0) as servidores_cedidos_count,
    coalesce(c.indice_integridade_siape_pct, 92.0) as indice_integridade_siape_pct
from {{ ref('stg_siape_servidores_resumo_municipal') }} r
left join lot l on r.id_municipio = l.id_municipio
left join fol f on r.id_municipio = f.id_municipio
left join comp c on r.id_municipio = c.id_municipio
