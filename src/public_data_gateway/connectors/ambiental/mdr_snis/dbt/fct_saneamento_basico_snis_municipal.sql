{{ config(materialized='table') }}

with ae as (
    select
        id_municipio,
        prestador_servico,
        natureza_juridica_prestador,
        atendimento_agua_total_pct,
        atendimento_esgoto_total_pct,
        esgoto_tratado_sobre_consumido_pct,
        indice_perdas_distribuicao_pct
    from {{ ref('stg_snis_agua_esgoto_municipio') }}
),

rs as (
    select
        id_municipio,
        cobertura_coleta_rdo_pct,
        coleta_seletiva_ativa,
        destinacao_adequada_aterro_pct,
        massa_recuperada_reciclagem_t
    from {{ ref('stg_snis_residuos_solidos_municipio') }}
),

fin as (
    select
        id_municipio,
        tarifa_media_agua_esgoto_m3,
        indice_suficiencia_caixa_pct,
        investimento_anual_per_capita_brl,
        investimento_total_milhoes_brl
    from {{ ref('stg_snis_investimentos_tarifas') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.prestador_servico,
    coalesce(ae.natureza_juridica_prestador, 'COMPANHIA_ESTADUAL') as natureza_juridica_prestador,
    r.atendimento_agua_total_pct,
    r.atendimento_esgoto_total_pct,
    r.esgoto_tratado_sobre_consumido_pct,
    r.indice_perdas_distribuicao_pct,
    r.cobertura_coleta_rdo_pct,
    r.investimento_total_milhoes_brl,
    coalesce(rs.coleta_seletiva_ativa, false) as coleta_seletiva_ativa,
    coalesce(rs.destinacao_adequada_aterro_pct, 85.0) as destinacao_adequada_aterro_pct,
    coalesce(rs.massa_recuperada_reciclagem_t, 0.0) as massa_recuperada_reciclagem_t,
    coalesce(f.tarifa_media_agua_esgoto_m3, 5.5) as tarifa_media_agua_esgoto_m3,
    coalesce(f.indice_suficiencia_caixa_pct, 100.0) as indice_suficiencia_caixa_pct,
    coalesce(f.investimento_anual_per_capita_brl, 100.0) as investimento_anual_per_capita_brl
from {{ ref('stg_snis_saneamento_resumo_municipal') }} r
left join ae on r.id_municipio = ae.id_municipio
left join rs on r.id_municipio = rs.id_municipio
left join fin f on r.id_municipio = f.id_municipio
