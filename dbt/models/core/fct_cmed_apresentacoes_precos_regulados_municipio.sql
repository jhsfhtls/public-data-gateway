{{ config(materialized='table') }}

with apres as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_mes_competencia,
        cnpj_14,
        laboratorio_detentor_referencia,
        total_apresentacoes_ativas_cmed,
        apresentacoes_genericos,
        apresentacoes_similares,
        apresentacoes_referencia_novos_biologicos,
        participacao_genericos_similares_pct,
        apresentacoes_tarja_vermelha_preta_pct,
        apresentacoes_hospitalares_restritas_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_cmed_apresentacoes_registradas_municipio') }}
),

cap as (
    select
        id_municipio,
        apresentacoes_sujeitas_cap_obrigatorio,
        proporcao_apresentacoes_cap_pct,
        coeficiente_cap_vigente_pct,
        preco_fabrica_medio_ponderado_brl,
        pmc_medio_ponderado_aliquota_uf_brl,
        pmvg_medio_ponderado_brl,
        economia_potencial_pmvg_sobre_pmc_pct,
        conformidade_teto_pmvg_compras_sus_pct
    from {{ ref('stg_cmed_compras_publicas_cap_pmvg') }}
),

trib as (
    select
        id_municipio,
        aliquota_icms_modal_uf_pct,
        apresentacoes_lista_positiva_piscofins,
        apresentacoes_lista_negativa_piscofins,
        apresentacoes_lista_neutra_piscofins,
        participacao_lista_positiva_piscofins_pct,
        apresentacoes_comercializacao_2024_ativa_pct,
        spread_medio_pmc_sobre_pf_pct
    from {{ ref('stg_cmed_regime_tributario_icms_piscofins') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    a.id_municipio,
    a.nome_municipio,
    a.sigla_uf,
    a.id_uf,
    coalesce(m.nome_uf, a.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    a.ano_mes_competencia,
    a.cnpj_14,
    a.laboratorio_detentor_referencia,
    a.total_apresentacoes_ativas_cmed,
    a.apresentacoes_genericos,
    a.apresentacoes_similares,
    a.apresentacoes_referencia_novos_biologicos,
    a.participacao_genericos_similares_pct,
    a.apresentacoes_tarja_vermelha_preta_pct,
    a.apresentacoes_hospitalares_restritas_pct,
    c.apresentacoes_sujeitas_cap_obrigatorio,
    c.proporcao_apresentacoes_cap_pct,
    c.coeficiente_cap_vigente_pct,
    c.preco_fabrica_medio_ponderado_brl,
    c.pmc_medio_ponderado_aliquota_uf_brl,
    c.pmvg_medio_ponderado_brl,
    c.economia_potencial_pmvg_sobre_pmc_pct,
    c.conformidade_teto_pmvg_compras_sus_pct,
    t.aliquota_icms_modal_uf_pct,
    t.apresentacoes_lista_positiva_piscofins,
    t.apresentacoes_lista_negativa_piscofins,
    t.apresentacoes_lista_neutra_piscofins,
    t.participacao_lista_positiva_piscofins_pct,
    t.apresentacoes_comercializacao_2024_ativa_pct,
    t.spread_medio_pmc_sobre_pf_pct,
    a.reference_date,
    a.extracted_at
from apres a
inner join cap c on a.id_municipio = c.id_municipio
inner join trib t on a.id_municipio = t.id_municipio
left join municipios m on a.id_municipio = m.id_municipio
