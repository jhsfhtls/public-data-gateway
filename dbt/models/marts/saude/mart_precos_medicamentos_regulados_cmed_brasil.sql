{{ config(materialized='table') }}

with base as (
    select
        *
    from {{ ref('fct_cmed_apresentacoes_precos_regulados_municipio') }}
),

classes_agg as (
    select
        id_municipio,
        max(case when classe_terapeutica_atc = 'Antineoplasicos_e_Imunomoduladores_L' then apresentacoes_classe else 0 end) as apresentacoes_onco_imuno_classe_l,
        max(case when classe_terapeutica_atc = 'Antineoplasicos_e_Imunomoduladores_L' then preco_fabrica_pf_medio_brl else 0.0 end) as pf_medio_antineoplasicos_brl,
        max(case when classe_terapeutica_atc = 'Sistema_Cardiovascular_e_Metabolico_C_A' then apresentacoes_classe else 0 end) as apresentacoes_cardio_metabolico_classe_ca,
        max(case when classe_terapeutica_atc = 'Sistema_Cardiovascular_e_Metabolico_C_A' then preco_fabrica_pf_medio_brl else 0.0 end) as pf_medio_cardiovascular_brl
    from {{ ref('fct_cmed_precos_classe_terapeutica_municipio') }}
    group by id_municipio
),

scored as (
    select
        b.*,
        coalesce(c.apresentacoes_onco_imuno_classe_l, 0) as apresentacoes_onco_imuno_classe_l,
        coalesce(c.pf_medio_antineoplasicos_brl, 0.0) as pf_medio_antineoplasicos_brl,
        coalesce(c.apresentacoes_cardio_metabolico_classe_ca, 0) as apresentacoes_cardio_metabolico_classe_ca,
        coalesce(c.pf_medio_cardiovascular_brl, 0.0) as pf_medio_cardiovascular_brl,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    cast(b.participacao_genericos_similares_pct as double) * 0.45
                    + cast(b.participacao_lista_positiva_piscofins_pct as double) * 0.30
                    + cast(b.apresentacoes_comercializacao_2024_ativa_pct as double) * 0.25
                )
            ),
            2
        ) as score_competitividade_genericos_cmed,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    cast(b.conformidade_teto_pmvg_compras_sus_pct as double) * 0.45
                    + cast(b.economia_potencial_pmvg_sobre_pmc_pct as double) * 0.35
                    + cast(b.proporcao_apresentacoes_cap_pct as double) * 0.20
                )
            ),
            2
        ) as score_eficiencia_regulatoria_pmvg_cap
    from base b
    left join classes_agg c on b.id_municipio = c.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
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
    apresentacoes_sujeitas_cap_obrigatorio,
    proporcao_apresentacoes_cap_pct,
    coeficiente_cap_vigente_pct,
    preco_fabrica_medio_ponderado_brl,
    pmc_medio_ponderado_aliquota_uf_brl,
    pmvg_medio_ponderado_brl,
    economia_potencial_pmvg_sobre_pmc_pct,
    conformidade_teto_pmvg_compras_sus_pct,
    aliquota_icms_modal_uf_pct,
    apresentacoes_lista_positiva_piscofins,
    apresentacoes_lista_negativa_piscofins,
    apresentacoes_lista_neutra_piscofins,
    participacao_lista_positiva_piscofins_pct,
    apresentacoes_comercializacao_2024_ativa_pct,
    spread_medio_pmc_sobre_pf_pct,
    apresentacoes_onco_imuno_classe_l,
    pf_medio_antineoplasicos_brl,
    apresentacoes_cardio_metabolico_classe_ca,
    pf_medio_cardiovascular_brl,
    score_competitividade_genericos_cmed,
    score_eficiencia_regulatoria_pmvg_cap,
    case
        when score_eficiencia_regulatoria_pmvg_cap >= 68.0
            and score_competitividade_genericos_cmed >= 66.0
            then 'POLO_FARMACEUTICO_ALTA_CONFORMIDADE_CMED'
        when score_eficiencia_regulatoria_pmvg_cap >= 62.0
            and score_competitividade_genericos_cmed >= 60.0
            then 'REDE_REGULADA_CONSOLIDADA_CAP_PMVG'
        when score_eficiencia_regulatoria_pmvg_cap >= 56.0
            then 'MERCADO_EM_QUALIFICACAO_COMPRAS_PUBLICAS'
        else 'POLO_PRIORITARIO_AUDITORIA_PRECOS_CMED'
    end as classificacao_polo_farmaceutico_cmed,
    reference_date,
    extracted_at
from scored
