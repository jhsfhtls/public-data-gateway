{{ config(materialized='table') }}

with tx_agg as (
    select
        id_municipio,
        sum(quantidade_atos_registrados_ano) as total_atos_registrais_imobiliarios_ano,
        round(sum(volume_financeiro_declarado_milhoes_brl), 2) as volume_total_transacionado_milhoes_brl
    from {{ ref('fct_onr_transacoes_natureza_municipio') }}
    group by id_municipio
),

base as (
    select
        f.*,
        tx.total_atos_registrais_imobiliarios_ano,
        tx.volume_total_transacionado_milhoes_brl,
        least(
            100.0,
            greatest(
                0.0,
                ((10.0 - f.prazo_medio_qualificacao_registral_dias) / 8.5) * 100.0
            )
        ) as s_sla,
        least(
            100.0,
            greatest(
                0.0,
                (
                    (
                        (
                            cast(f.novas_matriculas_abertas_ano as double)
                            / greatest(1.0, cast(f.total_matriculas_ativas_cnm as double))
                        )
                        * 100.0
                    )
                    / 4.5
                )
                * 100.0
            )
        ) as s_dinamismo,
        least(100.0, greatest(0.0, f.participacao_sfi_sbpe_pmcmv_pct)) as s_sfi,
        least(
            100.0,
            greatest(
                0.0,
                ((6.0 - f.taxa_execucao_garantia_fiduciaria_pct) / 5.0) * 100.0
            )
        ) as s_adimplencia
    from {{ ref('fct_onr_acervo_garantias_serp_municipio') }} f
    inner join tx_agg tx on f.id_municipio = tx.id_municipio
),

scored as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.30 * taxa_conversao_cnm_digital_pct
                    + 0.30 * indice_integracao_serp_saec_pct
                    + 0.25 * proporcao_titulos_nato_digitais_extrato_pct
                    + 0.15 * s_sla
                )
            ),
            2
        ) as score_maturidade_digital_srei_serp,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * s_dinamismo
                    + 0.35 * s_sfi
                    + 0.30 * s_adimplencia
                )
            ),
            2
        ) as score_liquidez_seguranca_imobiliaria
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    cep_8,
    competencia_referencia,
    serventias_ri_integradas_srei,
    total_matriculas_ativas_cnm,
    matriculas_urbanas_ativas,
    matriculas_rurais_georreferenciadas,
    taxa_conversao_cnm_digital_pct,
    novas_matriculas_abertas_ano,
    certidoes_digitais_srei_ano,
    visualizacoes_matricula_online_ano,
    total_atos_registrais_imobiliarios_ano,
    volume_total_transacionado_milhoes_brl,
    contratos_alienacao_fiduciaria_ano,
    volume_credito_imobiliario_garantido_milhoes_brl,
    participacao_sfi_sbpe_pmcmv_pct,
    contratos_home_equity_cgi_ano,
    intimacoes_purgacao_mora_fiduciaria_ano,
    consolidacoes_propriedade_inadimplencia_ano,
    taxa_execucao_garantia_fiduciaria_pct,
    indice_integracao_serp_saec_pct,
    proporcao_titulos_nato_digitais_extrato_pct,
    prazo_medio_qualificacao_registral_dias,
    prazo_medio_certidao_eletronica_horas,
    ordens_indisponibilidade_cnib_processadas_ano,
    penhoras_online_arisp_srei_ano,
    taxa_nota_devolutiva_exigencia_pct,
    score_maturidade_digital_srei_serp,
    score_liquidez_seguranca_imobiliaria,
    case
        when score_maturidade_digital_srei_serp >= 86.0
            then 'POLO_REGISTRAL_DIGITAL_ALTA_LIQUIDEZ_SERP'
        when score_maturidade_digital_srei_serp >= 77.0
            then 'MERCADO_IMOBILIARIO_CONSOLIDADO_INTEGRADO_SREI'
        when score_maturidade_digital_srei_serp >= 68.0
            then 'REDE_REGISTRAL_EM_TRANSICAO_ELETRONICA'
        else 'TERRITORIO_PRIORITARIO_REGULARIZACAO_FUNDIARIA'
    end as classificacao_mercado_registral_imobiliario,
    reference_date,
    extracted_at
from scored
