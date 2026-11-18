{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        total_ofertas_rcvm160,
        volume_captado_rcvm160_mil_brl,
        assessores_investimento_ativos,
        administradores_ativos_count,
        captacao_per_capita_brl,
        qtd_classes_emissao_cvm,
        prazo_maximo_emissao_anos,
        gestoras_fii_fip_count,
        patrimonio_administrado_mi_brl,
        taxa_conformidade_informes_pct,
        sociedades_aai_registradas,
        analistas_consultores_cvm,
        alertas_sancionatorios_pasm_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * taxa_conformidade_informes_pct
                    + 0.35 * least(100.0, (ln(1.0 + volume_captado_rcvm160_mil_brl) / 21.0) * 100.0)
                    + 0.20 * least(100.0, (ln(1.0 + assessores_investimento_ativos) / 10.5) * 100.0)
                )
            ),
            2
        ) as score_sofisticacao_mercado_cvm
    from {{ ref('fct_cvm_mercado_extra_municipal') }}
)

select
    *,
    case
        when score_sofisticacao_mercado_cvm >= 76.0
            then 'HUB_MERCADO_CAPITAIS_CVM'
        when score_sofisticacao_mercado_cvm >= 58.0
            then 'MERCADO_FINANCEIRO_AVANCADO'
        else 'ECOSISTEMA_CAPTACAO_EMERGENTE'
    end as tier_ecossistema_mercado_capitais
from scored
