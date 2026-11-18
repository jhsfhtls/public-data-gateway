{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        arrecadacao_federal_total_mi_brl,
        contribuintes_pj_ativos_municipio,
        inscritos_divida_ativa_pgfn_mi_brl,
        repasses_fpm_anual_mi_brl,
        crescimento_real_arrecadacao_pct,
        qtd_blocos_arrecadacao_rfb,
        soma_blocos_arrecadacao_mi_brl,
        estoque_divida_ativa_uf_mi_brl,
        devedores_previdenciarios_pj,
        recuperacao_creditos_anual_mi_brl,
        indice_recuperacao_pgfn_pct,
        transferencias_ipi_exportacao_mi_brl,
        cota_simples_nacional_munic_mi_brl,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * least(100.0, (ln(1.0 + arrecadacao_federal_total_mi_brl) / 13.5) * 100.0)
                    + 0.30 * least(100.0, (indice_recuperacao_pgfn_pct / 16.0) * 100.0)
                    + 0.25 * least(100.0, greatest(20.0, 50.0 + crescimento_real_arrecadacao_pct * 5.0))
                )
            ),
            2
        ) as score_dinamica_arrecadacao_rfb
    from {{ ref('fct_rfb_arrecadacao_municipal') }}
)

select
    *,
    case
        when score_dinamica_arrecadacao_rfb >= 78.0
            then 'POLO_ARRECADACAO_ALTA_CONFORMIDADE'
        when score_dinamica_arrecadacao_rfb >= 60.0
            then 'BASE_TRIBUTARIA_CONSOLIDADA'
        else 'EXPANSAO_BASE_FISCAL_REGIONAL'
    end as tier_capacidade_fiscal_rfb
from scored
