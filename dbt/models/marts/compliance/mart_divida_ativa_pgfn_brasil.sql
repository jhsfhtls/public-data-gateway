{{ config(materialized='table') }}

select
    id_municipio,
    sigla_uf,
    nome_municipio,
    ano_trimestre_ref,
    total_inscricoes_ativas,
    total_devedores_pj,
    valor_total_geral_brl,
    valor_total_previdenciario_brl,
    valor_total_fgts_brl,
    valor_total_consolidado_brl,
    taxa_ajuizamento_pct,
    round(
        least(
            99.0,
            greatest(
                10.0,
                0.55 * least(100.0, (ln(1.0 + valor_total_consolidado_brl) / 18.0) * 100.0)
                + 0.45 * taxa_ajuizamento_pct
            )
        ),
        2
    ) as score_risco_inadimplencia_fiscal,
    case
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.55 * least(100.0, (ln(1.0 + valor_total_consolidado_brl) / 18.0) * 100.0)
                    + 0.45 * taxa_ajuizamento_pct
                )
            ),
            2
        ) >= 75.0 then 'ALTO_RISCO_EXPOSICAO_PGFN'
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.55 * least(100.0, (ln(1.0 + valor_total_consolidado_brl) / 18.0) * 100.0)
                    + 0.45 * taxa_ajuizamento_pct
                )
            ),
            2
        ) >= 45.0 then 'RISCO_MODERADO_PGFN'
        else 'BAIXO_RISCO_PGFN'
    end as classificacao_risco_pgfn
from {{ ref('fct_divida_ativa_pgfn_municipal') }}
