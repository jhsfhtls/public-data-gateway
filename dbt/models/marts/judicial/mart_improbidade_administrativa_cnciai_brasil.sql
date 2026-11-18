{{ config(materialized='table') }}

select
    id_municipio,
    sigla_uf,
    nome_municipio,
    total_condenacoes_ativas,
    total_proibidos_contratar_pj,
    total_suspensao_direitos_politicos,
    valor_total_ressarcimento_multa_brl,
    taxa_proibicao_contratar_pct,
    maior_prazo_proibicao_anos,
    round(
        least(
            99.0,
            greatest(
                10.0,
                0.50 * least(100.0, (ln(1.0 + valor_total_ressarcimento_multa_brl) / 16.5) * 100.0)
                + 0.50 * taxa_proibicao_contratar_pct
            )
        ),
        2
    ) as score_exposicao_improbidade_cnciai,
    case
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.50 * least(100.0, (ln(1.0 + valor_total_ressarcimento_multa_brl) / 16.5) * 100.0)
                    + 0.50 * taxa_proibicao_contratar_pct
                )
            ),
            2
        ) >= 70.0 then 'ALTA_EXPOSICAO_CNCIAI'
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.50 * least(100.0, (ln(1.0 + valor_total_ressarcimento_multa_brl) / 16.5) * 100.0)
                    + 0.50 * taxa_proibicao_contratar_pct
                )
            ),
            2
        ) >= 40.0 then 'MODERADA_EXPOSICAO_CNCIAI'
        else 'BAIXA_EXPOSICAO_CNCIAI'
    end as classificacao_risco_improbidade
from {{ ref('fct_improbidade_cnciai_municipal') }}
