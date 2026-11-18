{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        total_beneficiarios_med_odo,
        taxa_cobertura_privada_pct,
        idss_medio_ponderado,
        sinistralidade_assistencial_pct,
        receita_contraprestacoes_mil_brl,
        qtd_segmentacoes_ans,
        operadoras_atuantes_count,
        ticket_medio_mensal_brl,
        reclamacoes_nip_10k_benef,
        provisoes_tecnicas_cobertura_pct,
        indice_solvencia_ans_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * (idss_medio_ponderado * 100.0)
                    + 0.30 * least(100.0, taxa_cobertura_privada_pct * 1.6)
                    + 0.25 * greatest(0.0, 100.0 - abs(sinistralidade_assistencial_pct - 75.0) * 2.0)
                )
            ),
            2
        ) as score_maturidade_saude_suplementar_ans
    from {{ ref('fct_ans_saude_suplementar_municipal') }}
)

select
    *,
    case
        when score_maturidade_saude_suplementar_ans >= 75.0
            then 'POLO_ALTA_COBERTURA_ANS'
        when score_maturidade_saude_suplementar_ans >= 55.0
            then 'MERCADO_SUPLEMENTAR_EQUILIBRADO'
        else 'BAIXA_PENETRACAO_PRIVADA'
    end as tier_saude_suplementar_ans
from scored
