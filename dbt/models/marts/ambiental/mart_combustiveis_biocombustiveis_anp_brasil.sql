{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        preco_gasolina_brl,
        preco_etanol_brl,
        preco_diesel_s10_brl,
        razao_etanol_gasolina_pct,
        participacao_biocombustivel_pct,
        volume_diesel_m3,
        qtd_postos_ativos_anp,
        participacao_bandeira_branca_pct,
        taxa_conformidade_pmqc_pct,
        capacidade_armazenagem_m3,
        volume_biocombustivel_m3,
        volume_total_comercializado_m3,
        qtd_produtos_coletados_anp,
        margem_media_revenda_brl,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * least(100.0, participacao_biocombustivel_pct * 1.9)
                    + 0.30 * greatest(0.0, 100.0 - greatest(0.0, razao_etanol_gasolina_pct - 60.0) * 3.0)
                    + 0.25 * taxa_conformidade_pmqc_pct
                )
            ),
            2
        ) as score_competitividade_combustiveis_bio
    from {{ ref('fct_combustiveis_biocombustiveis_anp_municipal') }}
)

select
    *,
    case
        when score_competitividade_combustiveis_bio >= 75.0
            then 'POLO_BIOCOMBUSTIVEL_COMPETITIVO'
        when score_competitividade_combustiveis_bio >= 50.0
            then 'MERCADO_EQUILIBRADO_DERIVADOS'
        else 'ALTA_PRESSAO_CUSTO_LOGISTICO'
    end as tier_matriz_combustiveis_anp
from scored
