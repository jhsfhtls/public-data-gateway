{{ config(materialized='table') }}

select
    id_municipio,
    sigla_uf,
    nome_ente,
    esfera,
    regiao,
    populacao,
    cnpj_14,
    ano_exercicio,
    is_capital,
    ativo_total_brl,
    passivo_total_brl,
    patrimonio_liquido_brl,
    receita_orcamentaria_realizada_brl,
    despesa_orcamentaria_empenhada_brl,
    resultado_orcamentario_brl,
    receita_corrente_liquida_brl,
    receita_tributaria_propria_brl,
    transferencias_correntes_brl,
    despesa_capital_investimento_brl,
    resultado_primario_brl,
    despesa_total_pessoal_brl,
    despesa_pessoal_rcl_pct,
    limite_prudencial_pessoal_pct,
    limite_maximo_pessoal_pct,
    divida_consolidada_liquida_brl,
    status_alerta_lrf,
    autonomia_tributaria_pct,
    taxa_investimento_rcl_pct,
    total_entregas_monitoradas,
    entregas_homologadas,
    indice_adimplencia_siconfi_pct,
    round(
        least(
            99.0,
            greatest(
                10.0,
                0.45 * least(100.0, greatest(0.0, (54.0 - despesa_pessoal_rcl_pct) * 5.0))
                + 0.30 * least(100.0, greatest(0.0, autonomia_tributaria_pct * 1.4))
                + 0.25 * least(100.0, greatest(0.0, taxa_investimento_rcl_pct * 6.5))
            )
        ),
        2
    ) as score_solidez_contabil_siconfi,
    case
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.45 * least(100.0, greatest(0.0, (54.0 - despesa_pessoal_rcl_pct) * 5.0))
                    + 0.30 * least(100.0, greatest(0.0, autonomia_tributaria_pct * 1.4))
                    + 0.25 * least(100.0, greatest(0.0, taxa_investimento_rcl_pct * 6.5))
                )
            ),
            2
        ) >= 80.0 then 'ALTA_SOLIDEZ_CONTABIL'
        when round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.45 * least(100.0, greatest(0.0, (54.0 - despesa_pessoal_rcl_pct) * 5.0))
                    + 0.30 * least(100.0, greatest(0.0, autonomia_tributaria_pct * 1.4))
                    + 0.25 * least(100.0, greatest(0.0, taxa_investimento_rcl_pct * 6.5))
                )
            ),
            2
        ) >= 60.0 then 'SOLIDEZ_MODERADA'
        else 'ATENCAO_FISCAL'
    end as classificacao_solidez_siconfi
from {{ ref('fct_contabilidade_siconfi_municipal') }}
