{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        bioma_predominante,
        qtd_contratos_sicor,
        valor_custeio_sicor_brl,
        valor_investimento_sicor_brl,
        valor_comercializacao_sicor_brl,
        valor_total_sicor_brl,
        area_financiada_total_ha,
        ticket_medio_contrato_brl,
        participacao_pronaf_pct,
        cobertura_proagro_sicor_pct,
        participacao_abc_renovagro_pct,
        taxa_glosa_res_bcb_140_pct,
        qtd_contratos_custeio_amostra,
        valor_custeio_amostra_brl,
        qtd_contratos_investimento_amostra,
        valor_investimento_amostra_brl,
        round(
            least(
                98.0,
                greatest(
                    15.0,
                    0.45 * least(100.0, (ln(1.0 + valor_total_sicor_brl / 1000000.0) / 8.0) * 100.0)
                    + 0.30 * least(100.0, cobertura_proagro_sicor_pct)
                    + 0.25 * least(100.0, participacao_abc_renovagro_pct * 2.5)
                )
            ),
            2
        ) as score_dinamismo_credito_rural_sicor
    from {{ ref('fct_credito_rural_sicor_municipal') }}
)

select
    *,
    case
        when score_dinamismo_credito_rural_sicor >= 75.0
            then 'POLO_AGRONEGOCIO_CREDITO_FORTE'
        when score_dinamismo_credito_rural_sicor >= 50.0
            then 'EXPANSAO_RURAL_MODERADA'
        else 'CREDITO_RURAL_REDUZIDO'
    end as classificacao_credito_rural_sicor
from scored
