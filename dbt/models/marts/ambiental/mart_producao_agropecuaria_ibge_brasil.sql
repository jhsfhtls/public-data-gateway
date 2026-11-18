{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        bioma_predominante,
        area_colhida_pam_ha,
        valor_producao_pam_mil_brl,
        efetivo_rebanho_bovino_cabecas,
        valor_producao_pecuaria_mil_brl,
        valor_producao_silvicultura_pevs_mil_brl,
        valor_bruto_agro_total_mil_brl,
        produtividade_media_pam_kg_ha,
        area_floresta_plantada_eucalipto_pinus_ha,
        producao_madeira_tora_m3,
        qtd_culturas_pam_amostra,
        quantidade_produzida_amostra_t,
        producao_leite_mil_litros,
        round(
            least(
                98.0,
                greatest(
                    15.0,
                    0.45 * least(100.0, (ln(1.0 + valor_bruto_agro_total_mil_brl / 1000.0) / 8.5) * 100.0)
                    + 0.30 * least(100.0, (ln(1.0 + area_colhida_pam_ha) / 13.5) * 100.0)
                    + 0.25 * least(100.0, (ln(1.0 + efetivo_rebanho_bovino_cabecas) / 13.2) * 100.0)
                )
            ),
            2
        ) as score_potencia_agropecuaria_ibge
    from {{ ref('fct_producao_agropecuaria_ibge_municipal') }}
)

select
    *,
    case
        when score_potencia_agropecuaria_ibge >= 75.0
            then 'POLO_AGROPECUARIO_ALTA_DENSIDADE'
        when score_potencia_agropecuaria_ibge >= 50.0
            then 'PRODUCAO_AGRO_DIVERSIFICADA'
        else 'BAIXA_VOCACAO_AGROPECUARIA'
    end as classificacao_vocacao_agro_ibge
from scored
