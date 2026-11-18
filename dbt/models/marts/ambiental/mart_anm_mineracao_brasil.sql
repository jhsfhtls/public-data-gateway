{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        processos_ativos_sigmine,
        area_total_titulada_ha,
        arrecadacao_cfem_total_mil_brl,
        indice_conformidade_sigbm_pct,
        participacao_substancias_criticas_pct,
        qtd_registros_amostra_sigmine,
        cfem_amostra_processos_mil_brl,
        substancia_principal,
        valor_operacao_mineral_mil_brl,
        aliquota_media_cfem_pct,
        qtd_barragens_sigbm,
        barragens_paebm_emergencia,
        nivel_dano_potencial_associado,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.50 * indice_conformidade_sigbm_pct
                    + 0.25 * least(100.0, (ln(1.0 + arrecadacao_cfem_total_mil_brl) / 14.5) * 100.0)
                    + 0.25 * greatest(0.0, 100.0 - barragens_paebm_emergencia * 35.0)
                )
            ),
            2
        ) as score_maturidade_mineral_esg
    from {{ ref('fct_anm_mineracao_municipal') }}
)

select
    *,
    case
        when score_maturidade_mineral_esg >= 78.0
            then 'POLO_MINERAL_ESG_CONFORME'
        when score_maturidade_mineral_esg >= 58.0
            then 'ATIVIDADE_EXTRATIVA_REGULAR'
        else 'ALERTA_FISCALIZACAO_BARRAGENS'
    end as tier_mineracao_sustentavel_anm
from scored
