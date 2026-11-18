{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        qtd_desastres_s2id_5a,
        qtd_ocorrencias_s2id_registradas,
        qtd_decretos_emergencia_5a,
        qtd_decretos_se_5_anos,
        qtd_decretos_calamidade_5_anos,
        total_afetados_5a,
        prejuizos_economicos_mil_brl,
        recursos_federais_liberados_mil_brl,
        qtd_pluviometros_cemaden,
        populacao_area_risco_pct,
        possui_plano_contingencia_ativo,
        alertas_cemaden_12m,
        round(
            least(
                98.0,
                greatest(
                    15.0,
                    100.0
                    - 1.6 * qtd_desastres_s2id_5a
                    - 4.0 * qtd_decretos_calamidade_5_anos
                    - 1.2 * populacao_area_risco_pct
                    + least(10.0, qtd_pluviometros_cemaden * 0.25)
                )
            ),
            2
        ) as score_resiliencia_desastres_sedec
    from {{ ref('fct_desastres_defesa_civil_sedec_municipal') }}
)

select
    *,
    case
        when score_resiliencia_desastres_sedec >= 75.0
            then 'BAIXA_VULNERABILIDADE_DESASTRES'
        when score_resiliencia_desastres_sedec >= 50.0
            then 'RISCO_HIDROCLIMATICO_MODERADO'
        else 'ALTA_RECORRENCIA_CALAMIDADES'
    end as tier_vulnerabilidade_climatica_s2id
from scored
