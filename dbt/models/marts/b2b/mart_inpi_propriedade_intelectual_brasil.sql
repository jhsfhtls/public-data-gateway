{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        patentes_depositadas_12m,
        marcas_concedidas_12m,
        registros_software_total,
        contratos_tecnologia_ativos,
        patentes_por_100k_hab,
        qtd_naturezas_pi_rpi,
        concessoes_patentes_software_12m,
        desenhos_industriais_vigentes,
        taxa_concessao_marcas_pct,
        participacao_saas_tech_pct,
        ict_universidades_depositantes,
        indicacoes_geograficas_locais,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * least(100.0, (ln(1.0 + patentes_depositadas_12m) / 8.5) * 100.0)
                    + 0.35 * least(100.0, (patentes_por_100k_hab / 65.0) * 100.0)
                    + 0.25 * taxa_concessao_marcas_pct
                )
            ),
            2
        ) as score_inovacao_propriedade_intelectual
    from {{ ref('fct_inpi_propriedade_intelectual_municipal') }}
)

select
    *,
    case
        when score_inovacao_propriedade_intelectual >= 78.0
            then 'POLO_DEEP_TECH_INOVACAO_INPI'
        when score_inovacao_propriedade_intelectual >= 58.0
            then 'ECOSSISTEMA_PI_CONSOLIDADO'
        else 'INOVACAO_TECNOLOGICA_EMERGENTE'
    end as tier_inovacao_tecnologica_inpi
from scored
