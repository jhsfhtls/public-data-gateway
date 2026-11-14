{{ config(materialized='table') }}

with base as (
    select * from {{ ref('fct_mapbiomas_cobertura_uso_terra_municipio') }}
),

scored as (
    select
        *,
        cast(
            round(
                cast(
                    (
                        0.30 * indice_preservacao_nativa
                        + 0.25 * indice_sustentabilidade_agro
                        + 0.25 * indice_integridade_territorial
                        + 0.20 * (100.0 - taxa_conversao_media_pct)
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_sustentabilidade_mapbiomas
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    ano_colecao,
    cobertura_nativa_pct,
    area_agropecuaria_total_ha,
    area_desmatada_validada_ha,
    indice_preservacao_nativa,
    indice_sustentabilidade_agro,
    indice_integridade_territorial,
    taxa_conversao_media_pct,
    score_sustentabilidade_mapbiomas,
    case
        when score_sustentabilidade_mapbiomas >= 78.0 and cobertura_nativa_pct >= 35.0
            then 'PATRIMONIO_NATIVO_ESTAVEL'
        when score_sustentabilidade_mapbiomas >= 65.0
            then 'TRANSICAO_PRODUTIVA_EQUILIBRADA'
        when score_sustentabilidade_mapbiomas >= 52.0
            then 'EXPANSAO_AGRO_EM_ATENCAO'
        else 'PRESSAO_CONVERSAO_CRITICA'
    end as tier_sustentabilidade_mapbiomas,
    reference_date
from scored
