{{ config(materialized='table') }}

with base as (
    select * from {{ ref('fct_sicar_regularizacao_fundiaria_municipio') }}
),

scored as (
    select
        *,
        cast(
            round(
                cast(
                    (
                        0.25 * indice_cobertura_cadastral_car
                        + 0.30 * indice_governanca_pra
                        + 0.25 * indice_qualidade_geoespacial_car
                        + 0.20 * (100.0 - taxa_passivo_media_pct)
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_conformidade_sicar
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    ano_referencia,
    qtd_imoveis_car_ativos,
    area_cadastrada_car_ha,
    imoveis_livres_embargo_ibama_pct,
    indice_cobertura_cadastral_car,
    indice_governanca_pra,
    indice_qualidade_geoespacial_car,
    taxa_passivo_media_pct,
    score_conformidade_sicar,
    case
        when score_conformidade_sicar >= 78.0 and imoveis_livres_embargo_ibama_pct >= 94.0
            then 'SICAR_CONFORMIDADE_EUDR_OURO'
        when score_conformidade_sicar >= 65.0
            then 'CAR_REGULARIZADO_EM_CONSOLIDACAO'
        when score_conformidade_sicar >= 52.0
            then 'PASSIVO_PRA_EM_MONITORAMENTO'
        else 'RISCO_SOBREPOSICAO_OU_PASSIVO'
    end as tier_conformidade_sicar,
    reference_date
from scored
