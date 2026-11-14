{{ config(materialized='table') }}

with vigil as (
    select
        *,
        least(
            100.0,
            greatest(
                0.0,
                ((12.0 - cast(proporcao_causas_mal_definidas_pct as double)) / 10.5) * 100.0
            )
        ) as definicao_norm,
        least(
            100.0,
            greatest(0.0, cast(proporcao_obitos_infantis_investigados_pct as double))
        ) as investigacao_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((20.0 - cast(taxa_mortalidade_infantil_1000_nv as double)) / 12.5) * 100.0
            )
        ) as sobrevivencia_inf_norm,
        least(
            100.0,
            greatest(
                0.0,
                (cast(taxa_mortalidade_causas_externas_100k_hab as double) / 95.0) * 100.0
            )
        ) as externas_norm,
        least(
            100.0,
            greatest(
                0.0,
                (cast(anos_potenciais_vida_perdidos_1000_hab as double) / 75.0) * 100.0
            )
        ) as apvp_norm,
        least(
            100.0,
            greatest(
                0.0,
                (cast(taxa_mortalidade_infantil_1000_nv as double) / 18.0) * 100.0
            )
        ) as infantil_norm
    from {{ ref('fct_sim_mortalidade_vigilancia_municipio') }}
),

cid10_agg as (
    select
        id_municipio,
        max(case when capitulo_cid10 = 'Cap_IX_Doencas_Aparelho_Circulatorio' then obitos_capitulo else 0 end) as circulatorio_obitos,
        max(case when capitulo_cid10 = 'Cap_II_Neoplasias_Tumores' then obitos_capitulo else 0 end) as neoplasias_obitos,
        max(case when capitulo_cid10 = 'Cap_X_Doencas_Aparelho_Respiratorio' then obitos_capitulo else 0 end) as respiratorio_obitos,
        max(case when capitulo_cid10 = 'Cap_XX_Causas_Externas_Morbimortalidade' then obitos_capitulo else 0 end) as causas_externas_obitos
    from {{ ref('fct_sim_mortalidade_capitulos_cid10_municipio') }}
    group by id_municipio
),

scored as (
    select
        v.*,
        coalesce(c.circulatorio_obitos, 0) as circulatorio_obitos,
        coalesce(c.neoplasias_obitos, 0) as neoplasias_obitos,
        coalesce(c.respiratorio_obitos, 0) as respiratorio_obitos,
        coalesce(c.causas_externas_obitos, 0) as causas_externas_obitos,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * v.definicao_norm
                    + 0.35 * v.investigacao_norm
                    + 0.25 * v.sobrevivencia_inf_norm
                )
            ),
            2
        ) as score_qualidade_vigilancia_obitos_sim,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * v.externas_norm
                    + 0.35 * v.apvp_norm
                    + 0.25 * v.infantil_norm
                )
            ),
            2
        ) as score_carga_mortalidade_evitavel
    from vigil v
    left join cid10_agg c on v.id_municipio = c.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_referencia,
    total_obitos_registrados,
    taxa_bruta_mortalidade_1000_hab,
    obitos_hospitalares,
    proporcao_obitos_hospitalares_pct,
    obitos_causas_mal_definidas,
    proporcao_causas_mal_definidas_pct,
    obitos_infantis_menores_1_ano,
    obitos_neonatais_precoce_tardio,
    taxa_mortalidade_infantil_1000_nv,
    obitos_maternos_declarados,
    razao_mortalidade_materna_100k_nv,
    proporcao_obitos_infantis_investigados_pct,
    obitos_prematuros_dcnt_30_69_anos,
    proporcao_mortalidade_prematura_dcnt_pct,
    taxa_mortalidade_causas_externas_100k_hab,
    taxa_mortalidade_acidentes_transporte_100k_hab,
    anos_potenciais_vida_perdidos_1000_hab,
    circulatorio_obitos,
    neoplasias_obitos,
    respiratorio_obitos,
    causas_externas_obitos,
    score_qualidade_vigilancia_obitos_sim,
    score_carga_mortalidade_evitavel,
    case
        when score_qualidade_vigilancia_obitos_sim >= 85.0 and taxa_mortalidade_infantil_1000_nv <= 10.5
            then 'TRANSICAO_AVANCADA_ALTA_VIGILANCIA_SIM'
        when score_qualidade_vigilancia_obitos_sim >= 72.0 and taxa_mortalidade_infantil_1000_nv <= 12.8
            then 'TRANSICAO_CONSOLIDADA_PREDOMINIO_DCNT'
        when score_qualidade_vigilancia_obitos_sim >= 58.0
            then 'TRANSICAO_INTERMEDIARIA_DUPLA_CARGA'
        else 'ALTA_PRESSAO_CAUSAS_EXTERNAS_E_EVITAVEIS'
    end as perfil_transicao_epidemiologica_mortalidade,
    reference_date,
    extracted_at
from scored
