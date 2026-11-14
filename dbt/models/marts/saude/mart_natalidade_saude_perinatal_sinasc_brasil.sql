{{ config(materialized='table') }}

with base as (
    select
        *,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(cobertura_prenatal_7_mais_consultas_pct as double) - 45.0) / 45.0) * 100.0
            )
        ) as cob7_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(inicio_prenatal_oportuno_1_trim_pct as double) - 50.0) / 42.0) * 100.0
            )
        ) as ini1_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((24.0 - cast(proporcao_gravidez_adolescencia_pct as double)) / 18.0) * 100.0
            )
        ) as adol_norm,
        least(
            100.0,
            greatest(
                0.0,
                (cast(proporcao_escolaridade_materna_12_anos_mais_pct as double) / 48.0) * 100.0
            )
        ) as esc12_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((13.5 - cast(proporcao_baixo_peso_nascer_pct as double)) / 6.5) * 100.0
            )
        ) as baixo_peso_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((16.0 - cast(proporcao_prematuridade_pct as double)) / 7.5) * 100.0
            )
        ) as premat_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(proporcao_apgar_5min_adequado_7_10_pct as double) - 92.0) / 7.5) * 100.0
            )
        ) as apgar_norm,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(proporcao_partos_hospitalares_pct as double) - 94.0) / 5.8) * 100.0
            )
        ) as hosp_norm
    from {{ ref('fct_sinasc_nascidos_vivos_vitalidade_municipio') }}
),

prenatal_agg as (
    select
        id_municipio,
        max(case when faixa_assistencia_prenatal = 'Adequado_Mais_7_Consultas' then nascidos_vivos_faixa else 0 end) as prenatal_7_mais_consultas_nv,
        max(case when faixa_assistencia_prenatal = 'Intermediario_4_a_6_Consultas' then nascidos_vivos_faixa else 0 end) as prenatal_4_a_6_consultas_nv,
        max(case when faixa_assistencia_prenatal = 'Inadequado_1_a_3_Consultas' then nascidos_vivos_faixa else 0 end) as prenatal_1_a_3_consultas_nv,
        max(case when faixa_assistencia_prenatal = 'Sem_Assistencia_Prenatal' then nascidos_vivos_faixa else 0 end) as sem_assistencia_prenatal_nv
    from {{ ref('fct_sinasc_assistencia_prenatal_parto_municipio') }}
    group by id_municipio
),

scored as (
    select
        b.*,
        coalesce(p.prenatal_7_mais_consultas_nv, 0) as prenatal_7_mais_consultas_nv,
        coalesce(p.prenatal_4_a_6_consultas_nv, 0) as prenatal_4_a_6_consultas_nv,
        coalesce(p.prenatal_1_a_3_consultas_nv, 0) as prenatal_1_a_3_consultas_nv,
        coalesce(p.sem_assistencia_prenatal_nv, 0) as sem_assistencia_prenatal_nv,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * b.cob7_norm
                    + 0.30 * b.ini1_norm
                    + 0.20 * b.adol_norm
                    + 0.15 * b.esc12_norm
                )
            ),
            2
        ) as score_qualidade_assistencia_prenatal_sinasc,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * b.baixo_peso_norm
                    + 0.30 * b.premat_norm
                    + 0.20 * b.apgar_norm
                    + 0.15 * b.hosp_norm
                )
            ),
            2
        ) as score_vitalidade_protecao_perinatal
    from base b
    left join prenatal_agg p on b.id_municipio = p.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_referencia,
    total_nascidos_vivos,
    taxa_bruta_natalidade_1000_hab,
    nascidos_vivos_hospitalares,
    proporcao_partos_hospitalares_pct,
    nascidos_vivos_parto_cesareo,
    proporcao_partos_cesareos_pct,
    cobertura_prenatal_7_mais_consultas_pct,
    inicio_prenatal_oportuno_1_trim_pct,
    idade_media_materna_anos,
    maes_adolescentes_menores_20_anos,
    proporcao_gravidez_adolescencia_pct,
    maes_35_anos_ou_mais,
    proporcao_gestacao_tardia_35_mais_pct,
    proporcao_escolaridade_materna_12_anos_mais_pct,
    nascidos_baixo_peso_menor_2500g,
    proporcao_baixo_peso_nascer_pct,
    nascidos_prematuros_menor_37_sem,
    proporcao_prematuridade_pct,
    proporcao_apgar_5min_adequado_7_10_pct,
    taxa_anomalias_congenitas_10k_nv,
    prenatal_7_mais_consultas_nv,
    prenatal_4_a_6_consultas_nv,
    prenatal_1_a_3_consultas_nv,
    sem_assistencia_prenatal_nv,
    score_qualidade_assistencia_prenatal_sinasc,
    score_vitalidade_protecao_perinatal,
    case
        when score_qualidade_assistencia_prenatal_sinasc >= 80.0
            and score_vitalidade_protecao_perinatal >= 65.0
            then 'REDE_PERINATAL_ALTA_COBERTURA_BAIXO_RISCO'
        when score_qualidade_assistencia_prenatal_sinasc >= 66.0
            then 'REDE_MATERNO_INFANTIL_CONSOLIDADA'
        when score_qualidade_assistencia_prenatal_sinasc >= 48.0
            then 'REDE_PERINATAL_EM_QUALIFICACAO_PRENATAL'
        else 'REDE_PRIORITARIA_VIGILANCIA_GESTACIONAL'
    end as classificacao_rede_materno_perinatal,
    reference_date,
    extracted_at
from scored
