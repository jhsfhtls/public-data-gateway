{{ config(materialized='table') }}

with cine_agg as (
    select
        id_municipio,
        sum(cursos_graduacao_ativos) as total_cursos_graduacao,
        max(
            case
                when area_cine_rotulo = 'STEM_Engenharia_TI'
                    then participacao_matriculas_area_pct
                else 0.0
            end
        ) as proporcao_matriculas_stem_ti_pct,
        sum(
            case
                when area_cine_rotulo = 'STEM_Engenharia_TI'
                    then matriculas_ativas_area
                else 0
            end
        ) as matriculas_stem_ti,
        sum(
            case
                when area_cine_rotulo = 'Saude_Bem_Estar'
                    then matriculas_ativas_area
                else 0
            end
        ) as matriculas_saude_bem_estar,
        round(avg(conceito_enade_medio_area), 2) as conceito_enade_medio_municipio
    from {{ ref('fct_censo_superior_cursos_areas_cine_municipio') }}
    group by id_municipio
),

base as (
    select
        f.*,
        c.total_cursos_graduacao,
        c.proporcao_matriculas_stem_ti_pct,
        c.matriculas_stem_ti,
        c.matriculas_saude_bem_estar,
        c.conceito_enade_medio_municipio,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * ((f.igc_medio_continuo_municipio - 1.0) / 4.0 * 100.0)
                    + 0.35 * least(100.0, (f.proporcao_doutores_pct / 70.0) * 100.0)
                    + 0.30 * least(100.0, (f.proporcao_regime_tempo_integral_de_pct / 75.0) * 100.0)
                )
            ),
            2
        ) as score_excelencia_academica_pesquisa_ies,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * least(100.0, (f.taxa_conclusao_acumulada_tcg_pct / 70.0) * 100.0)
                    + 0.35 * least(100.0, (c.proporcao_matriculas_stem_ti_pct / 35.0) * 100.0)
                    + 0.30 * greatest(0.0, 100.0 - (f.taxa_desistencia_anual_tda_pct / 30.0) * 100.0)
                )
            ),
            2
        ) as score_formacao_capital_humano_superior
    from {{ ref('fct_censo_superior_ies_docentes_fluxo_municipio') }} f
    inner join cine_agg c on f.id_municipio = c.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    cnpj_14,
    ano_censo,
    total_ies_ativas,
    universidades_e_centros_universitarios,
    ies_rede_publica,
    ies_rede_privada,
    proporcao_ies_publicas_pct,
    total_cursos_graduacao,
    total_matriculas_graduacao,
    matriculas_presenciais,
    matriculas_ead_polo,
    proporcao_matriculas_ead_pct,
    ingressantes_anuais_total,
    concluintes_anuais_total,
    proporcao_matriculas_stem_ti_pct,
    matriculas_stem_ti,
    matriculas_saude_bem_estar,
    total_docentes_exercicio_ies,
    docentes_doutores,
    docentes_mestres,
    proporcao_doutores_pct,
    proporcao_mestres_doutores_stricto_sensu_pct,
    proporcao_regime_tempo_integral_de_pct,
    razao_alunos_por_docente_equivalente,
    taxa_conclusao_acumulada_tcg_pct,
    taxa_desistencia_anual_tda_pct,
    igc_medio_continuo_municipio,
    conceito_enade_medio_municipio,
    score_excelencia_academica_pesquisa_ies,
    score_formacao_capital_humano_superior,
    case
        when score_excelencia_academica_pesquisa_ies >= 82.0
            then 'HUB_UNIVERSITARIO_PESQUISA_PONTA'
        when score_excelencia_academica_pesquisa_ies >= 70.0
            then 'POLO_CONSOLIDADO_ENSINO_SUPERIOR'
        when score_excelencia_academica_pesquisa_ies >= 58.0
            then 'POLO_REGIONAL_EXPANSAO_GRADUACAO'
        else 'REDE_EM_DESENVOLVIMENTO_ACESSO_SUPERIOR'
    end as classificacao_polo_educacao_superior,
    reference_date,
    extracted_at
from base
