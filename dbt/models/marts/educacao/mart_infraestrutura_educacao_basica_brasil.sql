{{ config(materialized='table') }}

with base as (
    select
        *,
        least(100.0, greatest(0.0, (escolas_com_internet_banda_larga_pct / 100.0) * 100.0)) as s_net,
        least(100.0, greatest(0.0, (escolas_com_laboratorio_informatica_pct / 85.0) * 100.0)) as s_info,
        least(100.0, greatest(0.0, (escolas_com_laboratorio_ciencias_pct / 65.0) * 100.0)) as s_cien,
        least(100.0, greatest(0.0, (escolas_com_biblioteca_pct / 90.0) * 100.0)) as s_bib,
        least(100.0, greatest(0.0, (escolas_acessibilidade_pcd_pct / 85.0) * 100.0)) as s_pcd,
        least(100.0, greatest(0.0, (indicador_adequacao_formacao_docente_afd_pct / 88.0) * 100.0)) as s_afd,
        least(100.0, greatest(0.0, (proporcao_tempo_integral_total_pct / 38.0) * 100.0)) as s_int,
        least(100.0, greatest(0.0, ((taxa_aprovacao_media_pct - 80.0) / 18.0) * 100.0)) as s_apr,
        least(100.0, greatest(0.0, ((10.0 - taxa_abandono_escolar_pct) / 10.0) * 100.0)) as s_ret
    from {{ ref('fct_censo_escolar_infraestrutura_docentes_municipio') }}
),

etapas_agg as (
    select
        id_municipio,
        sum(case when etapa_ensino = 'Educacao_Infantil' then matriculas_totais_etapa else 0 end) as matriculas_educacao_infantil,
        sum(case when etapa_ensino in ('Ensino_Fundamental_Anos_Iniciais', 'Ensino_Fundamental_Anos_Finais') then matriculas_totais_etapa else 0 end) as matriculas_ensino_fundamental,
        sum(case when etapa_ensino = 'Ensino_Medio' then matriculas_totais_etapa else 0 end) as matriculas_ensino_medio
    from {{ ref('fct_censo_escolar_matriculas_etapas_municipio') }}
    group by id_municipio
),

scored as (
    select
        b.*,
        coalesce(e.matriculas_educacao_infantil, 0) as matriculas_educacao_infantil,
        coalesce(e.matriculas_ensino_fundamental, 0) as matriculas_ensino_fundamental,
        coalesce(e.matriculas_ensino_medio, 0) as matriculas_ensino_medio,
        round(
            0.25 * b.s_net
            + 0.20 * b.s_info
            + 0.20 * b.s_cien
            + 0.15 * b.s_bib
            + 0.20 * b.s_pcd,
            2
        ) as score_infraestrutura_escolar_inep,
        round(
            0.30 * b.s_afd
            + 0.25 * b.s_int
            + 0.25 * b.s_apr
            + 0.20 * b.s_ret,
            2
        ) as score_qualidade_fluxo_educacional
    from base b
    left join etapas_agg e on b.id_municipio = e.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    cep_8,
    ano_censo,
    total_escolas_ativas,
    escolas_rede_publica,
    escolas_rede_privada,
    proporcao_rede_publica_pct,
    escolas_com_internet_banda_larga_pct,
    escolas_com_laboratorio_informatica_pct,
    escolas_com_laboratorio_ciencias_pct,
    escolas_com_biblioteca_pct,
    escolas_com_quadra_esportiva_pct,
    escolas_acessibilidade_pcd_pct,
    total_docentes_exercicio,
    docentes_ensino_superior_completo,
    proporcao_docentes_superior_pct,
    docentes_pos_graduacao_pct,
    indicador_adequacao_formacao_docente_afd_pct,
    razao_alunos_por_docente,
    total_matriculas_educacao_basica,
    matriculas_tempo_integral_total,
    proporcao_tempo_integral_total_pct,
    matriculas_educacao_infantil,
    matriculas_ensino_fundamental,
    matriculas_ensino_medio,
    taxa_aprovacao_media_pct,
    taxa_reprovacao_media_pct,
    taxa_abandono_escolar_pct,
    taxa_distorcao_idade_serie_media_pct,
    score_infraestrutura_escolar_inep,
    score_qualidade_fluxo_educacional,
    case
        when score_infraestrutura_escolar_inep >= 85.0 and score_qualidade_fluxo_educacional >= 82.0
            then 'REDE_EDUCACIONAL_EXCELENCIA_INTEGRAL'
        when score_infraestrutura_escolar_inep >= 72.0
            then 'POLO_EDUCACIONAL_INFRAESTRUTURA_AVANCADA'
        when score_infraestrutura_escolar_inep >= 58.0
            then 'REDE_ENSINO_BASICO_CONSOLIDADA'
        else 'REDE_PRIORITARIA_EXPANSAO_INFRAESTRUTURA'
    end as classificacao_rede_educacional_basica,
    reference_date,
    extracted_at
from scored
