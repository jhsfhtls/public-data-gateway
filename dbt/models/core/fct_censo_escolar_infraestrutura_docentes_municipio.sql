{{ config(materialized='table') }}

with escolas as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
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
        reference_date,
        extracted_at
    from {{ ref('stg_censo_escolar_escolas_infraestrutura') }}
),

docentes as (
    select
        id_municipio,
        total_docentes_exercicio,
        docentes_ensino_superior_completo,
        proporcao_docentes_superior_pct,
        docentes_pos_graduacao_pct,
        indicador_adequacao_formacao_docente_afd_pct,
        razao_alunos_por_docente
    from {{ ref('stg_censo_escolar_docentes_qualificacao') }}
),

rendimento as (
    select
        id_municipio,
        total_matriculas_educacao_basica,
        matriculas_tempo_integral_total,
        proporcao_tempo_integral_total_pct,
        taxa_aprovacao_media_pct,
        taxa_reprovacao_media_pct,
        taxa_abandono_escolar_pct,
        taxa_distorcao_idade_serie_media_pct
    from {{ ref('stg_censo_escolar_rendimento_fluxo') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('stg_ibge_municipios') }}
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.id_uf,
    coalesce(m.nome_uf, e.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    e.cep_8,
    e.ano_censo,
    e.total_escolas_ativas,
    e.escolas_rede_publica,
    e.escolas_rede_privada,
    e.proporcao_rede_publica_pct,
    e.escolas_com_internet_banda_larga_pct,
    e.escolas_com_laboratorio_informatica_pct,
    e.escolas_com_laboratorio_ciencias_pct,
    e.escolas_com_biblioteca_pct,
    e.escolas_com_quadra_esportiva_pct,
    e.escolas_acessibilidade_pcd_pct,
    d.total_docentes_exercicio,
    d.docentes_ensino_superior_completo,
    d.proporcao_docentes_superior_pct,
    d.docentes_pos_graduacao_pct,
    d.indicador_adequacao_formacao_docente_afd_pct,
    d.razao_alunos_por_docente,
    r.total_matriculas_educacao_basica,
    r.matriculas_tempo_integral_total,
    r.proporcao_tempo_integral_total_pct,
    r.taxa_aprovacao_media_pct,
    r.taxa_reprovacao_media_pct,
    r.taxa_abandono_escolar_pct,
    r.taxa_distorcao_idade_serie_media_pct,
    e.reference_date,
    e.extracted_at
from escolas e
inner join docentes d on e.id_municipio = d.id_municipio
inner join rendimento r on e.id_municipio = r.id_municipio
left join municipios m on e.id_municipio = m.id_municipio
