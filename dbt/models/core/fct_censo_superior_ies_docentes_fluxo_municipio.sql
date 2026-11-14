{{ config(materialized='table') }}

with ies as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        cnpj_14,
        ano_censo,
        total_ies_ativas,
        universidades_e_centros_universitarios,
        ies_rede_publica,
        ies_rede_privada,
        proporcao_ies_publicas_pct,
        igc_medio_continuo_municipio,
        reference_date,
        extracted_at
    from {{ ref('stg_censo_superior_instituicoes_ies') }}
),

docentes as (
    select
        id_municipio,
        total_docentes_exercicio_ies,
        docentes_doutores,
        docentes_mestres,
        proporcao_doutores_pct,
        proporcao_mestres_doutores_stricto_sensu_pct,
        proporcao_regime_tempo_integral_de_pct,
        razao_alunos_por_docente_equivalente
    from {{ ref('stg_censo_superior_docentes_titulacao_regime') }}
),

fluxo as (
    select
        id_municipio,
        total_matriculas_graduacao,
        matriculas_presenciais,
        matriculas_ead_polo,
        proporcao_matriculas_ead_pct,
        ingressantes_anuais_total,
        concluintes_anuais_total,
        taxa_conclusao_acumulada_tcg_pct,
        taxa_desistencia_anual_tda_pct
    from {{ ref('stg_censo_superior_matriculas_ingressantes_concluintes') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('stg_ibge_municipios') }}
)

select
    i.id_municipio,
    i.nome_municipio,
    i.sigla_uf,
    i.id_uf,
    coalesce(m.nome_uf, i.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    i.cnpj_14,
    i.ano_censo,
    i.total_ies_ativas,
    i.universidades_e_centros_universitarios,
    i.ies_rede_publica,
    i.ies_rede_privada,
    i.proporcao_ies_publicas_pct,
    i.igc_medio_continuo_municipio,
    d.total_docentes_exercicio_ies,
    d.docentes_doutores,
    d.docentes_mestres,
    d.proporcao_doutores_pct,
    d.proporcao_mestres_doutores_stricto_sensu_pct,
    d.proporcao_regime_tempo_integral_de_pct,
    d.razao_alunos_por_docente_equivalente,
    f.total_matriculas_graduacao,
    f.matriculas_presenciais,
    f.matriculas_ead_polo,
    f.proporcao_matriculas_ead_pct,
    f.ingressantes_anuais_total,
    f.concluintes_anuais_total,
    f.taxa_conclusao_acumulada_tcg_pct,
    f.taxa_desistencia_anual_tda_pct,
    i.reference_date,
    i.extracted_at
from ies i
inner join docentes d on i.id_municipio = d.id_municipio
inner join fluxo f on i.id_municipio = f.id_municipio
left join municipios m on i.id_municipio = m.id_municipio
