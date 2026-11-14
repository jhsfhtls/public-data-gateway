{{ config(materialized='table') }}

with nv as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_referencia,
        total_nascidos_vivos,
        taxa_bruta_natalidade_1000_hab,
        nascidos_vivos_hospitalares,
        proporcao_partos_hospitalares_pct,
        nascidos_vivos_parto_cesareo,
        proporcao_partos_cesareos_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_sinasc_nascidos_vivos_municipio') }}
),

prenatal_adeq as (
    select
        id_municipio,
        participacao_faixa_prenatal_pct as cobertura_prenatal_7_mais_consultas_pct,
        inicio_prenatal_primeiro_trimestre_pct as inicio_prenatal_oportuno_1_trim_pct
    from {{ ref('stg_sinasc_assistencia_prenatal_parto') }}
    where faixa_assistencia_prenatal = 'Adequado_Mais_7_Consultas'
),

materno as (
    select
        id_municipio,
        idade_media_materna_anos,
        maes_adolescentes_menores_20_anos,
        proporcao_gravidez_adolescencia_pct,
        maes_35_anos_ou_mais,
        proporcao_gestacao_tardia_35_mais_pct,
        proporcao_escolaridade_materna_12_anos_mais_pct
    from {{ ref('stg_sinasc_perfil_materno_demografico') }}
),

vitalidade as (
    select
        id_municipio,
        nascidos_baixo_peso_menor_2500g,
        proporcao_baixo_peso_nascer_pct,
        nascidos_prematuros_menor_37_sem,
        proporcao_prematuridade_pct,
        proporcao_apgar_5min_adequado_7_10_pct,
        taxa_anomalias_congenitas_10k_nv
    from {{ ref('stg_sinasc_condicoes_vitalidade_recem_nascido') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    nv.id_municipio,
    nv.nome_municipio,
    nv.sigla_uf,
    nv.id_uf,
    coalesce(m.nome_uf, nv.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    nv.ano_referencia,
    nv.total_nascidos_vivos,
    nv.taxa_bruta_natalidade_1000_hab,
    nv.nascidos_vivos_hospitalares,
    nv.proporcao_partos_hospitalares_pct,
    nv.nascidos_vivos_parto_cesareo,
    nv.proporcao_partos_cesareos_pct,
    pa.cobertura_prenatal_7_mais_consultas_pct,
    pa.inicio_prenatal_oportuno_1_trim_pct,
    mat.idade_media_materna_anos,
    mat.maes_adolescentes_menores_20_anos,
    mat.proporcao_gravidez_adolescencia_pct,
    mat.maes_35_anos_ou_mais,
    mat.proporcao_gestacao_tardia_35_mais_pct,
    mat.proporcao_escolaridade_materna_12_anos_mais_pct,
    vit.nascidos_baixo_peso_menor_2500g,
    vit.proporcao_baixo_peso_nascer_pct,
    vit.nascidos_prematuros_menor_37_sem,
    vit.proporcao_prematuridade_pct,
    vit.proporcao_apgar_5min_adequado_7_10_pct,
    vit.taxa_anomalias_congenitas_10k_nv,
    nv.reference_date,
    nv.extracted_at
from nv
inner join prenatal_adeq pa on nv.id_municipio = pa.id_municipio
inner join materno mat on nv.id_municipio = mat.id_municipio
inner join vitalidade vit on nv.id_municipio = vit.id_municipio
left join municipios m on nv.id_municipio = m.id_municipio
