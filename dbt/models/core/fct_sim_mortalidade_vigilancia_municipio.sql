{{ config(materialized='table') }}

with gerais as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_referencia,
        total_obitos_registrados,
        taxa_bruta_mortalidade_1000_hab,
        obitos_hospitalares,
        proporcao_obitos_hospitalares_pct,
        obitos_causas_mal_definidas,
        proporcao_causas_mal_definidas_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_sim_obitos_gerais_municipio') }}
),

materno_infantil as (
    select
        id_municipio,
        obitos_infantis_menores_1_ano,
        obitos_neonatais_precoce_tardio,
        taxa_mortalidade_infantil_1000_nv,
        obitos_maternos_declarados,
        razao_mortalidade_materna_100k_nv,
        proporcao_obitos_infantis_investigados_pct
    from {{ ref('stg_sim_mortalidade_materno_infantil') }}
),

prematura as (
    select
        id_municipio,
        obitos_prematuros_dcnt_30_69_anos,
        proporcao_mortalidade_prematura_dcnt_pct,
        taxa_mortalidade_causas_externas_100k_hab,
        taxa_mortalidade_acidentes_transporte_100k_hab,
        anos_potenciais_vida_perdidos_1000_hab
    from {{ ref('stg_sim_mortalidade_prematura_dcnt_externas') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    g.id_municipio,
    g.nome_municipio,
    g.sigla_uf,
    g.id_uf,
    coalesce(m.nome_uf, g.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    g.ano_referencia,
    g.total_obitos_registrados,
    g.taxa_bruta_mortalidade_1000_hab,
    g.obitos_hospitalares,
    g.proporcao_obitos_hospitalares_pct,
    g.obitos_causas_mal_definidas,
    g.proporcao_causas_mal_definidas_pct,
    mi.obitos_infantis_menores_1_ano,
    mi.obitos_neonatais_precoce_tardio,
    mi.taxa_mortalidade_infantil_1000_nv,
    mi.obitos_maternos_declarados,
    mi.razao_mortalidade_materna_100k_nv,
    mi.proporcao_obitos_infantis_investigados_pct,
    p.obitos_prematuros_dcnt_30_69_anos,
    p.proporcao_mortalidade_prematura_dcnt_pct,
    p.taxa_mortalidade_causas_externas_100k_hab,
    p.taxa_mortalidade_acidentes_transporte_100k_hab,
    p.anos_potenciais_vida_perdidos_1000_hab,
    g.reference_date,
    g.extracted_at
from gerais g
inner join materno_infantil mi on g.id_municipio = mi.id_municipio
inner join prematura p on g.id_municipio = p.id_municipio
left join municipios m on g.id_municipio = m.id_municipio
