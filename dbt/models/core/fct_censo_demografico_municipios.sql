{{ config(materialized='table') }}

with pop as (
    select * from {{ ref('stg_censo_populacao_domicilios') }}
),

age as (
    select * from {{ ref('stg_censo_estrutura_etaria') }}
),

edu as (
    select * from {{ ref('stg_censo_educacao_alfabetizacao') }}
),

urb as (
    select * from {{ ref('stg_censo_urbanizacao_situacao') }}
),

mun as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        is_capital
    from {{ ref('dim_municipio') }}
)

select
    p.id_municipio,
    coalesce(m.nome_municipio, p.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, p.sigla_uf) as sigla_uf,
    m.regiao,
    coalesce(m.is_capital, false) as is_capital,
    p.ano_censo,
    p.populacao_residente,
    p.total_domicilios_particulares_ocupados,
    p.total_moradores_domicilios_ocupados,
    p.media_moradores_por_domicilio,
    p.area_territorial_km2,
    p.densidade_demografica_hab_km2,
    a.idade_mediana_anos,
    a.indice_envelhecimento_razao,
    a.razao_sexo_homens_por_100_mulheres,
    a.populacao_homens,
    a.populacao_mulheres,
    e.populacao_15_anos_ou_mais,
    e.populacao_alfabetizada,
    e.taxa_alfabetizacao_pct,
    e.taxa_analfabetismo_pct,
    u.populacao_urbana,
    u.populacao_rural,
    u.taxa_urbanizacao_pct,
    u.predominancia_territorial,
    p.reference_date,
    p.extracted_at
from pop p
left join age a on p.id_municipio = a.id_municipio and p.ano_censo = a.ano_censo
left join edu e on p.id_municipio = e.id_municipio and p.ano_censo = e.ano_censo
left join urb u on p.id_municipio = u.id_municipio and p.ano_censo = u.ano_censo
left join mun m on p.id_municipio = m.id_municipio
