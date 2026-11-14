{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(obitos_infantis_menores_1_ano as integer) as obitos_infantis_menores_1_ano,
    cast(obitos_neonatais_precoce_tardio as integer) as obitos_neonatais_precoce_tardio,
    cast(taxa_mortalidade_infantil_1000_nv as double) as taxa_mortalidade_infantil_1000_nv,
    cast(obitos_maternos_declarados as integer) as obitos_maternos_declarados,
    cast(razao_mortalidade_materna_100k_nv as double) as razao_mortalidade_materna_100k_nv,
    cast(proporcao_obitos_infantis_investigados_pct as double) as proporcao_obitos_infantis_investigados_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sim_silver', 'sim_mortalidade_materno_infantil') }}
