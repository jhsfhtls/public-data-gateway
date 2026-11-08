{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano as integer) as ano,
    cast(populacao_residente as bigint) as populacao_residente,
    cast(unidade_medida as varchar) as unidade_medida,
    cast(densidade_demografica_hab_km2 as double) as densidade_demografica_hab_km2,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_sidra/silver/sidra_populacao_censo.parquet')
