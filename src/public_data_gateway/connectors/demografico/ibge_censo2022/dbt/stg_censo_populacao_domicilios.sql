{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(populacao_residente as bigint) as populacao_residente,
    cast(total_domicilios_particulares_ocupados as bigint) as total_domicilios_particulares_ocupados,
    cast(total_moradores_domicilios_ocupados as bigint) as total_moradores_domicilios_ocupados,
    cast(media_moradores_por_domicilio as double) as media_moradores_por_domicilio,
    cast(area_territorial_km2 as double) as area_territorial_km2,
    cast(densidade_demografica_hab_km2 as double) as densidade_demografica_hab_km2,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_censo2022/silver/censo_populacao_domicilios.parquet')
