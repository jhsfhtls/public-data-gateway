{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_epidemiologico as bigint) as ano_epidemiologico,
    cast(doses_aplicadas_calendario_pni_ano as bigint) as doses_aplicadas_calendario_pni_ano,
    cast(salas_vacina_ativas_cnes as bigint) as salas_vacina_ativas_cnes,
    cast(cobertura_pentavalente_d3_pct as double) as cobertura_pentavalente_d3_pct,
    cast(cobertura_poliomielite_vip_pct as double) as cobertura_poliomielite_vip_pct,
    cast(cobertura_triplice_viral_d1_pct as double) as cobertura_triplice_viral_d1_pct,
    cast(homogeneidade_cobertura_vacinal_pni_pct as double) as homogeneidade_cobertura_vacinal_pni_pct,
    cast(taxa_abandono_esquema_multidose_pct as double) as taxa_abandono_esquema_multidose_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('opendatasus_vigilancia_silver', 'vigilancia_cobertura_vacinal_sipni_municipio') }}
