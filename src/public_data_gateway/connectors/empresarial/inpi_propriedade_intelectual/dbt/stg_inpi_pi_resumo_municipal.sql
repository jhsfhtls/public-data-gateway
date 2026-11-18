{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(patentes_depositadas_12m as bigint) as patentes_depositadas_12m,
    cast(marcas_concedidas_12m as bigint) as marcas_concedidas_12m,
    cast(registros_software_total as bigint) as registros_software_total,
    cast(contratos_tecnologia_ativos as bigint) as contratos_tecnologia_ativos,
    cast(patentes_por_100k_hab as double) as patentes_por_100k_hab
from {{ source('inpi_propriedade_intelectual', 'inpi_pi_resumo_municipal') }}
where id_municipio is not null
