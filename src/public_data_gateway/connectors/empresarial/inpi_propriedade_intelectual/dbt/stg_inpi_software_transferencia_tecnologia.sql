{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(registros_programa_computador as bigint) as registros_programa_computador,
    cast(contratos_transferencia_tecnologia as bigint) as contratos_transferencia_tecnologia,
    cast(ict_universidades_depositantes as bigint) as ict_universidades_depositantes,
    cast(indicacoes_geograficas_locais as bigint) as indicacoes_geograficas_locais
from {{ source('inpi_propriedade_intelectual', 'inpi_software_transferencia_tecnologia') }}
where id_municipio is not null
