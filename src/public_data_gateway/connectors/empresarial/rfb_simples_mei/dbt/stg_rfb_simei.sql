{{ config(materialized='view') }}

select
    cast(cnpj_basico as varchar) as cnpj_basico,
    cast(opcao_simei as varchar) as opcao_simei,
    cast(data_opcao_simei as varchar) as data_opcao_simei,
    cast(data_exclusao_simei as varchar) as data_exclusao_simei,
    cast(motivo_desenquadramento_simei as varchar) as motivo_desenquadramento_simei,
    cast(status_simei_atual as varchar) as status_simei_atual
from {{ source('rfb_simples_mei', 'simei_cadastral') }}
