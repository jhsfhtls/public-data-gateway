{{ config(materialized='view') }}

select
    cast(cnpj_basico as varchar) as cnpj_basico,
    cast(opcao_simples as varchar) as opcao_simples,
    cast(data_opcao_simples as varchar) as data_opcao_simples,
    cast(data_exclusao_simples as varchar) as data_exclusao_simples,
    cast(motivo_exclusao_simples as varchar) as motivo_exclusao_simples,
    cast(status_simples_atual as varchar) as status_simples_atual
from {{ source('rfb_simples_mei', 'simples_nacional_cadastral') }}
