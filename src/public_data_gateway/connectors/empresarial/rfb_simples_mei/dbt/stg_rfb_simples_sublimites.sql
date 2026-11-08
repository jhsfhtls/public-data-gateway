{{ config(materialized='view') }}

select
    cast(ano_calendario as integer) as ano_calendario,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(sublimite_icms_iss_anual as double) as sublimite_icms_iss_anual,
    cast(adotou_sublimite_diferenciado as boolean) as adotou_sublimite_diferenciado,
    cast(base_legal_portaria as varchar) as base_legal_portaria
from {{ source('rfb_simples_mei', 'simples_sublimites_estaduais') }}
