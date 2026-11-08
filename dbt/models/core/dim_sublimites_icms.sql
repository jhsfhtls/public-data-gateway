{{ config(materialized='table') }}

select
    sub.ano_calendario,
    sub.sigla_uf,
    sub.sublimite_icms_iss_anual,
    sub.adotou_sublimite_diferenciado,
    sub.base_legal_portaria,
    case
        when sub.adotou_sublimite_diferenciado then 'SUBLIMITE_REDUZIDO_3_6M'
        else 'TETO_NACIONAL_4_8M'
    end as categoria_sublimite_estadual
from {{ ref('stg_rfb_simples_sublimites') }} sub
