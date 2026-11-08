{{ config(materialized='table') }}

select
    s.cnpj_basico,
    s.opcao_simples,
    s.data_opcao_simples,
    s.data_exclusao_simples,
    s.motivo_exclusao_simples,
    s.status_simples_atual,
    case
        when s.status_simples_atual = 'OPTANTE' then true
        else false
    end as is_optante_ativo
from {{ ref('stg_rfb_simples_nacional') }} s
