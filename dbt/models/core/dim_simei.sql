{{ config(materialized='table') }}

select
    m.cnpj_basico,
    m.opcao_simei,
    m.data_opcao_simei,
    m.data_exclusao_simei,
    m.motivo_desenquadramento_simei,
    m.status_simei_atual,
    case
        when m.status_simei_atual = 'ENQUADRADO' then true
        else false
    end as is_mei_ativo
from {{ ref('stg_rfb_simei') }} m
