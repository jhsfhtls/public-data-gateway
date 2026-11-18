{{ config(materialized='view') }}

select
    cast(cod_prev_susep as varchar) as cod_prev_susep,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(contribuicoes_previdencia_mil_brl as double) as contribuicoes_previdencia_mil_brl,
    cast(resgates_pagamentos_mil_brl as double) as resgates_pagamentos_mil_brl,
    cast(provisoes_tecnicas_mi_brl as double) as provisoes_tecnicas_mi_brl,
    cast(participacao_vgbl_pct as double) as participacao_vgbl_pct
from {{ source('susep_seguros', 'susep_previdencia_vgbl_pgbl_uf') }}
where cod_prev_susep is not null
