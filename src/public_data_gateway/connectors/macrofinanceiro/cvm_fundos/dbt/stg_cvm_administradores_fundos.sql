with raw_adm as (
    select * from {{ source('cvm_fundos', 'cvm_administradores_fundos') }}
)

select
    cast(cnpj_administrador as varchar) as cnpj_administrador,
    cast(nome_administrador as varchar) as nome_administrador,
    cast(total_fundos_administrados as integer) as total_fundos_administrados,
    cast(patrimonio_liquido_total as double) as patrimonio_liquido_total
from raw_adm
