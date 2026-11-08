with adm as (
    select * from {{ ref('stg_cvm_administradores_fundos') }}
)

select
    cnpj_administrador,
    nome_administrador,
    total_fundos_administrados,
    patrimonio_liquido_total
from adm
