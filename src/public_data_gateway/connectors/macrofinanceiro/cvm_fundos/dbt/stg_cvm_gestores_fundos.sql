with raw_gest as (
    select * from {{ source('cvm_fundos', 'cvm_gestores_fundos') }}
)

select
    cast(cnpj_gestor as varchar) as cnpj_gestor,
    cast(nome_gestor as varchar) as nome_gestor,
    cast(total_fundos_geridos as integer) as total_fundos_geridos,
    cast(patrimonio_liquido_total as double) as patrimonio_liquido_total,
    cast(captacao_liquida_recente_total as double) as captacao_liquida_recente_total,
    cast(principais_classes as varchar) as principais_classes
from raw_gest
