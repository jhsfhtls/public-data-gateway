with gest as (
    select * from {{ ref('stg_cvm_gestores_fundos') }}
)

select
    cnpj_gestor,
    nome_gestor,
    total_fundos_geridos,
    patrimonio_liquido_total,
    captacao_liquida_recente_total,
    principais_classes
from gest
