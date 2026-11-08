with raw_inf as (
    select * from {{ source('cvm_fundos', 'cvm_informe_diario') }}
)

select
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(tipo_fundo as varchar) as tipo_fundo,
    cast(id_subclasse as varchar) as id_subclasse,
    cast(data_competencia as date) as data_competencia,
    cast(valor_total as double) as valor_total,
    cast(valor_cota as double) as valor_cota,
    cast(patrimonio_liquido as double) as patrimonio_liquido,
    cast(captacao_dia as double) as captacao_dia,
    cast(resgate_dia as double) as resgate_dia,
    cast(captacao_liquida_dia as double) as captacao_liquida_dia,
    cast(numero_cotistas as integer) as numero_cotistas
from raw_inf
