{{ config(materialized='table') }}

with fornecedores as (
    select * from {{ ref('fct_fornecedor_compras_catalogo') }}
)

select
    cnpj_fornecedor,
    nome_fornecedor,
    total_itens_fornecidos,
    valor_total_fornecido,
    ticket_medio_unitario,
    total_uasgs_atendidas,
    sigla_uf_principal,
    case
        when valor_total_fornecido >= 1000000.0 then 'FORNECEDOR_ESTRATEGICO'
        when valor_total_fornecido >= 100000.0 then 'FORNECEDOR_MEDIO'
        else 'FORNECEDOR_REGIONAL'
    end as perfil_porte_governamental
from fornecedores
