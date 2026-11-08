{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_catmat_fornecedores_compras') }}
)

select
    cnpj_fornecedor,
    nome_fornecedor,
    total_itens_fornecidos,
    valor_total_fornecido,
    ticket_medio_unitario,
    total_uasgs_atendidas,
    sigla_uf_principal,
    reference_date,
    extracted_at
from stg
