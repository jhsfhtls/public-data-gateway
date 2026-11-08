{{ config(materialized='view') }}

select
    cast(cnpj_fornecedor as varchar) as cnpj_fornecedor,
    cast(nome_fornecedor as varchar) as nome_fornecedor,
    cast(total_itens_fornecidos as bigint) as total_itens_fornecidos,
    cast(valor_total_fornecido as double) as valor_total_fornecido,
    cast(ticket_medio_unitario as double) as ticket_medio_unitario,
    cast(total_uasgs_atendidas as bigint) as total_uasgs_atendidas,
    cast(sigla_uf_principal as varchar) as sigla_uf_principal,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/comprasgov_catmat/silver/catmat_fornecedores_compras.parquet')
