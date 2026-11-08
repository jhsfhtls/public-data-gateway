{{ config(materialized='view') }}

select
    cast(id_compra_item as varchar) as id_compra_item,
    cast(id_compra as varchar) as id_compra,
    cast(data_compra as varchar) as data_compra,
    cast(codigo_item_catalogo as varchar) as codigo_item_catalogo,
    cast(codigo_pdm as varchar) as codigo_pdm,
    cast(nome_pdm as varchar) as nome_pdm,
    cast(descricao_item as varchar) as descricao_item,
    cast(quantidade as double) as quantidade,
    cast(preco_unitario as double) as preco_unitario,
    cast(valor_total as double) as valor_total,
    cast(sigla_unidade_fornecimento as varchar) as sigla_unidade_fornecimento,
    cast(cnpj_fornecedor as varchar) as cnpj_fornecedor,
    cast(nome_fornecedor as varchar) as nome_fornecedor,
    cast(marca as varchar) as marca,
    cast(codigo_uasg as varchar) as codigo_uasg,
    cast(nome_uasg as varchar) as nome_uasg,
    cast(codigo_municipio_ibge as varchar) as codigo_municipio_ibge,
    cast(municipio as varchar) as municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/comprasgov_catmat/silver/catmat_precos_praticados.parquet')
