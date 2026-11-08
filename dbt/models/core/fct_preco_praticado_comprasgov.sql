{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_catmat_precos_praticados') }}
)

select
    id_compra_item,
    id_compra,
    data_compra,
    codigo_item_catalogo,
    codigo_pdm,
    nome_pdm,
    descricao_item,
    quantidade,
    preco_unitario,
    valor_total,
    sigla_unidade_fornecimento,
    cnpj_fornecedor,
    nome_fornecedor,
    marca,
    codigo_uasg,
    nome_uasg,
    codigo_municipio_ibge,
    municipio,
    sigla_uf,
    reference_date,
    extracted_at
from stg
