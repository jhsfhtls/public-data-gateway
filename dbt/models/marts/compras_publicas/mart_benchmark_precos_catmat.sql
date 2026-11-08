{{ config(materialized='table') }}

with precos as (
    select * from {{ ref('fct_preco_praticado_comprasgov') }}
)

select
    codigo_pdm,
    nome_pdm,
    count(distinct id_compra_item) as total_itens_comprados,
    coalesce(round(sum(quantidade), 2), 0.0) as quantidade_total,
    coalesce(round(sum(valor_total), 2), 0.0) as valor_total_homologado,
    coalesce(round(avg(preco_unitario), 2), 0.0) as preco_medio_praticado,
    coalesce(round(min(preco_unitario), 2), 0.0) as preco_minimo,
    coalesce(round(max(preco_unitario), 2), 0.0) as preco_maximo,
    coalesce(round(max(preco_unitario) - min(preco_unitario), 2), 0.0) as amplitude_preco,
    count(distinct cnpj_fornecedor) as total_fornecedores,
    count(distinct codigo_uasg) as total_uasgs_compradoras,
    count(distinct sigla_uf) as total_ufs_atendidas
from precos
group by
    codigo_pdm,
    nome_pdm
