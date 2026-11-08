{{ config(materialized='view') }}

select
    cast(codigo_item as varchar) as codigo_item,
    cast(codigo_pdm as varchar) as codigo_pdm,
    cast(nome_pdm as varchar) as nome_pdm,
    cast(codigo_classe as varchar) as codigo_classe,
    cast(nome_classe as varchar) as nome_classe,
    cast(codigo_grupo as varchar) as codigo_grupo,
    cast(nome_grupo as varchar) as nome_grupo,
    cast(descricao_item as varchar) as descricao_item,
    cast(status_item as boolean) as status_item,
    cast(item_sustentavel as boolean) as item_sustentavel,
    cast(codigo_ncm as varchar) as codigo_ncm,
    cast(descricao_ncm as varchar) as descricao_ncm,
    cast(data_hora_atualizacao as varchar) as data_hora_atualizacao,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/comprasgov_catmat/silver/catmat_itens_material.parquet')
