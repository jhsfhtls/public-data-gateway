{{ config(materialized='view') }}

select
    cast(codigo_servico as varchar) as codigo_servico,
    cast(nome_servico as varchar) as nome_servico,
    cast(codigo_classe as varchar) as codigo_classe,
    cast(nome_classe as varchar) as nome_classe,
    cast(codigo_grupo as varchar) as codigo_grupo,
    cast(nome_grupo as varchar) as nome_grupo,
    cast(codigo_divisao as varchar) as codigo_divisao,
    cast(nome_divisao as varchar) as nome_divisao,
    cast(codigo_secao as varchar) as codigo_secao,
    cast(nome_secao as varchar) as nome_secao,
    cast(codigo_cpc as varchar) as codigo_cpc,
    cast(status_servico as boolean) as status_servico,
    cast(exclusivo_central_compras as boolean) as exclusivo_central_compras,
    cast(data_hora_atualizacao as varchar) as data_hora_atualizacao,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/comprasgov_catmat/silver/catser_itens_servico.parquet')
