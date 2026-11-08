{{ config(materialized='view') }}

select
    cast(codigo_grupo as varchar) as codigo_grupo,
    cast(nome_grupo as varchar) as nome_grupo,
    cast(status_grupo as boolean) as status_grupo,
    cast(codigo_classe as varchar) as codigo_classe,
    cast(nome_classe as varchar) as nome_classe,
    cast(status_classe as boolean) as status_classe,
    cast(tipo_catalogo as varchar) as tipo_catalogo,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/comprasgov_catmat/silver/catmat_grupos_classes.parquet')
