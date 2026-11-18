{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/pgfn_divida_ativa/silver/pgfn_divida_previdenciaria.parquet')
)

select
    cast(numero_inscricao as varchar) as numero_inscricao,
    cast(documento_devedor as varchar) as documento_devedor,
    cast(nome_devedor as varchar) as nome_devedor,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(situacao_inscricao as varchar) as situacao_inscricao,
    cast(indicador_ajuizado as boolean) as indicador_ajuizado,
    cast(valor_consolidado_brl as double) as valor_consolidado_brl,
    cast(ano_trimestre_ref as varchar) as ano_trimestre_ref
from source
