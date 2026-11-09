{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(competencia_movimentacao as varchar) as competencia_movimentacao,
    cast(faixa_etaria as varchar) as faixa_etaria,
    cast(nivel_escolaridade as varchar) as nivel_escolaridade,
    cast(sexo_trabalhador as varchar) as sexo_trabalhador,
    cast(total_admissoes as integer) as total_admissoes,
    cast(total_desligamentos as integer) as total_desligamentos,
    cast(saldo_demografico as integer) as saldo_demografico,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_novo_caged/silver/caged_demografia_trabalhador.parquet')
