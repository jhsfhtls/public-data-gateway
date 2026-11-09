{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(competencia_movimentacao as varchar) as competencia_movimentacao,
    cast(codigo_cbo_6 as varchar) as codigo_cbo_6,
    cast(titulo_ocupacao_cbo as varchar) as titulo_ocupacao_cbo,
    cast(total_admissoes as integer) as total_admissoes,
    cast(total_desligamentos as integer) as total_desligamentos,
    cast(saldo_ocupacao as integer) as saldo_ocupacao,
    cast(salario_medio_admissao_reais as double) as salario_medio_admissao_reais,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_novo_caged/silver/caged_ocupacao_cbo.parquet')
