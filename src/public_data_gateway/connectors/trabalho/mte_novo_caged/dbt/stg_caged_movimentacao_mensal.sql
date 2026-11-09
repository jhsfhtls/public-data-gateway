{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(competencia_movimentacao as varchar) as competencia_movimentacao,
    cast(total_admissoes as integer) as total_admissoes,
    cast(total_desligamentos as integer) as total_desligamentos,
    cast(saldo_movimentacao as integer) as saldo_movimentacao,
    cast(estoque_declarado_aproximado as integer) as estoque_declarado_aproximado,
    cast(taxa_rotatividade_turnover_pct as double) as taxa_rotatividade_turnover_pct,
    cast(salario_medio_admissao_reais as double) as salario_medio_admissao_reais,
    cast(salario_medio_desligamento_reais as double) as salario_medio_desligamento_reais,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_novo_caged/silver/caged_movimentacao_mensal.parquet')
