{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(competencia_movimentacao as varchar) as competencia_movimentacao,
    cast(salario_medio_admissao_reais as double) as salario_medio_admissao_reais,
    cast(salario_medio_desligamento_reais as double) as salario_medio_desligamento_reais,
    cast(piso_salarial_admissao_reais as double) as piso_salarial_admissao_reais,
    cast(massa_salarial_admissoes_mil_reais as double) as massa_salarial_admissoes_mil_reais,
    cast(razao_salario_admissao_desligamento as double) as razao_salario_admissao_desligamento,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_novo_caged/silver/caged_salarios_remuneracao.parquet')
