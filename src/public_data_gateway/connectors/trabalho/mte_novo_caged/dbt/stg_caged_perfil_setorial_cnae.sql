{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(competencia_movimentacao as varchar) as competencia_movimentacao,
    cast(grande_setor_cnae as varchar) as grande_setor_cnae,
    cast(total_admissoes as integer) as total_admissoes,
    cast(total_desligamentos as integer) as total_desligamentos,
    cast(saldo_setorial as integer) as saldo_setorial,
    cast(salario_medio_admissao_reais as double) as salario_medio_admissao_reais,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_novo_caged/silver/caged_perfil_setorial_cnae.parquet')
