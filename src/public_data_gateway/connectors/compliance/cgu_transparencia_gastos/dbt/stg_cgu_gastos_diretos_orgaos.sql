{{ config(materialized='view') }}

select
    cast(cod_despesa_cgu as varchar) as cod_despesa_cgu,
    cast(orgao_superior_codigo as varchar) as orgao_superior_codigo,
    cast(funcao_orcamentaria as varchar) as funcao_orcamentaria,
    cast(modalidade_gasto_federal as varchar) as modalidade_gasto_federal,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(valor_empenhado_mil_brl as double) as valor_empenhado_mil_brl,
    cast(valor_pago_mil_brl as double) as valor_pago_mil_brl,
    cast(ano_exercicio as integer) as ano_exercicio
from {{ source('cgu_transparencia_gastos', 'cgu_gastos_diretos_orgaos') }}
where cod_despesa_cgu is not null
