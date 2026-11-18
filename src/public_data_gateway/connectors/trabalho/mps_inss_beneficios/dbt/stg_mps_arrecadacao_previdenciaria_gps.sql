{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(massa_contribuintes_ativos as integer) as massa_contribuintes_ativos,
    cast(arrecadacao_previdenciaria_mil_brl as double) as arrecadacao_previdenciaria_mil_brl,
    cast(razao_contribuintes_beneficiarios as double) as razao_contribuintes_beneficiarios
from {{ source('mps_inss_beneficios', 'mps_arrecadacao_previdenciaria_gps') }}
where id_municipio is not null
