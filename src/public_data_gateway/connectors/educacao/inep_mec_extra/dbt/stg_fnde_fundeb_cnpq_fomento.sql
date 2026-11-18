{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(repasses_fundeb_anual_mil_brl as double) as repasses_fundeb_anual_mil_brl,
    cast(investimento_aluno_ano_fundeb_brl as double) as investimento_aluno_ano_fundeb_brl,
    cast(bolsas_pesquisa_cnpq_ativas as bigint) as bolsas_pesquisa_cnpq_ativas,
    cast(projetos_ct_cnpq_vigentes as bigint) as projetos_ct_cnpq_vigentes
from {{ source('inep_mec_extra', 'fnde_fundeb_cnpq_fomento') }}
where id_municipio is not null
