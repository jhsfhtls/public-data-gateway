{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(proficiencia_saeb_matematica as double) as proficiencia_saeb_matematica,
    cast(proficiencia_saeb_linguagens as double) as proficiencia_saeb_linguagens,
    cast(beneficiarios_prouni_sisu_total as bigint) as beneficiarios_prouni_sisu_total,
    cast(repasses_fundeb_mil_brl as double) as repasses_fundeb_mil_brl,
    cast(bolsas_cnpq_100k_hab as double) as bolsas_cnpq_100k_hab
from {{ source('inep_mec_extra', 'inep_mec_extra_resumo_municipal') }}
where id_municipio is not null
