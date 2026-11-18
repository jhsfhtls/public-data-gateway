{{ config(materialized='view') }}

select
    cast(cod_transf_cgu as varchar) as cod_transf_cgu,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(repasse_fpm_fundeb_mil_brl as double) as repasse_fpm_fundeb_mil_brl,
    cast(repasse_sus_assistencia_mil_brl as double) as repasse_sus_assistencia_mil_brl,
    cast(dependencia_repasses_federais_pct as double) as dependencia_repasses_federais_pct,
    cast(ano_exercicio as integer) as ano_exercicio
from {{ source('cgu_transparencia_gastos', 'cgu_transferencias_constitucionais_fpm') }}
where id_municipio is not null
