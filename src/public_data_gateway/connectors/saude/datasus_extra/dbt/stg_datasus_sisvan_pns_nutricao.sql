{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(cobertura_sisvan_atencao_basica_pct as double) as cobertura_sisvan_atencao_basica_pct,
    cast(prevalencia_obesidade_adulto_pns_pct as double) as prevalencia_obesidade_adulto_pns_pct,
    cast(acompanhamento_condicoes_cronicas_pct as double) as acompanhamento_condicoes_cronicas_pct,
    cast(inseguranca_alimentar_grave_pct as double) as inseguranca_alimentar_grave_pct
from {{ source('datasus_extra', 'datasus_sisvan_pns_nutricao') }}
where id_municipio is not null
