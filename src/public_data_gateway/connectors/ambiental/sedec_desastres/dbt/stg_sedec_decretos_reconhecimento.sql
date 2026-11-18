{{ config(materialized='view') }}

select
    cast(cod_decreto_municipio as varchar) as cod_decreto_municipio,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_decretos_se_5_anos as integer) as qtd_decretos_se_5_anos,
    cast(qtd_decretos_calamidade_5_anos as integer) as qtd_decretos_calamidade_5_anos,
    cast(recursos_federais_liberados_mil_brl as double) as recursos_federais_liberados_mil_brl,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('sedec_desastres', 'sedec_decretos_reconhecimento') }}
where id_municipio is not null
