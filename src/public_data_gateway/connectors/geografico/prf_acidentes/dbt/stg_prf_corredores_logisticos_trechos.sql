{{ config(materialized='view') }}

select
    cast(cod_corredor_prf as varchar) as cod_corredor_prf,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(rodovia_br_principal as varchar) as rodovia_br_principal,
    cast(extensao_monitorada_km as double) as extensao_monitorada_km,
    cast(sinistros_carga_pesada_ano as integer) as sinistros_carga_pesada_ano,
    cast(indice_severidade_corredor as double) as indice_severidade_corredor,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('prf_acidentes', 'prf_corredores_logisticos_trechos') }}
where id_municipio is not null
