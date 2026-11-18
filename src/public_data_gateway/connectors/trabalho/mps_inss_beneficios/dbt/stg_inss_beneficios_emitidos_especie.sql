{{ config(materialized='view') }}

select
    cast(cod_beneficio_inss as varchar) as cod_beneficio_inss,
    cast(especie_beneficio_codigo as varchar) as especie_beneficio_codigo,
    cast(grupo_beneficio_inss as varchar) as grupo_beneficio_inss,
    cast(clientela_inss as varchar) as clientela_inss,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_beneficios_emitidos as integer) as qtd_beneficios_emitidos,
    cast(valor_mensal_emitido_mil_brl as double) as valor_mensal_emitido_mil_brl,
    cast(competencia_referencia as varchar) as competencia_referencia
from {{ source('mps_inss_beneficios', 'inss_beneficios_emitidos_especie') }}
where cod_beneficio_inss is not null
