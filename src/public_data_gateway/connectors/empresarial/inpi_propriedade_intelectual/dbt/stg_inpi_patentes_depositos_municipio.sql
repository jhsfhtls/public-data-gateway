{{ config(materialized='view') }}

select
    cast(cod_processo_inpi as varchar) as cod_processo_inpi,
    cast(natureza_ativo_pi as varchar) as natureza_ativo_pi,
    cast(classificacao_ipc_nice as varchar) as classificacao_ipc_nice,
    cast(situacao_concessao_inpi as varchar) as situacao_concessao_inpi,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(depositos_anuais_rpi as bigint) as depositos_anuais_rpi,
    cast(concessoes_anuais_inpi as bigint) as concessoes_anuais_inpi,
    cast(ano_referencia as bigint) as ano_referencia
from {{ source('inpi_propriedade_intelectual', 'inpi_patentes_depositos_municipio') }}
where cod_processo_inpi is not null
