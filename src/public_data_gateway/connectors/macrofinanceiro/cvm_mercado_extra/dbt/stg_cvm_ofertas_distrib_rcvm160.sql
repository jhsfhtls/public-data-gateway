{{ config(materialized='view') }}

select
    cast(cod_emissao_rcvm160 as varchar) as cod_emissao_rcvm160,
    cast(valor_mobiliario_tipo as varchar) as valor_mobiliario_tipo,
    cast(rito_registro_cvm as varchar) as rito_registro_cvm,
    cast(situacao_oferta_cvm as varchar) as situacao_oferta_cvm,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_ofertas_publicas as bigint) as qtd_ofertas_publicas,
    cast(volume_ofertado_mil_brl as double) as volume_ofertado_mil_brl,
    cast(prazo_medio_emissao_anos as integer) as prazo_medio_emissao_anos
from {{ source('cvm_mercado_extra', 'cvm_ofertas_distrib_rcvm160') }}
