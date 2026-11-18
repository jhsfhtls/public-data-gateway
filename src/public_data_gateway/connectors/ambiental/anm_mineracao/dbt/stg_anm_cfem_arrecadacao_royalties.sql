{{ config(materialized='view') }}

select
    cast(cod_guia_cfem as varchar) as cod_guia_cfem,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(substancia_principal as varchar) as substancia_principal,
    cast(valor_operacao_mineral_mil_brl as double) as valor_operacao_mineral_mil_brl,
    cast(arrecadacao_cfem_municipio_mil_brl as double) as arrecadacao_cfem_municipio_mil_brl,
    cast(aliquota_media_cfem_pct as double) as aliquota_media_cfem_pct,
    cast(ano_referencia as bigint) as ano_referencia
from {{ source('anm_mineracao', 'anm_cfem_arrecadacao_royalties') }}
where cod_guia_cfem is not null
