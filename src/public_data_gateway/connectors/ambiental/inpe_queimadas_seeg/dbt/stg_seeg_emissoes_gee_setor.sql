{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/inpe_queimadas_seeg/silver/seeg_emissoes_gee_setor.parquet')
)

select
    cast(cod_emissao_seeg as varchar) as cod_emissao_seeg,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_inventario as bigint) as ano_inventario,
    cast(setor_emissao as varchar) as setor_emissao,
    cast(subsetor as varchar) as subsetor,
    cast(gas_efeito_estufa as varchar) as gas_efeito_estufa,
    cast(emissao_bruta_tco2e as double) as emissao_bruta_tco2e,
    cast(remocao_tco2e as double) as remocao_tco2e,
    cast(emissao_liquida_tco2e as double) as emissao_liquida_tco2e
from source
