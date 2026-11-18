{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(subestacoes_alta_tensao as integer) as subestacoes_alta_tensao,
    cast(linhas_transmissao_km as double) as linhas_transmissao_km,
    cast(indice_continuidade_dec_horas as double) as indice_continuidade_dec_horas,
    cast(disponibilidade_rede_sin_pct as double) as disponibilidade_rede_sin_pct
from {{ source('mme_ons_energia', 'mme_intercambio_seguranca_energetica') }}
where id_municipio is not null
