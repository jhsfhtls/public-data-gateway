{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_exercicio_fiscal as bigint) as ano_exercicio_fiscal,
    cast(inscricoes_imobiliarias_ativas_iptu as bigint) as inscricoes_imobiliarias_ativas_iptu,
    cast(area_construida_tributada_mil_m2 as double) as area_construida_tributada_mil_m2,
    cast(valor_venal_total_bilhoes_brl as double) as valor_venal_total_bilhoes_brl,
    cast(valor_venal_medio_m2_brl as double) as valor_venal_medio_m2_brl,
    cast(aliquota_media_efetiva_iptu_pct as double) as aliquota_media_efetiva_iptu_pct,
    cast(arrecadacao_iptu_milhoes_brl as double) as arrecadacao_iptu_milhoes_brl,
    cast(taxa_adimplencia_iptu_pct as double) as taxa_adimplencia_iptu_pct,
    cast(indice_atualizacao_pgv_pct as double) as indice_atualizacao_pgv_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('capitais_iptu_itbi_silver', 'iptu_itbi_cadastro_imobiliario_municipal') }}
