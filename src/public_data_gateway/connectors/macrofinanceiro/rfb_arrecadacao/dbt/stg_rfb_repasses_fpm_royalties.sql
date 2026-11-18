{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(repasses_fpm_anual_mi_brl as double) as repasses_fpm_anual_mi_brl,
    cast(transferencias_ipi_exportacao_mi_brl as double) as transferencias_ipi_exportacao_mi_brl,
    cast(cota_simples_nacional_munic_mi_brl as double) as cota_simples_nacional_munic_mi_brl,
    cast(crescimento_real_arrecadacao_pct as double) as crescimento_real_arrecadacao_pct
from {{ source('rfb_arrecadacao', 'rfb_repasses_fpm_royalties') }}
