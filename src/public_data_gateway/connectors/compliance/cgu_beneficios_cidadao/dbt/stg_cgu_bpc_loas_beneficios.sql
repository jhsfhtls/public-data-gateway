{{ config(materialized='view') }}

select
    cast(cod_bpc_cgu as varchar) as cod_bpc_cgu,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(beneficiarios_bpc_idoso_pcd as bigint) as beneficiarios_bpc_idoso_pcd,
    cast(valor_mensal_bpc_mil_brl as double) as valor_mensal_bpc_mil_brl,
    cast(parcelas_auxilio_gas_ativas as bigint) as parcelas_auxilio_gas_ativas,
    cast(cobertura_cadunico_pobreza_pct as double) as cobertura_cadunico_pobreza_pct
from {{ source('cgu_beneficios_cidadao', 'cgu_bpc_loas_beneficios') }}
