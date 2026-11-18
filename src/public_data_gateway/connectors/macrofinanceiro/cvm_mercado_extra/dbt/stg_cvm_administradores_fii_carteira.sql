{{ config(materialized='view') }}

select
    cast(cod_adm_cvm as varchar) as cod_adm_cvm,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(administradores_ativos_count as bigint) as administradores_ativos_count,
    cast(gestoras_fii_fip_count as bigint) as gestoras_fii_fip_count,
    cast(patrimonio_administrado_mi_brl as double) as patrimonio_administrado_mi_brl,
    cast(taxa_conformidade_informes_pct as double) as taxa_conformidade_informes_pct
from {{ source('cvm_mercado_extra', 'cvm_administradores_fii_carteira') }}
