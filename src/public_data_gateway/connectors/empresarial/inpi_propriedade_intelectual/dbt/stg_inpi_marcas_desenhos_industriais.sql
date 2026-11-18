{{ config(materialized='view') }}

select
    cast(cod_marca_inpi as varchar) as cod_marca_inpi,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(registros_marcas_ativos as bigint) as registros_marcas_ativos,
    cast(desenhos_industriais_vigentes as bigint) as desenhos_industriais_vigentes,
    cast(taxa_concessao_marcas_pct as double) as taxa_concessao_marcas_pct,
    cast(participacao_saas_tech_pct as double) as participacao_saas_tech_pct
from {{ source('inpi_propriedade_intelectual', 'inpi_marcas_desenhos_industriais') }}
where cod_marca_inpi is not null
