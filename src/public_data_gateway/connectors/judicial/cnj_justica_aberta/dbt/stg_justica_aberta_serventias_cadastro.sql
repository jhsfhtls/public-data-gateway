{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(semestre_referencia as varchar) as semestre_referencia,
    cast(total_serventias_ativas as integer) as total_serventias_ativas,
    cast(serventias_providas_titular as integer) as serventias_providas_titular,
    cast(serventias_vagas_interino as integer) as serventias_vagas_interino,
    cast(taxa_provimento_titular_pct as double) as taxa_provimento_titular_pct,
    cast(oficios_registro_imoveis as integer) as oficios_registro_imoveis,
    cast(tabelionatos_notas_protesto as integer) as tabelionatos_notas_protesto,
    cast(oficios_registro_civil_rcpn as integer) as oficios_registro_civil_rcpn,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('cnj_justica_aberta_silver', 'justica_aberta_serventias_cadastro') }}
