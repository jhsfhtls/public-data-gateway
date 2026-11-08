{{ config(materialized='view') }}

select
    upper(trim(tribunal_sigla)) as tribunal_sigla,
    trim(tribunal_nome) as tribunal_nome,
    upper(trim(ramo_justica)) as ramo_justica,
    upper(trim(esfera_uf)) as esfera_uf,
    trim(municipio_sede_ibge) as municipio_sede_ibge,
    trim(portal_consulta_url) as portal_consulta_url,
    cast(total_processos_estimados as bigint) as total_processos_estimados,
    extracted_at
from read_parquet('data/cnj_datajud/silver/cnj_tribunais.parquet')
