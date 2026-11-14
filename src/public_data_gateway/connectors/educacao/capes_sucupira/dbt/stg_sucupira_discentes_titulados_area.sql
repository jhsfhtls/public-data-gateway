with source_data as (
    select *
    from read_parquet('data/capes_sucupira/silver/sucupira_discentes_titulados_area.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_base_sucupira as bigint) as ano_base_sucupira,
    cast(grande_area_conhecimento as varchar) as grande_area_conhecimento,
    cast(matriculados_mestrado_area as bigint) as matriculados_mestrado_area,
    cast(matriculados_doutorado_area as bigint) as matriculados_doutorado_area,
    cast(titulados_mestres_ano as bigint) as titulados_mestres_ano,
    cast(titulados_doutores_ano as bigint) as titulados_doutores_ano,
    cast(bolsistas_capes_area as bigint) as bolsistas_capes_area,
    cast(taxa_titulacao_doutorado_pct as double) as taxa_titulacao_doutorado_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
