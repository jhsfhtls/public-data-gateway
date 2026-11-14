with source_data as (
    select *
    from read_parquet('data/capes_sucupira/silver/sucupira_producao_cientifica_teses.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_base_sucupira as bigint) as ano_base_sucupira,
    cast(artigos_qualis_a1_a2_ano as bigint) as artigos_qualis_a1_a2_ano,
    cast(teses_dissertacoes_defendidas_ano as bigint) as teses_dissertacoes_defendidas_ano,
    cast(producao_tecnica_patentes_software_ano as bigint) as producao_tecnica_patentes_software_ano,
    cast(indice_impacto_producao_por_docente as double) as indice_impacto_producao_por_docente,
    cast(taxa_acesso_aberto_repositorio_pct as double) as taxa_acesso_aberto_repositorio_pct,
    cast(colaboracoes_internacionais_coautoria_pct as double) as colaboracoes_internacionais_coautoria_pct,
    cast(fomento_bolsas_estimado_milhoes_brl as double) as fomento_bolsas_estimado_milhoes_brl,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
