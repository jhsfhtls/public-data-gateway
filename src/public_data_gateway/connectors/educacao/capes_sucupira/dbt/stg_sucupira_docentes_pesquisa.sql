with source_data as (
    select *
    from read_parquet('data/capes_sucupira/silver/sucupira_docentes_pesquisa.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_base_sucupira as bigint) as ano_base_sucupira,
    cast(total_docentes_permanentes_ano as bigint) as total_docentes_permanentes_ano,
    cast(docentes_bolsistas_pq_cnpq_ano as bigint) as docentes_bolsistas_pq_cnpq_ano,
    cast(razao_discentes_por_docente as double) as razao_discentes_por_docente,
    cast(taxa_docentes_dedicacao_exclusiva_pct as double) as taxa_docentes_dedicacao_exclusiva_pct,
    cast(taxa_doutores_internacionalizacao_pct as double) as taxa_doutores_internacionalizacao_pct,
    cast(linhas_pesquisa_ativas_ano as bigint) as linhas_pesquisa_ativas_ano,
    cast(projetos_financiamento_externo_ano as bigint) as projetos_financiamento_externo_ano,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
