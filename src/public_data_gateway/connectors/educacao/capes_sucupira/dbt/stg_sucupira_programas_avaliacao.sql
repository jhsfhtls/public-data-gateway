with source_data as (
    select *
    from read_parquet('data/capes_sucupira/silver/sucupira_programas_avaliacao.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_base_sucupira as bigint) as ano_base_sucupira,
    cast(total_programas_stricto_sensu_ano as bigint) as total_programas_stricto_sensu_ano,
    cast(programas_nota_6_7_excelencia_ano as bigint) as programas_nota_6_7_excelencia_ano,
    cast(programas_doutorado_ativo_ano as bigint) as programas_doutorado_ativo_ano,
    cast(ies_ofertantes_pos_graduacao_ano as bigint) as ies_ofertantes_pos_graduacao_ano,
    cast(nota_media_capes_programas as double) as nota_media_capes_programas,
    cast(prop_programas_excelencia_6_7_pct as double) as prop_programas_excelencia_6_7_pct,
    cast(prop_programas_profissionais_pct as double) as prop_programas_profissionais_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
