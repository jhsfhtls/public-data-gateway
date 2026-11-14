{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(competencia_referencia as varchar) as competencia_referencia,
    cast(serventias_ri_integradas_srei as bigint) as serventias_ri_integradas_srei,
    cast(total_matriculas_ativas_cnm as bigint) as total_matriculas_ativas_cnm,
    cast(matriculas_urbanas_ativas as bigint) as matriculas_urbanas_ativas,
    cast(matriculas_rurais_georreferenciadas as bigint) as matriculas_rurais_georreferenciadas,
    cast(taxa_conversao_cnm_digital_pct as double) as taxa_conversao_cnm_digital_pct,
    cast(novas_matriculas_abertas_ano as bigint) as novas_matriculas_abertas_ano,
    cast(certidoes_digitais_srei_ano as bigint) as certidoes_digitais_srei_ano,
    cast(visualizacoes_matricula_online_ano as bigint) as visualizacoes_matricula_online_ano,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('onr_registro_imoveis_silver', 'onr_matriculas_acervo_municipio') }}
