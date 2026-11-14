{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(competencia_referencia as varchar) as competencia_referencia,
    cast(indice_integracao_serp_saec_pct as double) as indice_integracao_serp_saec_pct,
    cast(proporcao_titulos_nato_digitais_extrato_pct as double) as proporcao_titulos_nato_digitais_extrato_pct,
    cast(prazo_medio_qualificacao_registral_dias as double) as prazo_medio_qualificacao_registral_dias,
    cast(prazo_medio_certidao_eletronica_horas as double) as prazo_medio_certidao_eletronica_horas,
    cast(ordens_indisponibilidade_cnib_processadas_ano as bigint) as ordens_indisponibilidade_cnib_processadas_ano,
    cast(penhoras_online_arisp_srei_ano as bigint) as penhoras_online_arisp_srei_ano,
    cast(taxa_nota_devolutiva_exigencia_pct as double) as taxa_nota_devolutiva_exigencia_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('onr_registro_imoveis_silver', 'onr_interoperabilidade_serp_eficiencia') }}
