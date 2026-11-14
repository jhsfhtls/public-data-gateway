{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(competencia_referencia as varchar) as competencia_referencia,
    cast(natureza_ato_registral as varchar) as natureza_ato_registral,
    cast(categoria_operacao as varchar) as categoria_operacao,
    cast(quantidade_atos_registrados_ano as bigint) as quantidade_atos_registrados_ano,
    cast(volume_financeiro_declarado_milhoes_brl as double) as volume_financeiro_declarado_milhoes_brl,
    cast(ticket_medio_ato_brl as double) as ticket_medio_ato_brl,
    cast(proporcao_protocolo_eletronico_srei_pct as double) as proporcao_protocolo_eletronico_srei_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('onr_registro_imoveis_silver', 'onr_transacoes_atos_natureza') }}
