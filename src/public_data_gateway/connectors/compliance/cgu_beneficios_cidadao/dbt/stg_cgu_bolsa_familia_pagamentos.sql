{{ config(materialized='view') }}

select
    cast(cod_programa_social as varchar) as cod_programa_social,
    cast(programa_transferencia_cgu as varchar) as programa_transferencia_cgu,
    cast(modalidade_pagamento as varchar) as modalidade_pagamento,
    cast(situacao_cadunico_elegibilidade as varchar) as situacao_cadunico_elegibilidade,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(familias_beneficiarias_ativas as bigint) as familias_beneficiarias_ativas,
    cast(valor_mensal_transferido_mil_brl as double) as valor_mensal_transferido_mil_brl,
    cast(beneficio_medio_familiar_brl as double) as beneficio_medio_familiar_brl
from {{ source('cgu_beneficios_cidadao', 'cgu_bolsa_familia_pagamentos') }}
