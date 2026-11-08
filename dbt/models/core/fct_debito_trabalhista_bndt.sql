{{ config(materialized='table') }}

select
    d.id_debito_bndt,
    d.numero_certidao,
    d.cnpj_ou_cpf,
    d.numero_processo_nup,
    d.tribunal_origem,
    t.nome_tribunal,
    t.jurisdicao_uf,
    t.regiao_brasil,
    d.vara_trabalho,
    d.tipo_divida_trabalhista,
    d.valor_debito,
    d.situacao_exigibilidade,
    d.data_inclusao_bndt,
    case
        when d.situacao_exigibilidade = 'EXIGIVEL' then true
        else false
    end as is_debito_exigivel,
    d.ingestion_timestamp
from {{ ref('stg_tst_cndt_debitos_bndt') }} d
left join {{ ref('stg_tst_cndt_tribunais_regionais') }} t
    on d.tribunal_origem = t.sigla_trt
