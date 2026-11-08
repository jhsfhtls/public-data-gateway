{{ config(materialized='table') }}

select
    c.numero_certidao,
    c.ano_certidao,
    c.cnpj_ou_cpf,
    c.cnpj_raiz,
    c.razao_social_ou_nome,
    c.tipo_documento,
    c.status_certidao,
    c.is_regular,
    c.data_expedicao,
    c.hora_expedicao,
    c.data_validade,
    c.codigo_autenticidade,
    c.orgao_emissor,
    s.permite_licitar,
    s.nivel_risco_trabalhista,
    case
        when c.data_validade >= current_date and c.is_regular then true
        else false
    end as is_certidao_ativa_valida,
    c.ingestion_timestamp
from {{ ref('stg_tst_cndt_certidoes') }} c
left join {{ ref('stg_tst_cndt_status_dominio') }} s
    on c.status_certidao = s.codigo_status
