{{ config(materialized='table') }}

select
    p.numero_processo,
    p.tribunal_sigla,
    p.grau_jurisdicao,
    p.classe_codigo_tpu,
    p.classe_nome,
    p.data_ajuizamento,
    coalesce(p.valor_causa, 0.0) as valor_causa,
    p.orgao_julgador_nome,
    p.orgao_julgador_municipio_ibge,
    p.sistema_origem,
    p.is_segredo_justica,
    p.situacao_processual,
    p.polo_ativo_tipo,
    p.polo_passivo_documento,
    p.polo_passivo_nome,
    case
        when p.classe_codigo_tpu = 1116 then 'EXECUCAO_FISCAL'
        when p.classe_codigo_tpu = 985 then 'ACAO_TRABALHISTA'
        when p.classe_codigo_tpu = 128 then 'RECUPERACAO_JUDICIAL'
        else 'CIVEL_E_OUTROS'
    end as categoria_demanda,
    case
        when p.situacao_processual = 'ATIVO_EM_ANDAMENTO' then true
        else false
    end as is_processo_ativo,
    p.extracted_at
from {{ ref('stg_cnj_processos') }} p
