with cia as (
    select * from {{ ref('dim_companhia_aberta_cvm') }}
),

bal as (
    select * from {{ ref('fct_balanco_patrimonial_cvm') }}
),

dre as (
    select * from {{ ref('fct_resultado_exercicio_cvm') }}
),

dfc as (
    select * from {{ ref('fct_demonstracao_contabil_cvm') }}
)

select
    cia.codigo_cvm,
    cia.cnpj_14,
    cia.razao_social,
    cia.nome_comercial,
    cia.setor_atividade,
    cia.categoria_registro,
    cia.situacao_registro,
    cia.sigla_uf,
    bal.ano_exercicio,
    bal.tipo_demonstracao,
    bal.ativo_total,
    bal.ativo_circulante,
    bal.caixa_equivalentes,
    bal.estoques,
    bal.passivo_circulante,
    bal.passivo_nao_circulante,
    bal.patrimonio_liquido,
    dre.receita_liquida_vendas,
    dre.lucro_bruto,
    dre.ebitda_calculado,
    dre.lucro_liquido_exercicio,
    dfc.fluxo_caixa_operacional,
    round(bal.ativo_circulante / nullif(bal.passivo_circulante, 0), 2) as indice_liquidez_corrente,
    round((bal.ativo_circulante - bal.estoques) / nullif(bal.passivo_circulante, 0), 2) as indice_liquidez_seca,
    round((dre.lucro_bruto / nullif(dre.receita_liquida_vendas, 0)) * 100.0, 2) as margem_bruta_pct,
    round((dre.lucro_liquido_exercicio / nullif(dre.receita_liquida_vendas, 0)) * 100.0, 2) as margem_liquida_pct,
    round((dre.lucro_liquido_exercicio / nullif(bal.patrimonio_liquido, 0)) * 100.0, 2) as retorno_sobre_patrimonio_roe_pct,
    round(((bal.passivo_circulante + bal.passivo_nao_circulante) / nullif(bal.ativo_total, 0)) * 100.0, 2) as indice_endividamento_geral_pct,
    case
        when (dre.lucro_liquido_exercicio / nullif(bal.patrimonio_liquido, 0)) > 0.15 and (bal.ativo_circulante / nullif(bal.passivo_circulante, 0)) >= 1.2
            then 'SOLVENTE_GRAU_INVESTIMENTO'
        when (dre.lucro_liquido_exercicio / nullif(bal.patrimonio_liquido, 0)) >= 0.0 and (bal.ativo_circulante / nullif(bal.passivo_circulante, 0)) >= 1.0
            then 'ZONA_CINZENTA_MODERADA'
        else 'ALERTA_INSOLVENCIA'
    end as classificacao_solvencia,
    current_timestamp as mart_timestamp
from cia
inner join bal
   on cia.codigo_cvm = bal.codigo_cvm
inner join dre
   on bal.codigo_cvm = dre.codigo_cvm
  and bal.ano_exercicio = dre.ano_exercicio
  and bal.tipo_demonstracao = dre.tipo_demonstracao
left join dfc
   on bal.codigo_cvm = dfc.codigo_cvm
  and bal.ano_exercicio = dfc.ano_exercicio
  and bal.tipo_demonstracao = dfc.tipo_demonstracao
