with fund_cad as (
    select * from {{ ref('dim_fundo_investimento') }}
),
fund_stats as (
    select
        cnpj_14,
        min(valor_cota) as menor_cota_periodo,
        max(valor_cota) as maior_cota_periodo,
        sum(captacao_dia) as total_captacao_periodo,
        sum(resgate_dia) as total_resgate_periodo,
        sum(captacao_liquida_dia) as saldo_liquido_captacao,
        avg(numero_cotistas) as media_cotistas,
        max(patrimonio_liquido) as pl_maximo_periodo,
        count(distinct data_competencia) as dias_apurados
    from {{ ref('fct_informe_diario_fundos') }}
    group by cnpj_14
)

select
    c.cnpj_14,
    c.codigo_cvm,
    c.razao_social,
    c.classe_cvm,
    c.classe_anbima,
    c.situacao_registro,
    c.tipo_condominio,
    c.publico_alvo,
    c.cnpj_gestor,
    c.nome_gestor,
    c.cnpj_administrador,
    c.nome_administrador,
    c.patrimonio_liquido as patrimonio_liquido_atual,
    s.menor_cota_periodo,
    s.maior_cota_periodo,
    s.total_captacao_periodo,
    s.total_resgate_periodo,
    s.saldo_liquido_captacao,
    cast(round(s.media_cotistas, 0) as integer) as media_cotistas,
    s.dias_apurados,
    case
        when s.saldo_liquido_captacao > 0 then 'CAPTAÇÃO LÍQUIDA POSITIVA'
        when s.saldo_liquido_captacao < 0 then 'RESGATE LÍQUIDO NEGATIVO'
        else 'EQUILIBRADO'
    end as status_fluxo_captacao,
    case
        when c.tipo_condominio = 'ABERTO' and c.patrimonio_liquido >= 1000000000.0 and s.media_cotistas >= 10000 then 'ALTA_LIQUIDEZ'
        when c.tipo_condominio = 'ABERTO' and c.patrimonio_liquido >= 100000000.0 then 'MEDIA_LIQUIDEZ'
        when c.tipo_condominio = 'FECHADO' or c.fundo_exclusivo = 'S' then 'RESTRITA'
        else 'MEDIA_LIQUIDEZ'
    end as perfil_liquidez
from fund_cad c
left join fund_stats s on c.cnpj_14 = s.cnpj_14
