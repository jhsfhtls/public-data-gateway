{{ config(materialized='table') }}

with tipos_raw as (
    select
        'CEIS_INIDONEIDADE' as id_tipo_sancao,
        'Declaração de Inidoneidade' as nome_tipo_sancao,
        'CEIS' as cadastro_origem,
        'Lei nº 14.133/2021, Art. 156, IV / Lei nº 8.666/1993, Art. 87, IV' as amparo_legal_padrao,
        'Impedimento de licitar e contratar com toda a Administração Pública direta e indireta de todos os entes federativos' as alcance_sancao,
        'GRAVISSIMA' as nivel_gravidade,
        1 as ranking_gravidade
    union all
    select
        'CEIS_IMPEDIMENTO' as id_tipo_sancao,
        'Impedimento de Licitar e Contratar' as nome_tipo_sancao,
        'CEIS' as cadastro_origem,
        'Lei nº 14.133/2021, Art. 156, III / Lei nº 10.520/2002, Art. 7º' as amparo_legal_padrao,
        'Impedimento de licitar e contratar no âmbito do ente federativo prolator da sanção pelo prazo de até 3 anos' as alcance_sancao,
        'GRAVE' as nivel_gravidade,
        2 as ranking_gravidade
    union all
    select
        'CEIS_SUSPENSAO' as id_tipo_sancao,
        'Suspensão Temporária de Participação em Licitação' as nome_tipo_sancao,
        'CEIS' as cadastro_origem,
        'Lei nº 8.666/1993, Art. 87, III' as amparo_legal_padrao,
        'Suspensão de licitar e impedimento de contratar com o órgão ou entidade sancionadora por até 2 anos' as alcance_sancao,
        'MEDIA' as nivel_gravidade,
        3 as ranking_gravidade
    union all
    select
        'CNEP_MULTA_ANTICORRUPCAO' as id_tipo_sancao,
        'Multa e Publicação Extraordinária (Lei Anticorrupção)' as nome_tipo_sancao,
        'CNEP' as cadastro_origem,
        'Lei nº 12.846/2013, Art. 6º, I e II' as amparo_legal_padrao,
        'Multa pecuniária de 0,1% a 20% do faturamento bruto e publicação extraordinária da condenação por atos lesivos contra a administração pública' as alcance_sancao,
        'GRAVISSIMA' as nivel_gravidade,
        1 as ranking_gravidade
    union all
    select
        'CEPIM_IMPEDIMENTO_REPASSE' as id_tipo_sancao,
        'Impedimento de Convênios e Parcerias (CEPIM)' as nome_tipo_sancao,
        'CEPIM' as cadastro_origem,
        'Lei nº 13.019/2014, Art. 73 / Decreto Federal nº 7.592/2011' as amparo_legal_padrao,
        'Suspensão e impedimento de celebração de convênios, parcerias e termos de fomento decorrente de inadimplência ou irregularidade' as alcance_sancao,
        'GRAVE' as nivel_gravidade,
        2 as ranking_gravidade
)

select * from tipos_raw
