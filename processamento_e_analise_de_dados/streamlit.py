import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px

st.set_page_config(page_title='Apresentação Deloitte',
                    page_icon=':bar_chart:',
                    layout='wide')


@st.cache
def get_data_from_parquet(table:str):
    df = pd.read_parquet(f"https://storage.googleapis.com/parquetapresentacao/{table}")
    return df


st.title('Atividade Final de Ano - SoulCode')

recmes = get_data_from_parquet('recmes')
recass = get_data_from_parquet('x2')
pib17 = get_data_from_parquet('17ibge')
pib18 = get_data_from_parquet('18ibge')
df_qualidade = get_data_from_parquet('qualidade')

cobertura, reclamacao, qualidade, dados_ibge = 3426794, 2590318, 966680, 11140
total = sum([cobertura,reclamacao,qualidade,dados_ibge])
meses = ['Janeiro','Feveireo','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']

st.write('## Volume de dados')

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Cobertura", cobertura)
col2.metric("Dados IBGE", dados_ibge)
col3.metric("Qualidade", qualidade)
col4.metric("Reclamações", reclamacao)
col5.metric("Total", total)
col6, col7, col8, col9, col10 = st.columns(5)
col6.metric("", "48.98%")
col7.metric("", "0.15%")
col8.metric("", "13.81%")
col9.metric("","37.03%")
col10.metric("", "100%")

st.markdown('------------')
st.write('## 📊 Relatório de reclamações')

recmes = recmes[recmes.uf != "  "]
top10 = pib18['uf'].head(10).unique()
uf = st.sidebar.multiselect(
    "Selecione uma estado",
    options=recmes['uf'].sort_values().unique(),
    default=top10)
df_selection = recmes.query("uf == @uf").groupby(["marca","mes"], as_index=False)["count"].sum()

fig = px.line(df_selection, range_x=[1,12], x='mes', y='count', color='marca', markers=True, 
            labels={
                     "mes": "Ano 2018",
                     "count": "N° de reclamações",
                     "marca": "Operadoras"},
                    title='Número de reclamação/mês')

fig.update_xaxes(tickangle=-45, dtick=1, visible=True, fixedrange=False)
fig.update_yaxes(dtick=10000, visible=True, fixedrange=False)
fig.update_layout(
    xaxis = dict(
        tickmode = 'array',
        tickvals = [1,2,3,4,5,6,7,8,9,10,11,12],
        ticktext = meses))
st.plotly_chart(fig)

vivo_reclamacoes = recass.query('marca == "VIVO"').groupby(['assunto']).count()
claro_reclamacoes = recass.query('marca == "CLARO"').groupby(['assunto']).count()
tim_reclamacoes = recass.query('marca == "TIM"').groupby(['assunto']).count()
oi_reclamacoes = recass.query('marca == "OI"').groupby(['assunto']).count()

st.write('### VIVO - Anual')
col11,col12,col13,col14,col15 = st.columns(5)
col11.metric("Cancelamento",vivo_reclamacoes['mes'].iloc[0])
col12.metric("Cobrança",vivo_reclamacoes['mes'].iloc[1])
col13.metric("Oferta, Bônus e Mudança de Plano",vivo_reclamacoes['mes'].iloc[2])
col14.metric("Qualidade, Funcionamento e Reparo",vivo_reclamacoes['mes'].iloc[3])
col15.metric("Total", sum([vivo_reclamacoes['mes'].iloc[0],vivo_reclamacoes['mes'].iloc[1],vivo_reclamacoes['mes'].iloc[2],vivo_reclamacoes['mes'].iloc[3]]))

st.write('### CLARO - Anual')
col16,col17,col18,col19,col20 = st.columns(5)
col16.metric("Cancelamento",claro_reclamacoes['mes'].iloc[0])
col17.metric("Cobrança",claro_reclamacoes['mes'].iloc[1])
col18.metric("Oferta, Bônus e Mudança de Plano",claro_reclamacoes['mes'].iloc[2])
col19.metric("Qualidade, Funcionamento e Reparo",claro_reclamacoes['mes'].iloc[3])
col20.metric("Total", sum([claro_reclamacoes['mes'].iloc[0],claro_reclamacoes['mes'].iloc[1],claro_reclamacoes['mes'].iloc[2],claro_reclamacoes['mes'].iloc[3]]))

st.write('### TIM - Anual')
col21,col22,col23,col24,col25 = st.columns(5)
col21.metric("Cancelamento",tim_reclamacoes['mes'].iloc[0])
col22.metric("Cobrança",tim_reclamacoes['mes'].iloc[1])
col23.metric("Oferta, Bônus e Mudança de Plano",tim_reclamacoes['mes'].iloc[2])
col24.metric("Qualidade, Funcionamento e Reparo",tim_reclamacoes['mes'].iloc[3])
col25.metric("Total", sum([tim_reclamacoes['mes'].iloc[0],tim_reclamacoes['mes'].iloc[1],tim_reclamacoes['mes'].iloc[2],tim_reclamacoes['mes'].iloc[3]]))

st.write('### OI - Anual')
col26,col27,col28,col29,col30 = st.columns(5)
col26.metric("Cancelamento",oi_reclamacoes['mes'].iloc[0])
col27.metric("Cobrança",oi_reclamacoes['mes'].iloc[1])
col28.metric("Oferta, Bônus e Mudança de Plano",oi_reclamacoes['mes'].iloc[2])
col29.metric("Qualidade, Funcionamento e Reparo",oi_reclamacoes['mes'].iloc[3])
col30.metric("Total", sum([oi_reclamacoes['mes'].iloc[0],oi_reclamacoes['mes'].iloc[1],oi_reclamacoes['mes'].iloc[2],oi_reclamacoes['mes'].iloc[3]]))
st.markdown('------------')

vivo_cumpriu = df_qualidade.query('empresa == "VIVO" and ano == 2018').groupby(['empresa','uf','ano']).count().sum()
claro_cumpriu = df_qualidade.query('empresa == "CLARO" and ano == 2018').groupby(['empresa','uf','ano']).count().sum()
tim_cumpriu = df_qualidade.query('empresa == "TIM" and ano == 2018').groupby(['empresa','uf','ano']).count().sum()
oi_cumpriu = df_qualidade.query('empresa == "OI" and ano == 2018').groupby(['empresa','uf','ano']).count().sum()

st.write('## Qualidade Anatel 2018')
fig = go.Figure(go.Bar(
            x=[26.76,26.16, 23.61,23.46],
            y=['CLARO', 'VIVO', 'TIM', 'OI'],
            orientation='h'))
st.plotly_chart(fig)
col31,col32,col33,col34 = st.columns(4)
col31.metric("VIVO","26.16%")
col32.metric("CLARO", "26.76%")
col33.metric("TIM","23.61%")
col34.metric("OI","23.46%")