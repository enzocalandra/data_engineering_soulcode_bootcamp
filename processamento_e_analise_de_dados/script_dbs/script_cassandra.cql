--CRIAÇÃO DA BASE DE DADOS NO BANCO CASSANDRA

create keyspace if not exists telecomunicacao
WITH replication = {
 'class':'SimpleStrategy',
'replication_factor': 1
};

use telecomunicacao;


-- FAMÍLIA DADOS_IBGE
create table if not exists "telecomunicacao"."dados_ibge"(
id uuid primary key,
uf text,
ano int,
nome_municipio text,
populacao bigint,
cod_municipio int,
cod_uf int,
cod_ibge bigint,
pib_municipio bigint
);
-- uf, nome_municipio, populacao, cod_municipio, id_uf, pib_municipio


-- FAMÍLIA QUALIDADE
create table if not exists "telecomunicacao"."qualidade"(
id_qualidade uuid primary key,
servico text,
empresa text,
ano int,
uf text,
meta_indicador text, 
cumpriu int,
descumpriu int
);
-- servico, empresa, ano, uf, meta_indicador, cumpriu, descumpriu


-- FAMÍLIA RECLAMACOES
create table if not exists "telecomunicacao"."reclamacoes"(
id_reclamacao uuid primary key,
ano int,
mes int, 
uf text,
cidade text,
cod_municipio int,  
canal text,   
marca text, 
assunto text
);
-- ano, mes, uf, cidade, codigo_municipio, canal_entrada, marca, assunto 

-- FAMÍLIA COBERTURA_OPERADORAS
create table if not exists "telecomunicacao"."cobertura_operadoras"(
id_cobertura uuid primary key,
ano int, 
operadora text,
tecnologia text, 
setor text,
cod_uf int,
cod_municipio int,  
uf text,
regiao text,
domicilios int,
moradores int,
percentual_cobertura text
);
-- ano, operadora, tecnologia, tipo_setor, cod_municipio, UF, regiao, domicilios, moradores, percentual_cobertura
