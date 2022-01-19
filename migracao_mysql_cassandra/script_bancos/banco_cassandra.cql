-- Cria o banco (keyspace) no Cassandra
CREATE KEYSPACE IF NOT EXISTS migracao_dados WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Selecionar a keyspace no Cassandra
USE migracao_dados;

-- Cria familia sistema
CREATE TABLE IF NOT EXISTS migracao_dados.sistema(
    nota_fiscal int primary key,
    vendedor text,
    total float,
);

-- Carrega e insere os dados locais no banco
COPY migracao_dados.sistema (nota_fiscal, vendedor, total) 
FROM 'C:\Users\seuusuarioaqui\Documents\...\Sistema_B_NoSQL.csv' WITH DELIMITER=',' AND HEADER=TRUE;

-- Cria familia dados_tratados
CREATE TABLE IF NOT EXISTS migracao_dados.dados_tratados(
    sk uuid PRIMARY KEY,
    nota_fiscal text,
    vendedor text,
    total float,
);


