from modules.connector_mysql import ConnectorMySQL
from modules.connector_cassandra import ConnectorCassandra
import modules.functions as function
import pandas as pd
import numpy as np

if __name__ == '__main__':
    try:
    # Altera o caminho da pasta
        function.alter_path('data/')
        
    # Efetua a leitura da planilha Sistema_A_SQL e atribui os valores para o dataframe 'df_dados'
        df_dados = pd.read_csv('Sistema_A_SQL.csv', sep=",")
        
        function.alter_path('../')
        
    # Exibe as primeiras informações do dataframe 'df_dados'
        print(df_dados.head())
        
    # Retorna a quantidade de linhas e colunas
        print(df_dados.shape)
        
    # Retorna a quantidade de dados
        print(df_dados.count())
        
    # Retorna a quantidade de dados nulos por coluna do dataframe 'df_dados'
        print(df_dados.isnull().sum())
    
    ########################################
    # Conexão com a database no MySQL
    ########################################
        conexao_mysql = ConnectorMySQL('usuario', 'senha', 'host', 'db')
    # Efetua o INSERT no MySQL
        for index, row in df_dados.iterrows():
            if row[1] is np.NaN:
                conexao_mysql().executar(f"INSERT INTO SISTEMA (nota_fiscal, total) VALUES ({row[0]}, {row[2]})")
            else:
                conexao_mysql().executar(f"INSERT INTO SISTEMA (nota_fiscal, vendedor, total) VALUES ({row[0]}, '{row[1]}', {row[2]})")
        
    # Retorna os dados da database do MySQL
        dados_ordenados = conexao_mysql().buscar("*", "sistema", "")
        
    # Insere os dados retornados da database do MySQL em um DataFrame Pandas
        df_dados_mysql = pd.DataFrame(dados_ordenados, columns=['nota_fiscal', 'vendedor', 'total'])
        
    # Retorna a quantidade de dados nulos por coluna do dataframe 'df_dados_mysql'
        print(df_dados_mysql.isnull().sum())
        
    # Remove as linhas do dataframe que contem algum valor nulo
        df_dados_mysql.dropna(inplace=True)
        
    ########################################
    # Conexão com a keyspace no CASSANDRA
    ########################################
        conexao_cassandra = ConnectorCassandra()
        
    # Retorna com os dados que já existiam na tabela da keyspace
        dados_cassandra = conexao_cassandra.select("SELECT * FROM sistema;")
        
    # Insere os dados retornados em um dataframe Pandas
        df_dados_cassandra = pd.DataFrame(dados_cassandra, columns=['nota_fiscal','total', 'vendedor'])
        
    # Ordena os dados do 'df_dados_cassandra' pela coluna 'nota_fiscal'
        df_dados_cassandra = df_dados_cassandra.sort_values(by='nota_fiscal')
        print(df_dados_cassandra.head())
        print(df_dados_cassandra.isnull().sum())
        
    # Remove as linhas do dataframe que contem algum valor nulo e verifica se foram removidos
        df_dados_cassandra.dropna(inplace=True)
        print(df_dados_cassandra.isnull().sum())
        
    # Junta os dados em um novo dataframe
        dataframe_completo = pd.concat([df_dados_mysql, df_dados_cassandra])
        print(dataframe_completo.count())
        
    # INSERT na família DADOS_TRATADOS no banco CASSANDRA
        for index, row in dataframe_completo.iterrows():
            query = f"INSERT INTO nome_da_keyspace.dados_tratados (sk, nota_fiscal, vendedor, total) VALUES (uuid(), '{row[0]}', '{row[1]}', {row[2]});"
            #print(query)
            conexao_cassandra.inserir(query)
        
    except Exception as e:
        print(str(e))