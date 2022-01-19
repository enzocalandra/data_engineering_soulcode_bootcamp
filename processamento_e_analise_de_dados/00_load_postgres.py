from pyspark.sql import SparkSession, DataFrame
from functools import reduce
import numpy as np
import re


def concat_csv(lista):
    """Função para ler e concatenar arquivos '.csv'
    para geração de dataframes"""
    
    df = reduce(DataFrame.unionAll,lista)
    return df

def fixColName(df):
    """Função para corrigir e substituir caracteres especiais"""
    return (re.sub('([^A-z])', "_", df))

# PARÂMETROS DE CONEXÃO COM O POSTGRESQL
url = ''
db = ''
user = ''
password = ''

# CRIA MÉTODO PARA INSERIR DADOS NAS TABELAS DO POSTGRESQL
def inserirDados(df, table:str):
    """Função para inserir os dados dos dataframes
    nas tabelas criadas no POSTGRESQL"""
    
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{url}:5432/{db}") \
        .option("dbtable", table) \
        .option("driver", "org.postgresql.Driver") \
        .option("user", user) \
        .option("password", password) \
        .save()


# INICIA A SESSÃO SPARK
spark = SparkSession.builder.appName('InserindoPostgreSQL')\
.config('spark.sql.caseSensitive',"True")\
.getOrCreate()

# LÊ OS ARQUIVOS CSV QUE ESTÃO NO BUCKET DA CLOUD STORAGE E OS TRANSFORMA EM DATAFRAME SPARK
reclamacoes = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.load('gs://datalakenatalsoul/reclamacoes.csv')

cidades_ibge = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "ISO-8859-1")\
.load('gs://datalakenatalsoul/ibgecidades.csv')

dados_ibge = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',',')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/municipio.csv')

qualidade = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/qualidade.csv')

vivo = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/Cobertura_VIVO.csv')

tim = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/Cobertura_TIM.csv')

claro = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/Cobertura_CLARO.csv')

oi = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/Cobertura_OI.csv')

nextel = spark.read.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.option("encoding", "UTF-8")\
.load('gs://datalakenatalsoul/Cobertura_NEXTEL.csv')


# REÚNE OS DADOS DE TODAS AS OPERADORAS EM UM SÓ
lista_operadoras = [vivo,claro,tim,oi,nextel]
cobertura_operadoras = concat_csv(lista_operadoras)

for column in reclamacoes.columns:
        reclamacoes = reclamacoes.withColumnRenamed(column, fixColName(column))

for column in cidades_ibge.columns:
        cidades_ibge = cidades_ibge.withColumnRenamed(column, fixColName(column))

for column in dados_ibge.columns:
        dados_ibge = dados_ibge.withColumnRenamed(column, fixColName(column))

for column in qualidade.columns:
        qualidade = qualidade.withColumnRenamed(column, fixColName(column))

for column in cobertura_operadoras.columns:
        cobertura_operadoras = cobertura_operadoras.withColumnRenamed(column, fixColName(column))



# TRANSFORMANDO OS DATAFRAMES SPARK EM ARQUIVOS PARQUET

reclamacoes.write.mode("overwrite").parquet('gs://parquetnatalsoul/Reclamacoes')
reclamacoes = spark.read.parquet('gs://parquetnatalsoul/Reclamacoes')
cidades_ibge.write.mode("overwrite").parquet('gs://parquetnatalsoul/Cidades_ibge')
cidades_ibge = spark.read.parquet('gs://parquetnatalsoul/Cidades_ibge')
dados_ibge.write.mode("overwrite").parquet('gs://parquetnatalsoul/Dados_ibge')
dados_ibge = spark.read.parquet('gs://parquetnatalsoul/Dados_ibge')
qualidade.write.mode("overwrite").parquet('gs://parquetnatalsoul/Qualidade')
qualidade = spark.read.parquet('gs://parquetnatalsoul/Qualidade')
cobertura_operadoras.write.mode("overwrite").parquet('gs://parquetnatalsoul/Cobertura_operadoras')
cobertura_operadoras = spark.read.parquet('gs://parquetnatalsoul/Cobertura_operadoras')

# UTILIZANDO A FUNÇÃO DE INSERIR DADOS PARA POPULAR A BASE DE DADOS NO POSTGRESQL
inserirDados(reclamacoes,'reclamacoes')
inserirDados(cidades_ibge,'cidades_ibge')
inserirDados(dados_ibge,'dados_ibge')
inserirDados(qualidade,'qualidade')
inserirDados(cobertura_operadoras,'cobertura_operadoras')

print('Script Finalizado')