from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from funcoes import rename_column, concat_csv
from pyspark.sql import functions as F
import os

# USANDO UMA FUNÇÃO DO MÓDULO 'OS' PARA LIMPAR A TELA NO INÍCIO DA EXECUÇÃO DO CÓDIGO
os.system("cls")

# INSTANCIANDO OBJETOS PARA REALIZAR A CONEXÃO COM O CLUSTER
conf = SparkConf().setAppName('AplicacaoEnzo').setMaster("spark://ip:porta")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# USANDO A FUNÇÃO CRIADA 'concat_csv' PARA LER TODOS OS CSVs DE MANEIRA AUTOMÁTICA E GERAR OS DATAFRAMES
df_pesca = concat_csv(spark,"segurodefeso",os.listdir('data/segurodefeso'))
df_receitas = concat_csv(spark,"receitas",os.listdir('data/receitas'))

# USANDO A FUNÇÃO CRIADA 'rename_column' PARA PADRONIZAR OS NOMES DAS COLUNAS NOS DATAFRAMES CRIADOS
lista_colunas = rename_column(df_pesca)
df_pesca = df_pesca.toDF(*lista_colunas)
lista_colunas = rename_column(df_receitas)
df_receitas = df_receitas.toDF(*lista_colunas)

# TRANSFORMANDO OS DATAFRAMES EM ARQUIVOS DO TIPO 'PARQUET' E LENDO ESTES ARQUIVOS
df_pesca.write.parquet('C:/Atividade_17/data/DadosSeguroDefeso')
df_parquet_pesca = spark.read.parquet('C:/Atividade_17/data/DadosSeguroDefeso')
df_receitas.write.parquet('C:/Atividade_17/data/DadosReceitas')
df_parquet_receitas = spark.read.parquet('C:/Atividade_17/data/DadosReceitas')

# SUBSTITUINDO VÍRGULAS POR PONTOS NOS CAMPOS DE VALORES NUMÉRICOS DOS ARQUIVOS 'PARQUET' E TRANSFORMANDO ESTES DADOS PARA O TIPO FLOAT (CASTING)
df_parquet_pesca = df_parquet_pesca.withColumn('VALOR_PARCELA', F.regexp_replace('VALOR_PARCELA', ',', '.').cast('Float'))
df_parquet_receitas = df_parquet_receitas.withColumn('VALOR_REALIZADO', F.regexp_replace('VALOR_REALIZADO', ',', '.').cast('Float'))

# MOSTRANDO A CONTAGEM TOTAL DE LINHAS DOS ARQUIVOS
print(df_parquet_pesca.count())
print(df_parquet_receitas.count())


# MENU CRIADO PARA POSSIBILITAR CONSULTAS ESPECÍFICAS
while True:
    menu= """
############# MENU DE OPÇÕES #############:

    [1] - Mostrar valores nulos
    [2] - Buscar pelo NIS do beneficiário do Seguro Defeso
    [3] - Buscar receitas pelo código do órgão superior
    [0] - Encerrar"""
    print(menu)
    opcao = int(input("\nDigite aqui sua opção: "))
    
    if opcao == 1: # MOSTRAR VALORES NULOS 
        print('QUANTIDADE DE VALORES NULOS NOS DADOS DO SEGURO DEFESO\n')
        for coluna in df_parquet_pesca.dtypes:
            nulos = df_parquet_pesca.filter(df_parquet_pesca[coluna[0]].isNull()).count()
            print(f"{coluna[0]}: {nulos}")
        print("")
        print('QUANTIDADE DE VALORES NULOS NOS DADOS DAS RECEITAS\n')
        for coluna in df_parquet_receitas.dtypes:
            nulos = df_parquet_receitas.filter(df_parquet_receitas[coluna[0]].isNull()).count()
            print(f"{coluna[0]}: {nulos}")
        print("")
        if input("Selecionar nova opção? (S/N): ").upper() != "S":
            break
            
    elif opcao == 2: # BUSCAR PESSOA PELO NÚMERO DO NIS
        dados = df_parquet_pesca.filter(F.col('NIS_FAVORECIDO') == input('Digite o NIS: ')).groupBy(['NIS_FAVORECIDO','MES_REFERENCIA','UF','NOME_FAVORECIDO']).agg(F.count('VALOR_PARCELA'),F.sum('VALOR_PARCELA'),F.min('VALOR_PARCELA'),F.max('VALOR_PARCELA'))
        dados.show(truncate=False)
        if input("Selecionar nova opção? (S/N): ").upper() != "S":
            break

    elif opcao == 3: # BUSCAR PELO CÓDIGO DO ÓRGÃO SUPERIOR
        dados = df_parquet_receitas.filter(F.col('CODIGO_ORGAO_SUPERIOR') == input('Digite o código do órgão superior: ')).groupBy(['CODIGO_ORGAO_SUPERIOR','NOME_ORGAO_SUPERIOR','CODIGO_ORGAO','NOME_ORGAO']).agg(F.count('VALOR_REALIZADO'),F.sum('VALOR_REALIZADO'),F.min('VALOR_REALIZADO'),F.max('VALOR_REALIZADO'))
        dados.show(truncate=True)
        if input("Selecionar nova opção? (S/N): ").upper() != "S":
            break
    
    elif opcao == 0:
        break
    
    else:
        print("Opção inválida. Tente novamente")