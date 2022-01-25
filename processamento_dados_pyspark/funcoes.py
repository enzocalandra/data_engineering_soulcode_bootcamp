__author__ = 'André Ventura'
__version__ = '1.0.1'
__maintainer__ = "André Ventura"
__status__ = "Production"


from pyspark.sql import DataFrame
from functools import reduce


def rename_column(df):
    """Função para transformar os nomes das colunas de
    um dataframe através da criação de um dicionário que
    implementa substituição de caracteres"""
    
    dicionario_caracteres = {'Á':'A', 'Ã':'A', 'Â':'A', 'À':'A',
                            'É':'E','Ê':'E', 'Ë':'E',
                            'Í':'I',
                            'Ó':'O','Ô':'O',
                            'Ú':'U', 'Ü':'U',
                            ' ':'_'}
    lista_colunas = [coluna[0] for coluna in df.dtypes]
    lista_colunas_renomeada = []
    for coluna in lista_colunas:
        for key,value in dicionario_caracteres.items():
            coluna = coluna.replace(key,value)
        lista_colunas_renomeada.append(coluna)
    return lista_colunas_renomeada


def concat_csv(sessao,dir,lista):
    """Função para ler e concatenar arquivos '.csv'
    para geração de dataframes"""
    
    lista_dfs = []
    for i in lista:
        csv = sessao.read.format('csv').option('header','True').option("encoding", "ISO-8859-1").option('delimiter',';').load(f'C:/Atividade/data/{dir}/{i}')
        lista_dfs.append(csv)
    df = reduce(DataFrame.unionAll,lista_dfs)
    return df