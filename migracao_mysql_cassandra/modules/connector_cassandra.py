# CLASSE PARA CONEXÃO COM O BANCO DE DADOS CASSANDRA
# DEFINIÇÃO DE MÉTODOS BÁSICOS PARA REALIZAÇÃO DE CRUD

from cassandra.cluster import Cluster

class ConnectorCassandra:
    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('nome_da_keyspace')
    

    def inserir(self, query_insert):
        """Método para realizar inserções

        Args:
            tabela: tabela em que será inserido o valor
            atributos: campos da tabela em que serão inseridos valores
            dados: valores que serão inseridos
        """
        try:
            self.session.execute(query_insert)
        except Exception as e:
            print(str(e))
    
    
    def selecionar(self, query_select):
        """Método genérico para realizar uma query de consulta SELECT
        
        O usuário fica livre para construir a query como achar melhor,
        passando-a como parâmetro do método

        Args:
            query_select ([string]): Query que será realizada no banco
        """
        lista_select = []
        retorno_query = self.session.execute(query_select)
        for i in retorno_query:
            lista_select.append(i)
        for linha in lista_select:
            print(linha)
    
            
    def deletar(self, tabela, coluna, dados):
        """Método para excluir

        Args:
            tabela: tabela onde será realizada a exclusão de dados
            coluna: coluna de referência
            dados: valor da coluna de referência
        """
        try:
            query = "DELETE FROM "+tabela+" WHERE "+coluna+" = "+dados+";"
            self.session.execute(query)
        except Exception as e:
            print(str(e))
       
            
    def atualizar(self, tabela, coluna_alterar, novo_valor, coluna_ref, dados_ref):
        """Método de atualização/alteração

        Args:
            tabela: tabela que contém as colunas a serem atualizadas
            coluna_alterar = coluna que vai receber atualização de dados
            novo_valor: novo dado que será inserido na atualização (se string, colocar entre aspas simples)
            coluna_ref: coluna de referência (geralmente a que contém a PK)
            dados_ref: valor da coluna de referência (se string, colocar entre aspas simples)
            
            Exemplo de query: "UPDATE alunos SET nome = 'TesteUpdate' WHERE matricula = 01010101"
        """
        try:
            query = "UPDATE "+tabela+" SET "+coluna_alterar+ " = "+novo_valor+" WHERE "+coluna_ref+" = "+dados_ref+";"
            self.session.execute(query)
        except Exception as e:
            print(str(e))