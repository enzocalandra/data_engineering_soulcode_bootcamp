# CLASSE PARA CONEXÃO COM O BANCO DE DADOS MYSQL
# DEFINIÇÃO DE MÉTODOS BÁSICOS PARA REALIZAÇÃO DE CRUD

import mysql.connector

class ConnectorMySQL:
    usuario, senha, host, database = "", "", "", ""
    
# método de conexão ao MySQL
    def __init__(self, usuario, senha, host, database):
        """Construtor da classe ConnectorMySQL

        Args:
            usuario (string): nome de usuário para conexão com a database
            senha (string): senha de acesso a database
            host (string): endereço de conexão com o servidor da database
            database (string): nome do banco de dados que será acessado
        """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.database = database
            
        except Exception as e:
            print(str(e))
    
    # Método de conexão com a database        
    def conectar(self):
        try:
            con = mysql.connector.connect(user = self.usuario, password = self.senha, host = self.host, database = self.database)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))
    
    # Método que encerra a conexão com a database
    def desconectar(self, con, cursor):
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))
    
    # Método que retorna todos as tabelas de uma database  
    def showtables(self):
        try:
            con, cursor = self.conectar()
            query = "SHOW TABLES;"
            cursor.execute(query)
            for row in cursor:
                print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
    
    # Método que retorna todos os dados de uma tabela         
    def selectall(self, tabela):
        try:
            con, cursor = self.conectar()
            query = "SELECT * FROM " + tabela+ ";"
            cursor.execute(query)
            for row in cursor:
                print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
    
    
    # Método genérico para realizar INSERT ou DELETE      
    def executar(self, query):
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
    
    # Método que efetua o SELECT    
    def buscar(self, atributos, tabela, argumentos):
        try:
            con, cursor = self.conectar()
            query = f"SELECT {atributos} FROM {tabela} {argumentos}"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)