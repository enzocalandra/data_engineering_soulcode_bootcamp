import mysql.connector

class ConnectorMySQL:
    usuario, senha, host, banco = "", "", "", ""
    
# método de conexão ao MySQL
    def __init__(self, usuario, senha, host, banco):
        """Construtor da classe ConnectorMySQL

        Args:
            usuario (string): nome de usuário para conexão com o banco de dados
            senha (string): senha de acesso ao banco de dados
            host (string): endereço de conexão com o servidor do banco de dados
            banco (string): nome do banco de dados que será acessado
        """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
            
        except Exception as e:
            print(str(e))
            
    def conectar(self):
        try:
            con = mysql.connector.connect(user = self.usuario, password = self.senha, host = self.host, database = self.banco)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))
            
    def desconectar(self, con, cursor):
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))
    
    
    
    def select(self, coluna, tabela, dados):
        try:
            con, cursor = self.conectar()
            query = "SELECT " + coluna + " FROM " + tabela + dados + ";"
            cursor.execute(query)
            for row in cursor:
                print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def clientesFora (self):
        try:
            con, cursor = self.conectar()
            query = "select count(id_cliente) from cliente where curdate()-data_plano > 365;"
            cursor.execute(query)
            #for row in cursor:
            #    print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
    
    def totalClientes (self):
        try:
            con, cursor = self.conectar()
            query = "SELECT count(id_cliente) FROM cliente;"
            cursor.execute(query)
            #for row in cursor:
            #    print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def clientesPlanoCaro (self):
        try:
            con, cursor = self.conectar()
            query = "select c.id_cliente, c.nome_cliente, p.preco_plano  from cliente c, plano p where p.id_plano = c.id_plano and p.preco_plano = (select max(preco_plano) from plano);"
            cursor.execute(query)
            for row in cursor:
                print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def listarClientes (self):
        try:
            con, cursor = self.conectar()
            query = "select nome_cliente, data_plano from cliente where curdate() - data_plano > 365;"
            cursor.execute(query)
            for row in cursor:
                print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def insert(self, tabela, colunas, dados):
        try:
            con, cursor = self.conectar()
            query = "INSERT INTO " + tabela + colunas + " VALUES ("+dados+");"
            cursor.execute(query)
            #for row in cursor:
            #    print(str(row))
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
    
    def update(self, novo_dado, outros_dados):
        try:
            con, cursor = self.conectar()
            query = "UPDATE produtos SET estoque = "+novo_dado+" WHERE referencia = "+outros_dados+";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def delete(self, tabela, atributos):
        try:
            con, cursor = self.conectar()
            query = "DELETE FROM " + tabela + "WHERE "+ atributos +";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)