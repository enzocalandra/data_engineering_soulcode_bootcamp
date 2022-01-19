from modules.connector import ConnectorMySQL


if __name__ == "__main__":
    try:
        conexaobanco = ConnectorMySQL("user","senha","host","db")
        
        while True:
            print("[1] - Cadastrar novo cliente")
            print("[2] - Porcentagem de clientes fora do prazo de fidelidade")
            print("[3] - Listar clientes fora do prazo de fidelidade")
            print("[4] - Clientes assinantes do plano mais caro")
            print("[0] - Encerrar")
            opcao = int(input("Digite sua opção: "))
            
            if opcao == 1:
                nomecliente = input("Digite o nome do cliente: ")
                datacadastro = input("Digite a data de cadastro (DD/MM/AAAA): ")
                idplano = input("Digite o ID do plano: ")
                dataplano = input("Digite a data do plano (DD/MM/AAAA): ")
                valores = "'"+nomecliente+"', '"+datacadastro+"', '"+idplano+"', '"+dataplano+"'"
                conexaobanco.insert("cliente", "(nome_cliente, data_cadastro, id_plano, data_plano)", valores)
                print("\nCLIENTE CADASTRADO.\n")
                
            
            elif opcao == 2:
                clientesFora = conexaobanco.clientesFora()
                totalClientes = conexaobanco.totalClientes()
                porcentagem = (clientesFora[0][0]/totalClientes[0][0])*100
                print("Porcentagem de clientes com planos fora do prazo de fidelidade:", round(porcentagem, 2),"%")
            
            elif opcao == 3:
                print(conexaobanco.listarClientes())
            
            elif opcao == 4:
                print(conexaobanco.clientesPlanoCaro())
            
            elif opcao == 0:
                break
            
            else:
                print("Opção inválida.")
            
        
    except Exception as erro:
        print("erro: ", str(erro))