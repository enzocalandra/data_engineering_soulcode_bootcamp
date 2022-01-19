import os

#função para mostrar o CWD   
def current_path():
    os.getcwd
    print("Current working directory:")
    print(os.getcwd())
    
#função para trocar de pasta
def alter_path(x):
    os.chdir(x)
    print("Alterado a diretorio para: ")
    print(os.getcwd())

# função para listar os arquivos de uma pasta
def list_dir(y):
    os.listdir(y)