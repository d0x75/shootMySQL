import sys
import pymysql
# CRIAÇÃO DE UMA FUNÇÃO DE PARSING
def parse_sql(filename):
    data = open(filename, 'r').readlines()
    stmts = []
    DELIMITER = ';'
    stmt = ''

    # INICIANDO O LOOP, E VERIFICANDO SE HÁ ALGUM ESPAÇO EM BRANCO NA LINHA , SE HOUVER INTERROMPE A ITERAÇÃO ATUAL.

    for lineno, line in enumerate(data):
        if not line.strip():
            continue
    # CONDICIONAL QUE VERIFICA SE HÁ UM COMENTÁRIO ' -- ' NO INICIO DA LINHA, SE HOUVER INTERROMPE A ITERAÇÃO ATUAL.
        if line.startswith('--'):
            continue
    # CONDICIONAL QUE VERIFICA SE HÁ A PALAVRA 'DELIMITER' NA LINHA .. FAZER ALGO PARA TRATAR CASO TENHA.!!!!!!
        if 'DELIMITER' in line:
            DELIMITER = line.split()[1]
            continue
    # CONDICIONAL QUE VERIFICA SE HÁ O VALOR DA VARIÁVEL DELIMITER ';' NA LINHA, E VAI IR SALVANDO NA VARIÁVEL stmt AS LINHAS QUE NÃO TIVEREM ' ; ' 

        if (DELIMITER not in line):
            stmt += line.replace(DELIMITER, ';')
            continue
    # FINALIZANDO O PARSING DO ARQUIVO

        if stmt:
            stmt += line
            stmts.append(stmt.strip()) 
            stmt = ''
        else:
            stmts.append(line.strip())
    return stmts

# CRIAÇÃO DE UMA FUNÇÃO PARA FAZER A CONEXÃO NO BANCO DE DADOS


def mysqlAcess(myhost,databases,passw,porta):
    connmysql = pymysql.connect( 
        host=myhost, 
        user='root',
        password =passw, 
        db=databases,
        port=porta,
        cursorclass=pymysql.cursors.DictCursor
        )

    # USANDO A FUNCAO DE PARSING NO ARQUIVO 'query.sql'E DEPOIS CRIA UM NOVO CURSOR MySQL, QUE VAI EXECUTAR OS COMANDOS JÁ PARSEADOS PELO parse_sql() E POR FIM FAZER O COMMIT DO QUE FOI FEITO.
    stmts = parse_sql('query.sql')
    with connmysql.cursor() as cursor:
        for stmt in stmts:
            cursor.execute(stmt)
        connmysql.commit()
    connmysql.close()

# ARRAY USADO PARA FAZER AS CONEXOES
x = [


{'host':'localhost', 'banco':'sakila', 'senha':'mysql', 'door':3306},

]

# LOOP QUE VAI ACESSAR O a funcao mysqlAcess para todos dicts da array.

for i in range(0,1):
    try:
        mysqlAcess(x[i]['host'],x[i]['banco'],x[i]['senha'],x[i]['door'])
        print("OK")
    except:
        print("Error")
