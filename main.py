from mysql.connector import connect, Error
import psutil
import time
import os
import platform
import pingparsing
from dotenv import load_dotenv
import socket #serve para pegar o nome da máquina

load_dotenv()

#pegando nome do dispositivo
nome_dispositivo = socket.gethostname()
ip_maquina = socket.gethostbyname(nome_dispositivo)

#vejo sistema operacional
SO = platform.system()

#configurando o banco de dados
config = {
  "user": os.getenv("USER_LOGIN"),
  "password": os.getenv("DB_PASSWORD"),
  "host": os.getenv("HOST"),
  "database": os.getenv("DATABASE")
}

#pegando a fkDispositivo e a fkNR por meio do nome e armazenando em uma variável
try:
        # Conectar ao banco de dados
        mydb = connect(**config)
        if mydb.is_connected():
            mycursor = mydb.cursor()

            result = mycursor.execute(f"SELECT idDispositivo, fkLinha FROM dispositivo WHERE nome LIKE '%{nome_dispositivo}%';")
            select = mycursor.fetchall()
            fkDispositivo = select[0][0]
            fkLinha = select[0][1]

            print(f"fkDispositivo: {fkDispositivo}")
            print(f"fkLinha: {fkLinha}")

except Error as e:
        print("Erro ao conectar com o MySQL (parte da fkDispositivo):", e)
        
finally:
        # Fechar cursor e conexão
        if mydb.is_connected():
            mycursor.close()
            mydb.close()


#começo da captura de dados

#serve para o disco sendo como um indicador para quando capturar 
contador_disco = 0

while True:
    PercCPU = psutil.cpu_percent()
    PercMEM = psutil.virtual_memory().percent
    FreqCPU = psutil.cpu_freq()

    # Captura do tempo de resposta com pingparsing
    transmitter = pingparsing.PingTransmitter()
    transmitter.destination = "ip_catraca"
    transmitter.count = 1
    result = transmitter.ping()
    ping_parser = pingparsing.PingParsing()
    TempoResposta = ping_parser.parse(result).rtt_avg  # Armazenando tempo de resposta


    #listas para trabalhar com mais de um valor
    lista_valor = [PercCPU, PercMEM, FreqCPU, TempoResposta]
    lista_variavel = ["PercCPU", "PercMEM", "FreqCPU", "TempoResposta"]
    lista_nomeVariavel = ["porcentagem de CPU", "porcentagem de memória RAM", "frequência de CPU", "tempo de resposta"]
    lista_idComponente = []
    lista_limite = []
    lista_tipoAlerta = []
    lista_foraLimite = []

    try:
        # Conectar ao banco de dados
        mydb = connect(**config)
        if mydb.is_connected():
            mycursor = mydb.cursor()

        #select pra pegar idComponente
        for captura in lista_variavel:
            result = mycursor.execute(f"SELECT idComponente FROM componente WHERE nome LIKE '%{captura}%';")
            idComponente = mycursor.fetchall()
            idComponente = idComponente[0][0]
            lista_idComponente.append(idComponente)

        for idComponente in lista_idComponente:
            result = mycursor.execute(f"SELECT valor, tipo FROM limite WHERE fkComponente = {idComponente} AND fkDispositivo = {fkDispositivo};")
            result = mycursor.fetchall()
            for sql in result:
                valor_limite = sql[0]
                limite_tipo = sql[1]
                lista_limite.append(valor_limite)
                lista_tipoAlerta.append(limite_tipo)

        #colocando todas as variáveis fora dos limites na lista "lista_foraLimite"
        for index in range(len(lista_valor)):
            if lista_tipoAlerta[index] == "acima" and lista_valor[index] > lista_limite[index]:  # Caso de valores acima do limite
                lista_foraLimite.append((lista_nomeVariavel[index], lista_valor[index], lista_limite[index], "acima"))
            elif lista_tipoAlerta[index] == "abaixo" and lista_valor[index] < lista_limite[i]:  # Caso de valores abaixo do limite
                lista_foraLimite.append((lista_nomeVariavel[index], lista_valor[index], lista_limite[index], "abaixo"))
                

        if len(lista_foraLimite) > 0:

            #serve para identificar na lista_idComponente, qual a fkComponente
            i = 0

            for item in lista_valor:
                sql_query = """
                INSERT INTO captura (fkDispositivo, fkLinha, fkComponente, registro, dataRegistro)
                VALUES (%s, %s,%s, %s, current_timestamp())
                """
                val = (fkDispositivo, fkLinha, lista_idComponente[i], item)
                mycursor.execute(sql_query, val)
                mydb.commit()
                print(f"Dado inserido em 'captura' com fkComponente = {lista_idComponente[i]} e valor = {item}")

                #pego a fkCaptura pegando o último dado que foi inserido
                result = mycursor.execute(f"SELECT idCaptura FROM captura ORDER BY idCaptura DESC LIMIT 1;")
                idUltimoDado = mycursor.fetchall()
                idUltimoDado = idUltimoDado[0][0]

                #vejo com o índice qual é a variável da lista e se o valor dela ultrapassa o limite para fazer a descrição do alerta
                if lista_variavel[i] == lista_foraLimite[i][0]:
                        descricao = f"{lista_foraLimite[i][0]} está {lista_foraLimite[i][3]} do limite de {lista_foraLimite[i][2]}: valor atual é {lista_foraLimite[i][1]}"
                        
                        sql_query = "INSERT INTO alerta(fkDispositivo, fkCaptura, fkLinha, dataAlerta, descricao) VALUES (%s, %s, %s, current_timestamp(), %s);"
                        val = [fkDispositivo, idUltimoDado, fkLinha, descricao]
                        mycursor.execute(sql_query, val)
                        mydb.commit()
                        print(f"Dado inserido em 'alerta' com fkCaptura = {idUltimoDado} e descrição = '{descricao}'")

                i += 1

        else:

            # Inserir dados na tabela
            i = 0

            for item in lista_valor:
                sql_query = """
                    INSERT INTO captura (fkDispositivo, fkLinha, fkComponente, registro, dataRegistro)
                    VALUES (%s, %s,%s, %s, current_timestamp())
                    """
                val = (fkDispositivo, fkLinha, lista_idComponente[i],item)
                mycursor.execute(sql_query, val)
                mydb.commit()
                print(f"Dado inserido em 'captura' com fkComponente = {lista_idComponente[i]} e valor = {item}")
                i += 1
                
                print(mycursor.rowcount, "registro inserido")


        #cada dado de cpu e ram será cadastrado a cada 30 segundos e dessa forma a cada 120 dados pegos, irá inserir um dado de disco (30 segundos = 120 dados em uma hora)
        if (contador_disco == 0 or contador_disco % 120 == 0):
            print("entrando em disco")

            result = mycursor.execute(f"SELECT idComponente FROM componente WHERE nome LIKE '%PercDISCO%';")
            idComponente = mycursor.fetchall()
            idComponente = idComponente[0][0]

            result = mycursor.execute(f"SELECT valor, tipo FROM limite WHERE fkComponente = {idComponente} AND fkDispositivo = {fkDispositivo};")
            result = mycursor.fetchall()
            valor_limite = result[0][0]
            tipo_limite = result[0][1]

            #vejo sistema operacional e assim coloco a pasta
            if(SO == "Windows"):
                PercDISCO = psutil.disk_usage('C:\\').percent
            else:
                PercDISCO = psutil.disk_usage('/').percent

            sql_query = """
                INSERT INTO captura (fkDispositivo, fkLinha, fkComponente, registro, dataRegistro)
                VALUES (%s, %s, %s, %s, current_timestamp())
                """
            val = (fkDispositivo, fkLinha, idComponente, PercDISCO)
            mycursor.execute(sql_query, val)
            mydb.commit()
            print(f"Dado inserido em 'captura' com fkComponente = {idComponente} e valor = {PercDISCO}")

            result = mycursor.execute(f"SELECT idCaptura FROM captura ORDER BY idCaptura DESC LIMIT 1;")
            idUltimoDadoDISK = mycursor.fetchall()
            idUltimoDadoDISK = idUltimoDadoDISK[0][0]
            

            if(PercDISCO > valor_limite):
                descricao = f"Porcentual de uso de disco está acima do limite de {valor_limite}: valor atual é {PercDISCO}"
                
                sql_query = "INSERT INTO alerta(fkDispositivo, fkCaptura, fkLinha, dataAlerta, descricao) VALUES (%s, %s, %s, current_timestamp(), %s);"
                val = [fkDispositivo, idUltimoDadoDISK, fkLinha, descricao]
                mycursor.execute(sql_query, val)
                mydb.commit()
                print(f"Dado inserido em 'alerta' com fkCaptura = {idUltimoDadoDISK} e descrição = '{descricao}'")


    except Error as e:
        print("Erro ao conectar com o MySQL:", e)
        
    finally:
        # Fechar cursor e conexão
        if mydb.is_connected():
            mycursor.close()
            mydb.close()


    contador_disco += 1
    time.sleep(30)