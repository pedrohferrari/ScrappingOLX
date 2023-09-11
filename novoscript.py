# # Imports

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json
import mysql.connector
import os

# Variavel de controle para o código processar enquanto houver páginas com anúncios
devoProcessar = True
# Varíavel para controlar o número de páginas com anúncio
pagina = 1

# Criando os atributos principais de cada anúncio.
id = []
titulo = []
preco = []
local = []
url = []
# Criando os atributos opcionais de cada anúncio.
atributos = {
    'Modelo': [],
    'Marca': [],
    'Tipo de veículo': [],
    'Ano': [],
    'Quilometragem': [],
    'Potência do motor': [],
    'Câmbio': [],
    'Direção': [],
    'Cor': [],
    'Único dono': [],
    'Opcionais': [],
    'Kit GNV': [],
    'Revisões feitas em concessionária': [],
    'Com garantia': [],
    'De leilão': [],
    'IPVA pago': [],
    'Com multas': [],
    'Quitado': []
}   

while devoProcessar:

    link = 'https://www.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios/peugeot/208/estado-es?q=208&re=69&rs=66&o=' + str(pagina)
    filename = 'acompanhamento208'


    # Request da url.
    header = {'user-agent': 'Mozila/5.0'}
    request = requests.get(link, headers = header)


    # Aplicando o BS para retornar os dados em lista.
    response = request.text
    responsebs = BeautifulSoup(response, 'html.parser')

    # Retornando o compartimento com os anúncios.
    request = responsebs.find('script', id='__NEXT_DATA__')

    # Retornando os dados em JSON.
    json_data = request.contents[0]

    # Transformando o JSON em dicionário.
    data_dict = json.loads(json_data)



    # Retornando a lista com os atributos principais de cada anúncio.
    anuncios = data_dict['props']['pageProps']['ads']

    # Verificação se realmente há conteúdo no anúncio (Exclusão de propagandas que aparecem entre os anúncios.)
    temAnuncio = True if len(anuncios) != 0 else False

    # Loop para verificar cada anúncio e adicionar os atributos em suas respectivas listas.
    if temAnuncio:
        for ad in anuncios:
            if 'subject' in ad:
                id.append(ad['listId'])
                titulo.append(ad['title'])
                preco.append(ad['price'])
                url.append(ad['url'])
                local.append(ad['location'])
        
        for ad in anuncios:
            if 'properties' in ad:
                propriedades = ad['properties']
                for atributo, lista in atributos.items():
                    valor = 'Nulo'  # Valor padrão para informações ausentes
                    for i in propriedades:
                        if atributo in i['label']:
                            valor = i['value']
                            break
                    lista.append(valor)
        pagina += 1
    else:
        devoProcessar = False



finaldf = pd.DataFrame({
    'ID': id,
    'Título': titulo,
    'Preço': preco,
    'Local': local,
    'URL': url,
    **atributos  # Inclui os atributos opcionais do dicionário
})

# Adicione a coluna 'Data' usando o método assign
finaldf = finaldf.assign(Data=datetime.today().strftime('%d-%m-%Y'))

# Salve o DataFrame em um arquivo Excel
#finaldf.to_excel(filename + '.xlsx', index=None)


#Criando conexão com o banco de dados
#Parâmetros de conexão
config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_DATABASE')
}

#Tratamento para verificar a conexão com o banco
try:
    conn = mysql.connector.connect(**config)

    if conn.is_connected():
        print("Conexão com o banco de dados está ativa.")

    # Criando cursor para interação com o banco de dados.
    cursor = conn.cursor()


    # Parametrizando nome da tabela no banco de dados
    tabela = 'ACOMPANHAMENTO_ANUNCIOS'

    for index, row in finaldf.iterrows():
            # Defina a ordem das colunas para corresponder à tabela MySQL
            colunas = finaldf.columns
            # Cria uma lista de valores correspondentes às colunas
            valores = [row[coluna] for coluna in colunas]
            # Gera a lista de marcadores de posição (%s) com base no número de colunas
            marcadores = ', '.join(['%s'] * len(row.index))
            # Crie a consulta SQL dinâmica
            query = f"INSERT INTO {tabela} (ID, TITULO, PRECO, ENDERECO, URL, MODELO, MARCA, TIPO_VEICULO, ANO, KM, POTENCIA_MOTOR, CAMBIO, DIRECAO, COR, UNICO_DONO, OPCIONAIS, GNV, REVISOES, GARANTIA, LEILAO, IPVA_PAGO, MULTAS, QUITADO, DATA_EXTRACAO) VALUES ({marcadores})"
            # Execute a consulta com os valores da linha atual do DataFrame
            cursor.execute(query, tuple(valores))

    # Confirma a transação e encerra a conexão
    conn.commit()
    print(finaldf.shape[0], "registros inseridos.")
    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    print(f"Erro ao conectar ao banco de dados: {err}")