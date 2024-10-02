import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

# define o intervalo de datas
data_inicio = datetime.today()  # no caso é o dia de hoje
data_fim = data_inicio + timedelta(days=7)  # datas de uma semana

# formatando as datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

city = 'Curitiba'  # cidade de interesse para extrair os dados
key = 'XXX'

# url do site da API do tempo
URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
           f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

# checando os dados
dados = pd.read_csv(URL)
print(dados.head())

# cria o diretório onde vai salvar os dados do site
file_path = f'/home/pacer/Documents/Cursos-Alura/Apache-Airflow/src/API_clima/semana={data_inicio}/'
os.mkdir(file_path)


# seleciona os dados de interesse
dados.to_csv(file_path + 'dados_brutos.csv') # nome do diretório + nome do arquivo
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv') 
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
