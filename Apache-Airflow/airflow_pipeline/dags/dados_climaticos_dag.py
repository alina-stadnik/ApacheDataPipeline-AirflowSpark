from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum # define data específica
from os.path import join
import pandas as pd


with DAG(
        "dados_climaticos",
        start_date=pendulum.datetime(2024, 4, 1, tz="UTC"),
        schedule_interval='0 0 * * 1', # executar toda segunda feira a meia noite
        # CRON expression: 5 argumentos (minuto 0-59, hora 0-23, dia do mês 1-31, mês 1-12, dia da semana 0-6)
        # dois primeiros representam minuto e a hora (00h em ponto, no caso '0 0')
        # dois proximos argumentos representam o dia do mes e o mes (como a dag rodará todo mes será usado ('* *'))
        # ultimo argumento trata-se dos dias da semana em que o DAG será executado, nesse caso sendo apenas na segunda ('1')
) as dag:
    
    # criação das tasks
    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/alina/Documents/estudos/Airflow/datalake/dados_climaticos/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
        )
    
    def extrai_dados(data_interval_end):
        city = 'Curitiba'  # cidade de interesse para extrair os dados
        key = 'XXX'

        # url do site da API do tempo
        # ds_add(data_interval_end, 7): pega a data que deseja somar (data_interval_end) e a quantidade de dias (7)
        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                   f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)
        # path diretório para salvar os dados da API
        file_path = f'/home/pacer/Documents/Cursos-Alura/Apache-Airflow/datalake/dados_climaticos/semana={data_interval_end}/'
        # seleciona os dados de interesse
        dados.to_csv(file_path + 'dados_brutos.csv') # nome do diretório + nome do arquivo
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv') 
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')      

    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados, 
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2

