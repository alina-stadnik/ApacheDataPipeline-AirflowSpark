import sys
import os
sys.path.append("airflow_pipeline")
from datetime import datetime, timedelta
import json
from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):
    # informar uma lista de campos que vão usar jinja template
    template_fields = ["query", "file_path", "start_time", "end_time"]

    # BaseOperator possui métodos que são importantes para o comportamento de rastreamento do processo
    # operadores derivados desta classe devem executar ou acionar determinadas tarefas de forma síncrona (aguardar a conclusão)
    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.file_path = file_path
        self.end_time = end_time
        self.start_time = start_time
        self.query = query

        super().__init__(**kwargs)  # parâmetros necessários para a classe mãe

    def create_parent_folder(self):
        # cria tds as pastas do datalake se n existirem ou n faz nada se existirem (n retorna um erro)
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) 

    def execute(self, context):
        # Esse método execute aplica a característica da atomicidade

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for page in TwitterHook(self.end_time, self.start_time, self.query).run():
                # dump salva as informações no arquivo
                # ensure_ascii=False garante que não terá quebra/caracteres estranhos no arquivo
                json.dump(page, output_file, ensure_ascii=False)
                # pula uma linha para ficar melhor estruturado
                output_file.write("\n")


if __name__ == "__main__":
    # parâmetros obrigatórios para instanciar a classe TwitterOperator
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)  # dia atual
    start_time = (datetime.now() + timedelta(days=-1)
                  ).date().strftime(TIMESTAMP_FORMAT)
    query = 'data science'

    # Importante: um operador é chamado pela task e quem chama uma task é a DAG
    # precisa criar uma DAG e uma task instance que vai rodar o operator
    # start_date quando a DAG vai ser inicializada
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        # file_path parâmetro diz respeito ao local de armazenamento da extração
        # join() ajuda a estruturas as pastas
        # datalake para armazenar os dados e o nome do projeto e da query
        # o segundo parâmetro está relacionado com a idempotência para salvar os arquivos nessa mesma pasta 
        # terceiro parâmetro é o nome da extração
        to = TwitterOperator(
            file_path=join("datalake/twitter_datascience", 
                           f"extract_date={datetime.now().date()}",
                           f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
            query=query, start_time=start_time, end_time=end_time, task_id='test_run')
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)  # context = ti.task_id
