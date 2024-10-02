import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from os.path import join
from airflow.utils.dates import days_ago # para usar dias passados 
from pathlib import Path

with DAG(dag_id="TwitterDAG", start_date=days_ago(5), schedule_interval="@daily") as dag: # 2 dias atrás e frequência diária
    BASE_FOLDER = join(
           str(Path("~/Documents").expanduser()),
           "Cursos-Alura/Apache-Airflow/datalake/{stage}/twitter_datascience/{partition}",
       )
    PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = 'data science'

    # {{}} jinja template disponível no airflow, no caso ds é o mesmo que datetime.now()
    # ds_nodash é a data sem barra
    # bronze/silver/gold segue a arquitetura medallion
    twitter_operator = TwitterOperator(file_path=join(BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
        "datascience_{{ ds_nodash }}.json"),
        query=query, 
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
        task_id="twitter_datascience")
    
    twitter_transform = SparkSubmitOperator(task_id="tranform_twitter_datascience",
                                            application="/home/pacer/Documents/estudos/Airflow/src/spark/transformation.py",
                                            name="twitter_transformation",
                                            application_args=["--src", BASE_FOLDER.format(stage="Bronze", partition=""),
                                                              "--destino", BASE_FOLDER.format(stage="Silver", partition=""),
                                                              "--process_date", "{{ ds }}"
                                                              ])
    
    twitter_insight = SparkSubmitOperator(task_id="insight_twitter",
                                            application="/home/alina/Documents/estudos/Airflow/src/spark/insight_tweet.py",
                                            name="insight_twitter",
                                            application_args=["--src", BASE_FOLDER.format(stage="Silver", partition=""),
                                             "--destino", BASE_FOLDER.format(stage="Gold", partition=""),
                                             "--process_date", "{{ ds }}"])
    
twitter_operator >> twitter_transform >> twitter_insight