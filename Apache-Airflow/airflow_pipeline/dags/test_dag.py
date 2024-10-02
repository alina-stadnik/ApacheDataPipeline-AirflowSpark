from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
  'TestDAG',
  start_date=days_ago(2),
  schedule_interval='@daily',
) as dag:

   tarefa_1 = EmptyOperator(task_id='tarefa_1')
   tarefa_2 = EmptyOperator(task_id='tarefa_2')
   tarefa_3 = EmptyOperator(task_id='tarefa_3')

   tarefa_4 = BashOperator(
        task_id = 'print_hello_world',
        bash_command = 'echo "Hello, World!"'
      )

   tarefa_1 >> [tarefa_2, tarefa_3] 
   tarefa_3 >> tarefa_4