import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from pathlib import Path
import pendulum
from time import sleep

# extração de dados das ações da apple, microsoft, google e tesla
TICKERS = [
    "AAPL",
    "MSFT",
    "GOOG",
    "TSLA"
]

# vai extrair dados do dia anterior da data de execução da tarefa no airflow
@task()
def get_history(ticker, ds=None, ds_nodash=None):
        file_path = f"datalake/stocks/{ticker}//{ticker}_{ds_nodash}.csv"
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        yfinance.Ticker(ticker).history(
            period ="1d",
            interval = "1h",
            start = ds_add(ds, -1),
            end = ds,
            prepost = True
                        ).to_csv(file_path)
        sleep(10)

# goal: executar todos os dias do mês enquanto o mercado de ações estiver aberto (seg-sex)
# as tarefas buscam do dia anterior e por isso o dag precisa ser executado ter-sab: cron expression
# cachup=True para o Airflow executar o dag a partir do start_date especificada (quando False ele usa a data de hoje)
@dag(
        schedule_interval = "0 0 * * 2-6",
        start_date = pendulum.datetime(2024, 7, 1, tz="UTC"),
        catchup=True)

def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker, pool='small_pool')(ticker)

dag = get_stocks_dag()