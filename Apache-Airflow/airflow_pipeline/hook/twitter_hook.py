from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime, timedelta
import json

class TwitterHook(HttpHook):

    def __init__(self, end_time, start_time, query, conn_id=None,):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "twitter_default"  # nome da conexão do web server
        # super para acessar classe herdada e passar no construtor da classe a conexão
        # passa a conexão para o httphook para se comunicar dentro do Airflow
        super().__init__(http_conn_id=self.conn_id)

    # como já existe a conexão, agora é possível mexer com o script de extração
    def create_url(self):
        # BLOCO DE MONTAR A URL (adaptada do extração_api_twitter.py)
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        end_time = self.end_time  # usuário que define e por isso vem no construtor
        start_time = self.start_time  # usuário que define e por isso vem no construtor
        query = self.query  # usuário que define e por isso vem no construtor
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        # a base da conexão (https://labdados.com) fica definida no host do airflow
        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        # BLOCO DA REQUISIÇÃO (adaptada do extração_api_twitter.py)
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        # toda vez que for utilizado será possível ver um log para ver qual url está sendo utilizada
        self.log.info(f"URL: {url}")

        # run_and_check existe no httphook
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):
        # BLOCO PAGINATE/EXTRAÇÃO DAS PÁGINAS DISPONÍVEIS (adaptada do extração_api_twitter.py)
        # acúmula todas as respostas do json em uma única variável
        response = self.connect_to_endpoint(url_raw, session)
        list_json_response = []
        json_response = response.json()
        list_json_response.append(json_response)
        count = 1  # count é para caso as requisições sejam limitadas

        # paginate
        while "next_token" in json_response.get("meta", {}) and count < 100:
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()  # sobrescreve a variável para cada nova página
            list_json_response.append(json_response)
            count += 1

        return list_json_response

    def run(self):
        # conexão com a rede é responsabilidade do airflor
        session = self.get_conn()  # método da classe que não foi criado e é padrão do httphook
        url_raw = self.create_url()
        return self.paginate(url_raw, session)


if __name__ == "__main__":
    # parâmetros obrigatórios para instanciar a classe TwitterHook
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)  # dia atual
    start_time = (datetime.now() + timedelta(days=-1)
                  ).date().strftime(TIMESTAMP_FORMAT)
    query = 'data science'

    # Importante: TwitterHook retorna uma lista de json
    for page in TwitterHook(end_time, start_time, query).run():
        # print em cada 'tweet'
        print(json.dumps(page, indent=4, sort_keys=True))
