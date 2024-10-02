# Objetivo: fazer a extração de tweets diariamente
# API do tt passou a ser paga, portanto é utilizado uma API alternativa (labdados.com)

# guardar em duas variáveis: start_time e end_time
from datetime import datetime, timedelta
import os  # quando precisa utilizar algun tipo de token
import requests
import json

# montando url
# formato padrão da data do tt
# ano-mês-dia-hora-minuto-segundo-timezone (região do planeta)
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

end_time = datetime.now().strftime(TIMESTAMP_FORMAT)  # dia atual
# dia de início sempre será menos um dia do dia atual: intervalo do dia atual e dia anterior
# timedelta permite operações matemáticas com data
start_time = (datetime.now() + timedelta(days=-1)
              ).date().strftime(TIMESTAMP_FORMAT)
# timedelta pode ser para uma semana atrás por ex (considerando o horário tb e timezone):
# end_time = (datetime.now() + timedelta(hours=3, seconds=-30)).strftime(TIMESTAMP_FORMAT)
# start_time = (datetime.now() + timedelta(days=-7, hours=3)).strftime(TIMESTAMP_FORMAT)

# palavra de interesse para fazer extração dos tweets
query = "data science"

# quais campos do tweet (formato json) devem ser pegos e de qual fonte
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

# montando headers
# bearer_token = os.envir.get("BEARE_TOKEN")
# headers = {"Authorization": "Bearer {}".format(bearer_token)}

# API pronta para fazer requisição
response = requests.request("GET", url_raw)  # headers=headers)

# resposta da url
json_response = response.json()

# extraindo json no formato de str
# print(json.dumps(json_response, indent=4, sort_keys=True))

# goal: todas as páginas disponíveis para extrair todos os tweets
# paginate
while "next_token" in json_response.get("meta", {}):
    next_token = json_response['meta']['next_token']
    url = f"{url_raw}&next_token={next_token}"
    response = requests.request("GET", url)
    json_response = response.json()
    # enquanto existir uma nova página será feito o print
    print(json.dumps(json_response, indent=4, sort_keys=True))
