# Transformação: extrair os dados do tweet, extrair os dados do usuário e salvar

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame
from typing import List
import os
from os.path import join
import argparse

def get_tweets_data(df: DataFrame) -> DataFrame:
    """
    Extrai dados de tweets de um nested DataFrame e retorna um DataFrame simplificado.

    Esta função recebe um DataFrame `df` que contém dados de nested tweets. 
    Ela explode a coluna "data", seleciona informações relevantes de cada tweet 
    e retorna um DataFrame com colunas selecionadas e suas métricas públicas.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada contendo dados dos nested tweets.

    Retorna:
    DataFrame: Um novo DataFrame contendo as seguintes colunas:
        - author_id: ID do autor do tweet
        - conversation_id: ID da conversa do tweet
        - created_at: Data e hora de criação do tweet
        - id: ID do tweet
        - public_metrics.*: Métricas públicas do tweet (ex.: retweets, respostas, curtidas, impressões)
        - text: Texto do tweet
    """

    tweet_df = df.select(f.explode("data").alias("tweets"))\
                .select("tweets.author_id", "tweets.conversation_id",
                        "tweets.created_at", "tweets.id",
                        "tweets.public_metrics.*", "tweets.text")
    return tweet_df

def get_users_data(df: DataFrame) -> DataFrame:
    """
    Extrai dados de usuários de um nested DataFrame e retorna um DataFrame simplificado.

    Esta função recebe um DataFrame `df` que contém dados de nested usuários. 
    Ela explode a coluna "includes.users" e retorna um DataFrame com todas as colunas 
    dos dados de usuários.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada contendo dados de nested usuários.

    Retorna:
    DataFrame: Um novo DataFrame contendo todas as colunas dos dados de usuários.
    """

    user_df = df.select(f.explode("includes.users").alias("users")).select("users.*")
    return user_df

def export_json(df: DataFrame, destino: str) -> None:
    """
    Exporta um DataFrame para arquivos JSON em um destino específico.

    Esta função grava o DataFrame `df` em formato JSON no caminho especificado por `destino`. 
    O DataFrame é coalescido para um único arquivo de saída para garantir que apenas um 
    arquivo JSON final seja gerado.

    Parâmetros:
    df (DataFrame): O DataFrame a ser exportado para JSON.
    dest (str): O caminho completo para o diretório de destino onde os arquivos JSON serão salvos.
    """

    df.coalesce(1).write.mode("overwrite").json(destino)

def twitter_transformation(spark: SparkSession, src: str, destino: str, process_date: str) -> None:
    """
    Realiza a transformação dos dados de tweets e usuários lidos de arquivos JSON para formatos estruturados.

    Esta função utiliza o Spark para ler dados de arquivos JSON contendo informações de tweets e usuários.
    Os dados são transformados e exportados para arquivos JSON estruturados, organizados por tabela e data de processamento.

    Parâmetros:
    spark (SparkSession): Sessão do Spark inicializada e configurada (será responsabilidade do airflow criar a sessão Spark).
    src (str): Caminho completo para os arquivos JSON contendo dados de tweets e usuários (dados de origem).
    destino (str): Caminho base onde os arquivos JSON estruturados serão salvos, com placeholders para tabela e data.
    process_date (str): Data de processamento no formato YYYY-MM-DD para ser incluída no caminho dos arquivos exportados.
    """
    
    # src é o caminho para o parent_folder
    subfolders = [os.path.join(src, folder) for folder in os.listdir(src) if os.path.isdir(os.path.join(src, folder))]
    # Lista todos os arquivos JSON dentro dos subdiretórios
    json_files = []
    for folder in subfolders:
        json_files.extend([os.path.join(folder, file) for file in os.listdir(folder) if file.endswith(".json")])


    df = spark.read.json(json_files)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_destino = join(destino, "{table_name}", f"process_date={process_date}")

    export_json(tweet_df, table_destino.format(table_name="tweet"))
    export_json(user_df, table_destino.format(table_name="user"))



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--destino", required=True)
    parser.add_argument("--process_date", required=True)

    args = parser.parse_args() 

    spark = SparkSession\
    .builder\
    .appName("twitter_transformation")\
    .getOrCreate()

    twitter_transformation(spark, args.src, args.destino, args.process_date)