from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from os.path import join
import argparse

def get_tweet_conversas(df_tweet: DataFrame) -> DataFrame:
    """
    Aggregates tweet data by date, calculating distinct authors, and summing like, quote, reply, and retweet counts.

    Args:
        df_tweet (pyspark.sql.DataFrame): DataFrame containing tweet data with the following columns:
            - created_at (timestamp): Timestamp of when the tweet was created.
            - author_id (string): ID of the tweet author.
            - like_count (int): Number of likes the tweet received.
            - quote_count (int): Number of quotes the tweet received.
            - reply_count (int): Number of replies the tweet received.
            - retweet_count (int): Number of retweets the tweet received.

    Returns:
        pyspark.sql.DataFrame: DataFrame with the following columns:
            - created_date (date): Date of tweet creation.
            - n_tweets (int): Number of distinct tweet authors.
            - n_like (int): Total number of likes.
            - n_quote (int): Total number of quotes.
            - n_reply (int): Total number of replies.
            - n_retweet (int): Total number of retweets.
            - weekday (string): Day of the week corresponding to the created_date.
    """

    return df_tweet.alias("tweet")\
        .groupBy(f.to_date("created_at").alias("created_date"))\
        .agg(
            f.countDistinct("author_id").alias("n_tweets"),
            f.sum("like_count").alias("n_like"),
            f.sum("quote_count").alias("n_quote"),
            f.sum("reply_count").alias("n_reply"),
            f.sum("retweet_count").alias("n_retweet")
        ).withColumn("weekday", f.date_format("created_date", "E"))

def export_json(df: DataFrame, destino: str) -> None:
    """
    Exporta um DataFrame para arquivos JSON em um destino específico.

    Esta função grava o DataFrame `df` em formato JSON no caminho especificado por `dest`. 
    O DataFrame é coalescido para um único arquivo de saída para garantir que apenas um 
    arquivo JSON final seja gerado.

    Parâmetros:
    df (DataFrame): O DataFrame a ser exportado para JSON.
    dest (str): O caminho completo para o diretório de destino onde os arquivos JSON serão salvos.
    """
        
    df.coalesce(1).write.mode("overwrite").json(destino)

def twitter_insight(spark:SparkSession, src: str, destino: str, process_date: str) -> None:
    """
    Executa a transformação e exportação de dados de tweets para insights diários.

    Esta função lê dados de tweets a partir de arquivos JSON, calcula estatísticas agregadas diárias, 
    e exporta os resultados para arquivos JSON estruturados.

    Parâmetros:
    spark (SparkSession): Sessão do Spark inicializada e configurada (será responsabilidade do airflow criar a sessão Spark).
    src (str): Caminho completo para os arquivos JSON contendo dados de tweets (dados de origem).
    dest (str): Caminho base onde os arquivos JSON estruturados serão salvos, com placeholders para data de processamento.
    process_date (str): Data de processamento no formato YYYY-MM-DD para ser incluída no caminho dos arquivos exportados.
    """

    df_tweet = spark.read.json(join(src, 'tweet'))
    tweet_conversas = get_tweet_conversas(df_tweet)
    export_json(tweet_conversas, join(destino, f"process_date={process_date}"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation Silver"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--destino", required=True)
    parser.add_argument("--process_date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_insight(spark, args.src, args.destino, args.process_date)

