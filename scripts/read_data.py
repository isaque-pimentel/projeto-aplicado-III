"""Projeto Aplicado III - Read Data

O script carrega, processa e agrega os dados dos arquivos movies.dat,
ratings.dat e users.dat da base MovieLens 1M usando pandas.
Cada arquivo é lido em funções separadas, que formatam as colunas e tratam
os dados.
Os filmes têm seus títulos e anos extraídos e organizados, as avaliações dos
usuários são estruturadas,
e as informações de perfil dos usuários são carregadas.
Em seguida, os dados são combinados em um único DataFrame, unindo as
avaliações, filmes e usuários. 
O resultado é salvo em um arquivo parquet para futuras análises.

"""

import pandas as pd


def load_movies(filepath: str) -> pd.DataFrame:
    """
    Carrega o arquivo de filmes e processa os dados.
    """

    # Define os nomes das colunas
    movies_cols = ["MovieID", "Title", "Genres"]

    # Carrega o arquivo movies.dat, separando as colunas por "::"
    movies_df = pd.read_csv(
        filepath, sep="::", engine="python", names=movies_cols, encoding="latin1"
    )

    # Extrai o ano do título, se presente entre parênteses no final
    movies_df["Year"] = movies_df["Title"].str.extract(r"\((\d{4})\)", expand=False)

    # Remove o ano do título
    movies_df["Title"] = movies_df["Title"].str.replace(r"\(\d{4}\)", "").str.strip()

    return movies_df


def load_ratings(filepath: str) -> pd.DataFrame:
    """
    Carrega o arquivo de avaliações e processa os dados.
    """

    # Define os nomes das colunas
    ratings_cols = ["UserID", "MovieID", "Rating", "Timestamp"]

    # Carrega o arquivo, separando as colunas por "::"
    ratings_df = pd.read_csv(filepath, sep="::", engine="python", names=ratings_cols)

    return ratings_df


def load_users(filepath: str) -> pd.DataFrame:
    """
    Carrega o arquivo de usuários e processa os dados.
    """

    # Define os nomes das colunas
    users_cols = ["UserID", "Gender", "Age", "Occupation", "Zip-code"]

    # Carrega o arquivo, separando as colunas por "::"
    users_df = pd.read_csv(filepath, sep="::", engine="python", names=users_cols)

    return users_df


def merge_data(
    movies_df: pd.DataFrame, ratings_df: pd.DataFrame, users_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Agrega as bases de dados de filmes, avaliações e usuários.
    """

    # Agrega as avaliações (ratings) com as informações dos filmes
    movies_ratings_df = pd.merge(ratings_df, movies_df, on="MovieID")

    # Agrega o resultado com as informações dos usuários
    full_df = pd.merge(movies_ratings_df, users_df, on="UserID")

    return full_df


def main() -> None:
    """Função main que executa o carregamento e agregação dos dados"""

    # Paths dos arquivos da base de dados MovieLens 1M
    movies_filepath = "./dataset/ml-1m/movies.dat"
    ratings_filepath = "./dataset/ml-1m/ratings.dat"
    users_filepath = "./dataset/ml-1m/users.dat"

    # Carrega os arquivos
    print(f"Carregando o arquivo {movies_filepath}...")
    movies_df = load_movies(movies_filepath)
    print(f"Arquivo {movies_filepath} carregado!")

    print(f"Carregando o arquivo {ratings_filepath}...")
    ratings_df = load_ratings(ratings_filepath)
    print(f"Arquivo {ratings_filepath} carregado!")

    print(f"Carregando o arquivo {users_filepath}...")
    users_df = load_users(users_filepath)
    print(f"Arquivo {users_filepath} carregado!")

    # Agrega as bases de dados em um único dataframe
    full_df = merge_data(movies_df, ratings_df, users_df)
    print("Base de dados foi agregada!")

    # Exibi uma parte da base agregada
    print(full_df.head())

    # Salva o DataFrame no formato parquet para leitura mais eficiente
    full_df.to_parquet("./dataset/movielens_full_data.parquet", index=False)

    # Carrega o DataFrame salvo no formato parquet
    # full_df = pd.read_parquet(".dataset/movielens_full_data.parquet")


if __name__ == "__main__":
    main()
