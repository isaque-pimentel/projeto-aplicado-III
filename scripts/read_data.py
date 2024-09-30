# Projeto Aplicado III - Read Data

import pandas as pd


def load_movies(filepath):
    """Carrega o arquivo movies.dat"""

    # Carregar o arquivo, separando as colunas por "::"
    movies_cols = ["MovieID", "Title", "Genres"]
    movies_df = pd.read_csv(filepath, sep="::", engine="python", names=movies_cols)

    # Extrair o ano do título, se presente entre parênteses no final
    movies_df["Year"] = movies_df["Title"].str.extract(r"\((\d{4})\)", expand=False)

    # Remover o ano do título
    movies_df["Title"] = movies_df["Title"].str.replace(r"\(\d{4}\)", "").str.strip()

    return movies_df


def load_ratings(filepath):
    """Carrega o arquivo ratings.dat"""

    # Carregar o arquivo, separando as colunas por "::"
    ratings_cols = ["UserID", "MovieID", "Rating", "Timestamp"]
    ratings_df = pd.read_csv(filepath, sep="::", engine="python", names=ratings_cols)

    return ratings_df


def load_users(filepath):
    """Carregar o arquivo users.dat"""

    # Carregar o arquivo, separando as colunas por "::"
    users_cols = ["UserID", "Gender", "Age", "Occupation", "Zip-code"]
    users_df = pd.read_csv(filepath, sep="::", engine="python", names=users_cols)

    return users_df


def merge_data(movies_df, ratings_df, users_df):
    """Agregar as bases de dados"""

    # Mesclar as avaliações (ratings) com as informações dos filmes
    movies_ratings_df = pd.merge(ratings_df, movies_df, on="MovieID")

    # Mesclar o resultado com as informações dos usuários
    full_df = pd.merge(movies_ratings_df, users_df, on="UserID")

    return full_df


# Paths dos arquivos
movies_filepath = "./dataset/ml-1m/movies.dat"
ratings_filepath = "./dataset/ml-1m/ratings.dat"
users_filepath = "./dataset/ml-1m/users.dat"

# Carregar os arquivos
movies_df = load_movies(movies_filepath)
ratings_df = load_ratings(ratings_filepath)
users_df = load_users(users_filepath)

# Agregar as bases de dados
full_df = merge_data(movies_df, ratings_df, users_df)

# Exibir a primeira linha da base agregada para verificação
print(full_df.head())

# Salvar o DataFrame no formato Parquet para leitura mais eficiente
full_df.to_parquet("./dataset/movielens_full_data.parquet", index=False)
