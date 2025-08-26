# Importa bibliotecas
import pandas as pd

def carregamento(caminho_origem, caminho_destino):
    df_limpa = pd.read_parquet(caminho_origem)
    df_limpa.to_csv(caminho_destino)
