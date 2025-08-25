# Importa biblioteca pandas
import pandas as pd
import openpyxl
import logging

# Função para realizar a importação dos arquivos
def converte_parquet(caminho_arquivo, caminho_destino):
    
    # Importa arquivos .xlsx
    arquivo_excel = pd.read_excel(caminho_arquivo)
    logging.info(f"Realizando a leitura do {caminho_arquivo}")
    # Retorna a base de dados em .parquet
    arquivo_excel.to_parquet(caminho_destino, engine="pyarrow", index=False)
    logging.info(f"Convertendo para parquet")

