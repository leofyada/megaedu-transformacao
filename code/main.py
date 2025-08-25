from importacao import converte_parquet
from transformacao import limpeza_basica

# Caminho para o arquivo com o baseline
caminho_baseline = "/Users/leonardoyada/Desktop/megaedu-transformacao/data/bronze/20250825_baseline_escolas.xlsx"
# Caminho para a fonte única
caminho_fonteunica = "/Users/leonardoyada/Desktop/megaedu-transformacao/data/bronze/20250825_fonte_unica.xlsx"

# Caminho de destino da base com o baseline
caminho_destino_baseline = "/Users/leonardoyada/Desktop/megaedu-transformacao/data/prata/20250825_baseline_escolas.parquet"
# Caminho de destino da fonte única
caminho_destino_fonteunica = "/Users/leonardoyada/Desktop/megaedu-transformacao/data/prata/20250825_fonte_unica.parquet"

# Caminho de destino da base limpa
caminho_destino_limpa = "/Users/leonardoyada/Desktop/megaedu-transformacao/data/prata/base_limpa.parquet"

def run_etl():
    
    # Converte arquivos Excel para .parquet
    converte_parquet(caminho_baseline, caminho_destino_baseline)
    converte_parquet(caminho_fonteunica, caminho_destino_fonteunica)
    
    # Limpa os arquivos e retorna uma base tratada
    limpeza_basica(caminho_destino_baseline, caminho_destino_fonteunica, caminho_destino_limpa)

if __name__ == "__main__":
    run_etl()
    

