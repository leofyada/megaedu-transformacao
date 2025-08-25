from importacao import converte_parquet
from transformacao import limpeza_basica
import logging

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

# Configuração do log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("megaedu.transformacao")

def run_etl():
    
    try:

        # Converte arquivos Excel para .parquet
        converte_parquet(caminho_baseline, caminho_destino_baseline)
        logger.info("Conversão do arquivo baseline para parquet realizado com sucesso")
        converte_parquet(caminho_fonteunica, caminho_destino_fonteunica)
        logger.info("Conversão do arquivo fonte única para parquet realizada com sucesso")    

        # Limpa os arquivos e retorna uma base tratada
        limpeza_basica(caminho_destino_baseline, caminho_destino_fonteunica, caminho_destino_limpa)
        logger.info("Limpeza dos arquivos realizada com sucesso")

    except Exception as e:
        logger.exception("ETL failed", extra={"status": "error"})
        raise

if __name__ == "__main__":
    run_etl()
    

