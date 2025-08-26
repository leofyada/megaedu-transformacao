from importacao import converte_parquet
from transformacao import recursos
from config import settings
from carregamento import carregamento
import logging

# Configuração do log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("megaedu.transformacao")

def run_etl():
    
    run = settings.RUN_DATE
    bronze = settings.BRONZE_DIR
    prata = settings.PRATA_DIR
    ouro = settings.OURO_DIR


    caminho_baseline_xlsx = bronze / settings.BASELINE_XLSX.format(run=run)
    caminho_fonteunica_xlsx = bronze / settings.FONTEUNICA_XLSX.format(run=run)

    caminho_baseline_parquet = prata / f"{run}_baseline_escolas.parquet"
    caminho_fonteunica_parquet = prata / f"{run}_fonte_unica.parquet"
    caminho_limpa_parquet = ouro / f"{run}_base_limpa.parquet"

    caminho_base_final = ouro / f"{run}_base_limpa.csv"

    try:

        # Converte arquivos Excel para .parquet
        converte_parquet(caminho_baseline_xlsx, caminho_baseline_parquet)
        logger.info("Conversão do arquivo baseline para parquet realizado com sucesso")
        converte_parquet(caminho_fonteunica_xlsx, caminho_fonteunica_parquet)
        logger.info("Conversão do arquivo fonte única para parquet realizada com sucesso")    

        # Limpa os arquivos e retorna uma base tratada
        recursos(caminho_baseline_parquet, caminho_fonteunica_parquet, caminho_limpa_parquet)
        logger.info("Inclusão de variáveis de recursos/políticas realizada com sucesso")

        # Carrega a base limpa
        carregamento(caminho_limpa_parquet, caminho_base_final)
        logger.info("ETL finalizado!")

    except Exception as e:
        logger.exception("ETL failed", extra={"status": "error"})
        raise

if __name__ == "__main__":
    run_etl()
    

