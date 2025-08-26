# Importa bibliotecas
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from datetime import datetime

now = datetime.now()

class Settings(BaseSettings):
    
    DATA_DIR: Path = Path.cwd() / "data"
    BRONZE_DIR: Path = DATA_DIR / "bronze"
    PRATA_DIR: Path = DATA_DIR / "prata"
    OURO_DIR: Path = DATA_DIR / "ouro"
    RUN_DATE: str = now.strftime("%Y%m")

    BASELINE_XLSX: str = "{run}_baseline_escolas.xlsx"
    FONTEUNICA_XLSX: str = "{run}_fonte_unica.xlsx"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()

