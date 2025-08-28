# Bibliotecas
import pandas as pd
from pathlib import Path
from typing import Union

# Pathish pode ser uma string ou um objeto Path
Pathish = Union[str, Path]

# Função que lê arquivo Excel
def read_excel(path: Pathish) -> pd.DataFrame:
    return pd.read_excel(path)

# Função que lê arquivo parquet
def read_parquet(path: Pathish) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")

# Função que escreve arquivo parquet
def write_parquet(df: pd.DataFrame, path: Pathish) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)

