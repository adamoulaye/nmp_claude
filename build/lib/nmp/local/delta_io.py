"""Ecriture de DataFrames pandas en tables Delta (sans Spark). Sur Databricks: utiliser Spark."""

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .config import DATA_DIR


def to_delta(df: pd.DataFrame, name: str, mode: str = "append") -> str:
    df = df.copy()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].astype("datetime64[us, UTC]")  # Delta: precision microseconde
        elif str(df[c].dtype).startswith("datetime64"):
            df[c] = df[c].astype("datetime64[us]")
    path = DATA_DIR / "delta" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    extra = {"schema_mode": "overwrite"} if mode == "overwrite" else {}
    write_deltalake(str(path), pa.Table.from_pandas(df, preserve_index=False), mode=mode, **extra)  # type: ignore[call-overload]
    return str(path)


def read_delta(name: str) -> pd.DataFrame:
    return DeltaTable(str(DATA_DIR / "delta" / name)).to_pandas()
