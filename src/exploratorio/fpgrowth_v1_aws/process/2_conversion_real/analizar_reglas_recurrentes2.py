import polars as pl
import math

df = pl.read_parquet("output_reglas_v3/reglas_recurrentes.parquet")

df = df.with_columns(
    (
        pl.col("apariciones") *
        (pl.col("soporte_total").cast(pl.Float64).log10()) *
        ((pl.col("lift_prom") + 1).log10())
    ).alias("score")
)

top = df.sort("score", descending=True)

print(top.head(50))

top.write_csv("output_reglas_v3/reglas_top_score.csv")
top.write_parquet("output_reglas_v3/reglas_top_score.parquet")
