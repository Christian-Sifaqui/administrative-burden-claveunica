import polars as pl

# --- CONFIG ---
PARQUET_PATH = "output_mes/lift_2024-01.parquet"
DIM_PATH = "../data/dim_integracion_cu.csv"
TOP_N = 10

# --- 1. Cargar dimensión ---
dim = pl.read_csv(DIM_PATH, infer_schema_length=10000)
# crea etiqueta legible
dim = dim.with_columns(
    (pl.col("nombre_app") + " - " + pl.col("institucion")).alias("nombre_completo")
).select(["client_id", "nombre_completo"])

# --- 2. Cargar lift ---
df = pl.read_parquet(PARQUET_PATH)

# --- 3. Join para servicio_a y servicio_b ---
df = df.join(
    dim, left_on="servicio_a", right_on="client_id", how="left"
).rename({"nombre_completo": "servicio_a_nombre"})

df = df.join(
    dim, left_on="servicio_b", right_on="client_id", how="left"
).rename({"nombre_completo": "servicio_b_nombre"})

# si no encuentra en la dimensión, deja el hash
df = df.with_columns([
    pl.col("servicio_a_nombre").fill_null(pl.col("servicio_a")),
    pl.col("servicio_b_nombre").fill_null(pl.col("servicio_b")),
])

# --- 4. Mostrar top por grupo ---
for g in ["A","B","C","D","E","F"]:
    print(f"\n=== GRUPO {g} ===")
    out = (
        df.filter(pl.col("grupo") == g)
          .sort("lift", descending=True)
          .select([
              "servicio_a_nombre",
              "servicio_b_nombre",
              "n_ab",
              "lift",
              "total_personas"
          ])
          .head(TOP_N)
    )
    print(out)
