import polars as pl
import unicodedata

# --- CONFIG ---
MESES = ["2024-01", "2024-02", "2024-03"]
PARQUET_DIR = "output_mes"
DIM_PATH = "../data/dim_integracion_cu.csv"

N_AB_MIN = 200
LIFT_MIN = 5

EXCLUIR = [
    "contrataciones censo 2024",
    "plataforma de capacitacion censo"
]

def limpiar(texto: str) -> str:
    # quita tildes, pasa a minúsculas
    t = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode()
    t = t.lower().strip()
    # unifica duplicados conocidos
    if "evaluacion ambiental" in t:
        return "servicio de evaluacion ambiental"
    if "dipreca" in t:
        return "dipreca"
    return t

# --- 1. Dimensión ---
dim = pl.read_csv(DIM_PATH, infer_schema_length=10000)
dim = dim.with_columns(
    (pl.col("nombre_app") + " - " + pl.col("institucion")).alias("nombre")
).select(["client_id", "nombre"])

datos_meses = {}

for mes in MESES:
    path = f"{PARQUET_DIR}/lift_{mes}.parquet"
    df = pl.read_parquet(path)

    # join nombres
    df = df.join(dim, left_on="servicio_a", right_on="client_id", how="left") \
          .rename({"nombre": "a_nombre"})
    df = df.join(dim, left_on="servicio_b", right_on="client_id", how="left") \
          .rename({"nombre": "b_nombre"})

    df = df.with_columns([
        pl.col("a_nombre").fill_null(pl.col("servicio_a")),
        pl.col("b_nombre").fill_null(pl.col("servicio_b")),
    ])

    # filtro base
    df = df.filter(
        (pl.col("n_ab") >= N_AB_MIN) &
        (pl.col("lift") > LIFT_MIN)
    )

    # excluye Censo
    for patron in EXCLUIR:
        df = df.filter(
            ~pl.col("a_nombre").str.to_lowercase().str.contains(patron) &
            ~pl.col("b_nombre").str.to_lowercase().str.contains(patron)
        )

    # limpia nombres y crea clave de par
    df = df.with_columns([
        pl.col("a_nombre").map_elements(limpiar).alias("a_clean"),
        pl.col("b_nombre").map_elements(limpiar).alias("b_clean"),
    ])
    df = df.with_columns(
        pl.concat_str([
            pl.when(pl.col("a_clean") < pl.col("b_clean")).then(pl.col("a_clean")).otherwise(pl.col("b_clean")),
            pl.lit("||"),
            pl.when(pl.col("a_clean") < pl.col("b_clean")).then(pl.col("b_clean")).otherwise(pl.col("a_clean"))
        ]).alias("pair_key")
    )

    datos_meses[mes] = df

# --- 2. Intersección por grupo ---
for g in ["A","B","C","D","E","F"]:
    set1 = set(datos_meses[MESES[0]].filter(pl.col("grupo")==g)["pair_key"].to_list())
    set2 = set(datos_meses[MESES[1]].filter(pl.col("grupo")==g)["pair_key"].to_list())
    set3 = set(datos_meses[MESES[2]].filter(pl.col("grupo")==g)["pair_key"].to_list())

    comunes = set1 & set2 & set3

    if not comunes:
        print(f"\n=== GRUPO {g} ===\nSin pares estables en 3 meses")
        continue

    # junta métricas promedio
    dfs = []
    for mes in MESES:
        d = datos_meses[mes].filter(
            (pl.col("grupo")==g) & pl.col("pair_key").is_in(list(comunes))
        ).select(["pair_key","a_nombre","b_nombre","n_ab","lift"])
        dfs.append(d)

    final = dfs[0].join(dfs[1], on="pair_key", suffix="_feb") \
                .join(dfs[2], on="pair_key", suffix="_mar")

    final = final.with_columns([
        ((pl.col("n_ab") + pl.col("n_ab_feb") + pl.col("n_ab_mar"))/3).alias("n_ab_prom"),
        ((pl.col("lift") + pl.col("lift_feb") + pl.col("lift_mar"))/3).alias("lift_prom"),
    ]).select([
        "a_nombre","b_nombre","n_ab_prom","lift_prom"
    ]).sort("lift_prom", descending=True)

    print(f"\n=== GRUPO {g} - pares estables ene-feb-mar ===")
    print(final)
