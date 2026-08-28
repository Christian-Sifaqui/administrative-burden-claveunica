# detectar_relaciones_sospechosas.py
# Busca reglas probablemente artificiales:
# - lift exagerado con soporte bajo
# - nombres internos / técnicos
# - hubs dominantes
# - pares improbables entre dominios distintos
# - confianza extrema poco creíble

import polars as pl
from pathlib import Path

INPUT = Path("output_reglas_v3/reglas_temporales_completo.parquet")

if not INPUT.exists():
    print(f"❌ No existe {INPUT}")
    raise SystemExit(1)

print("📂 Leyendo parquet consolidado...")
df = pl.read_parquet(INPUT)

print(f"✅ Filas originales: {df.height:,}")

# =====================================================
# PALABRAS SOSPECHOSAS (interno / técnico)
# =====================================================

SOSPECHOSAS = [
    "api", "apis", "ws", "webservice",
    "middleware", "batch", "interno",
    "ambiente", "sistema", "backend",
    "firmador", "midas", "undefined",
    "autenticacion", "autentificacion",
    "single sign", "sso",
    "gestión documental", "gestion documental"
]

# =====================================================
# HELPERS
# =====================================================

def contiene_alguno(expr, palabras):
    out = None
    for p in palabras:
        cond = expr.str.to_lowercase().str.contains(p, literal=True)
        out = cond if out is None else (out | cond)
    return out

# =====================================================
# FRECUENCIA DE CONSECUENTES (para detectar hubs)
# =====================================================

hub_freq = (
    df.group_by("consecuente")
    .agg(pl.len().alias("freq_consecuente"))
)

df = df.join(hub_freq, on="consecuente", how="left")

# =====================================================
# SCORE DE SOSPECHA
# =====================================================

df = df.with_columns([

    # lift exagerado
    (pl.col("lift_temporal") >= 20).cast(pl.Int8).alias("flag_lift_extremo"),

    # soporte bajo relativo
    (pl.col("soporte") <= 350).cast(pl.Int8).alias("flag_soporte_bajo"),

    # confianza muy alta
    (pl.col("confianza") >= 0.85).cast(pl.Int8).alias("flag_confianza_extrema"),

    # nombre técnico antecedente
    contiene_alguno(pl.col("antecedente"), SOSPECHOSAS)
        .cast(pl.Int8)
        .alias("flag_tecnico_a"),

    # nombre técnico consecuente
    contiene_alguno(pl.col("consecuente"), SOSPECHOSAS)
        .cast(pl.Int8)
        .alias("flag_tecnico_b"),

    # hub dominante
    (pl.col("freq_consecuente") >= 40).cast(pl.Int8).alias("flag_hub"),

])

df = df.with_columns([
    (
        pl.col("flag_lift_extremo") * 3 +
        pl.col("flag_soporte_bajo") * 2 +
        pl.col("flag_confianza_extrema") * 2 +
        pl.col("flag_tecnico_a") * 2 +
        pl.col("flag_tecnico_b") * 2 +
        pl.col("flag_hub") * 1
    ).alias("suspicion_score")
])

# =====================================================
# FILTRAR CASOS INTERESANTES
# =====================================================

sospechosas = (
    df.filter(pl.col("suspicion_score") >= 5)
    .select([
        "antecedente",
        "consecuente",
        "soporte",
        "confianza",
        "lift_temporal",
        "mes_a",
        "mes_b",
        "freq_consecuente",
        "suspicion_score"
    ])
    .sort(
        by=["suspicion_score", "lift_temporal"],
        descending=[True, True]
    )
)

print("\n=== TOP 100 RELACIONES SOSPECHOSAS / ARTIFICIALES ===")
print(sospechosas.head(100))

# =====================================================
# RESUMEN POR TIPO DESTINO
# =====================================================

resumen = (
    sospechosas.group_by("consecuente")
    .agg([
        pl.len().alias("casos"),
        pl.mean("suspicion_score").alias("score_prom"),
        pl.mean("lift_temporal").alias("lift_prom")
    ])
    .sort("casos", descending=True)
)

print("\n=== DESTINOS MÁS SOSPECHOSOS ===")
print(resumen.head(30))

# =====================================================
# EXPORTAR
# =====================================================

OUT1 = Path("output_reglas_v3/relaciones_sospechosas.parquet")
OUT2 = Path("output_reglas_v3/relaciones_sospechosas.csv")
OUT3 = Path("output_reglas_v3/resumen_destinos_sospechosos.csv")

sospechosas.write_parquet(OUT1)
sospechosas.write_csv(OUT2)
resumen.write_csv(OUT3)

print("\n💾 Guardado:")
print(" - output_reglas_v3/relaciones_sospechosas.parquet")
print(" - output_reglas_v3/relaciones_sospechosas.csv")
print(" - output_reglas_v3/resumen_destinos_sospechosos.csv")

print(f"\n📊 Casos sospechosos detectados: {sospechosas.height:,}")
