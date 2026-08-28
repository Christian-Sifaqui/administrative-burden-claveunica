# detectar_hubs_universales.py
# Detecta servicios que aparecen como destino desde muchos antecedentes distintos.
# Útil para identificar hubs tipo ChileAtiende / MercadoPublico / PJUD / SSO.

import polars as pl
from pathlib import Path

INPUT = Path("output_reglas_v3/reglas_temporales_completo.parquet")

if not INPUT.exists():
    print(f"❌ No existe {INPUT}")
    raise SystemExit(1)

print("📂 Leyendo parquet consolidado...")
df = pl.read_parquet(INPUT)

print(f"✅ Filas: {df.height:,}")

# =====================================================
# HUBS COMO CONSECUENTE
# =====================================================
# cuántos antecedentes distintos apuntan al servicio
# en cuántos pares mensuales aparece
# soporte total acumulado
# lift promedio

hubs = (
    df.group_by("consecuente")
    .agg([
        pl.col("antecedente").n_unique().alias("antecedentes_unicos"),
        pl.len().alias("apariciones"),
        pl.sum("soporte").alias("soporte_total"),
        pl.mean("lift_temporal").alias("lift_prom"),
        (pl.col("mes_a") + "->" + pl.col("mes_b")).n_unique().alias("pares_meses")
    ])
    .with_columns([
        (
            pl.col("antecedentes_unicos") *
            pl.col("apariciones").log10() *
            pl.col("soporte_total").cast(pl.Float64).log10()
        ).alias("hub_score")
    ])
    .sort("hub_score", descending=True)
)

print("\n=== TOP 50 HUBS UNIVERSALES ===")
print(hubs.head(50))

# =====================================================
# HUBS SOSPECHOSOS
# mucho alcance pero lift bajo = destino masivo genérico
# =====================================================

sospechosos = (
    hubs
    .filter(
        (pl.col("antecedentes_unicos") >= 20) &
        (pl.col("lift_prom") <= 5)
    )
    .sort("antecedentes_unicos", descending=True)
)

print("\n=== HUBS MASIVOS / GENÉRICOS ===")
print(sospechosos.head(30))

# =====================================================
# EXPORTAR
# =====================================================

OUT1 = Path("output_reglas_v3/hubs_universales.parquet")
OUT2 = Path("output_reglas_v3/hubs_universales.csv")
OUT3 = Path("output_reglas_v3/hubs_sospechosos.csv")

hubs.write_parquet(OUT1)
hubs.write_csv(OUT2)
sospechosos.write_csv(OUT3)

print("\n💾 Archivos guardados:")
print(" - output_reglas_v3/hubs_universales.parquet")
print(" - output_reglas_v3/hubs_universales.csv")
print(" - output_reglas_v3/hubs_sospechosos.csv")

print(f"\n📊 Servicios destino únicos analizados: {hubs.height:,}")
