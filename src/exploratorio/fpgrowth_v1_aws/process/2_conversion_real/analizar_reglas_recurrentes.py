# analizar_reglas_recurrentes.py

import polars as pl
from pathlib import Path

# Ruta del parquet consolidado
INPUT = Path("output_reglas_v3/reglas_temporales_completo.parquet")

if not INPUT.exists():
    print(f"❌ No existe archivo: {INPUT}")
    raise SystemExit(1)

print("📂 Leyendo archivo...")
df = pl.read_parquet(INPUT)

print(f"✅ Filas: {df.height:,}")
print(f"✅ Columnas: {len(df.columns)}")

# Agrupación solicitada
resultado = (
    df.group_by(["antecedente", "consecuente"])
    .agg([
        pl.len().alias("apariciones"),
        pl.mean("lift_temporal").alias("lift_prom"),
        pl.sum("soporte").alias("soporte_total")
    ])
    .sort(
        by=["apariciones", "soporte_total"],
        descending=[True, True]
    )
)

print("\n=== TOP 50 REGLAS MÁS RECURRENTES ===")
print(resultado.head(50))

# Guardar resultados
OUT_PARQUET = Path("output_reglas_v3/reglas_recurrentes.parquet")
OUT_CSV = Path("output_reglas_v3/reglas_recurrentes.csv")

resultado.write_parquet(OUT_PARQUET)
resultado.write_csv(OUT_CSV)

print(f"\n💾 Guardado parquet: {OUT_PARQUET}")
print(f"💾 Guardado csv:     {OUT_CSV}")
print(f"📊 Total combinaciones únicas: {resultado.height:,}")
