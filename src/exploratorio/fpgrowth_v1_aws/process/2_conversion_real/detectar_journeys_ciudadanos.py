# detectar_journeys_ciudadanos.py
# Busca transiciones plausibles entre servicios ciudadanos,
# filtrando hubs internos, APIs y relaciones absurdas.

import polars as pl
from pathlib import Path

INPUT = Path("output_reglas_v3/reglas_temporales_completo.parquet")

if not INPUT.exists():
    print(f"❌ No existe {INPUT}")
    raise SystemExit(1)

print("📂 Leyendo parquet consolidado...")
df = pl.read_parquet(INPUT)

print(f"✅ Filas originales: {df.height:,}")

# ==========================================================
# PALABRAS QUE SUELEN INDICAR SISTEMAS INTERNOS / TÉCNICOS
# ==========================================================

BLOQUEAR = [
    "api", "apis", "ws", "webservice",
    "firmador", "middleware", "backoffice",
    "ambiente", "proveedores", "interno",
    "sistema", "gestion documental",
    "autenticacion", "autentificacion",
    "single sign", "sso",
    "undefined",
    "portal interno",
    "midas"
]

# ==========================================================
# PALABRAS QUE SUGIEREN SERVICIOS CIUDADANOS
# ==========================================================

CIUDADANO = [
    "subsidio", "bono", "registro social",
    "fonasa", "salud", "licencia",
    "tramite", "certificado", "postulacion",
    "empleo", "trabajo", "mi chileatiende",
    "chileatiende", "beneficiario",
    "pension", "afiliacion", "denuncia",
    "fiscalia", "causas", "judicial",
    "movilidad", "transporte",
    "aduana", "servel", "beca",
    "fuas", "educacion"
]

# ==========================================================
# HELPERS
# ==========================================================

def contiene_alguno(expr, palabras):
    out = None
    for p in palabras:
        cond = expr.str.to_lowercase().str.contains(p, literal=True)
        out = cond if out is None else (out | cond)
    return out

# ==========================================================
# FILTRADO
# ==========================================================

df2 = (
    df.filter(
        ~contiene_alguno(pl.col("antecedente"), BLOQUEAR) &
        ~contiene_alguno(pl.col("consecuente"), BLOQUEAR)
    )
    .filter(
        contiene_alguno(pl.col("antecedente"), CIUDADANO) |
        contiene_alguno(pl.col("consecuente"), CIUDADANO)
    )
    .filter(pl.col("antecedente") != pl.col("consecuente"))
)

print(f"✅ Filas tras filtro ciudadano: {df2.height:,}")

# ==========================================================
# CONSOLIDAR JOURNEYS
# ==========================================================

journeys = (
    df2.group_by(["antecedente", "consecuente"])
    .agg([
        pl.len().alias("apariciones"),
        pl.sum("soporte").alias("soporte_total"),
        pl.mean("confianza").alias("conf_prom"),
        pl.mean("lift_temporal").alias("lift_prom"),
        pl.col("grupo").n_unique().alias("grupos_presentes")
    ])
    .filter(
        (pl.col("apariciones") >= 3) &
        (pl.col("soporte_total") >= 1000) &
        (pl.col("lift_prom") >= 2)
    )
    .with_columns([
        (
            pl.col("apariciones") *
            pl.col("soporte_total").cast(pl.Float64).log10() *
            (pl.col("lift_prom") + 1).log10()
        ).alias("journey_score")
    ])
    .sort("journey_score", descending=True)
)

print("\n=== TOP 50 JOURNEYS PLAUSIBLES CIUDADANOS ===")
print(journeys.head(50))

# ==========================================================
# EXPORTAR
# ==========================================================

OUT1 = Path("output_reglas_v3/journeys_ciudadanos.parquet")
OUT2 = Path("output_reglas_v3/journeys_ciudadanos.csv")

journeys.write_parquet(OUT1)
journeys.write_csv(OUT2)

print("\n💾 Guardado:")
print(" - output_reglas_v3/journeys_ciudadanos.parquet")
print(" - output_reglas_v3/journeys_ciudadanos.csv")

print(f"\n📊 Journeys detectados: {journeys.height:,}")
