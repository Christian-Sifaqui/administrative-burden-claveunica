# tabla1_final.py
# -------------------------------------------------------------
# Genera:
#   1) tabla1_resumen.csv
#   2) tabla1_anual.csv
#   3) tabla1_mensual.csv
#
# Diseñado para:
#   cu_user_client-2024.zip
#   cu_user_client-2025.zip
#   dim_integracion_cu.csv
#
# Requiere:
#   pip install duckdb tqdm pandas
#
# Ejecutar:
#   python3 tabla1_final.py
# -------------------------------------------------------------

import io
import csv
import zipfile
import duckdb
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
ZIP_FILES = {
    "2024": "../../data/cu_user_client-2024.zip",
    "2025": "../../data/cu_user_client-2025.zip",
}

DIM_FILE = "../../data/dim_integracion_cu.csv"
DB_FILE = "tabla1.duckdb"

THREADS = 8
MEMORY_LIMIT = "12GB"
CHUNK = 200_000

# -------------------------------------------------------------
# DUCKDB
# -------------------------------------------------------------
con = duckdb.connect(DB_FILE)
con.execute(f"PRAGMA threads={THREADS}")
con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")

con.execute("""
CREATE OR REPLACE TABLE eventos(
    anio VARCHAR,
    fecha DATE,
    run VARCHAR,
    servicio VARCHAR
)
""")

# -------------------------------------------------------------
# DIMENSION INSTITUCIONAL
# -------------------------------------------------------------
if not Path(DIM_FILE).exists():
    raise FileNotFoundError("No existe dim_integracion_cu.csv")

dim = pd.read_csv(DIM_FILE, sep=",", dtype=str).fillna("")
dim.columns = [c.strip().lower() for c in dim.columns]

svc_col = next(c for c in dim.columns if "client" in c)
inst_col = next(c for c in dim.columns if "instit" in c)

dim = dim[[svc_col, inst_col]].copy()
dim.columns = ["servicio", "institucion"]

con.register("dim_df", dim)
con.execute("CREATE OR REPLACE TABLE dim AS SELECT * FROM dim_df")

# -------------------------------------------------------------
# BUFFER
# -------------------------------------------------------------
buffer = []
filas_invalidas = 0

def flush():
    global buffer
    if not buffer:
        return

    df = pd.DataFrame(
        buffer,
        columns=["anio", "fecha", "run", "servicio"]
    )

    con.register("tmp_df", df)
    con.execute("INSERT INTO eventos SELECT * FROM tmp_df")
    buffer = []

# -------------------------------------------------------------
# INGESTA
# -------------------------------------------------------------
for anio, zip_path in ZIP_FILES.items():

    path = Path(zip_path)
    if not path.exists():
        print("No existe:", zip_path)
        continue

    with zipfile.ZipFile(path, "r") as top_zip:

        inner_zips = [
            n for n in top_zip.namelist()
            if n.lower().endswith(".zip")
        ]

        for inner_name in tqdm(inner_zips, desc=path.name, unit="zip"):

            raw = top_zip.read(inner_name)

            with zipfile.ZipFile(io.BytesIO(raw), "r") as inner_zip:

                csv_files = [
                    n for n in inner_zip.namelist()
                    if n.lower().endswith(".csv")
                ]

                for csv_name in csv_files:

                    with inner_zip.open(csv_name) as f:

                        reader = csv.DictReader(
                            io.TextIOWrapper(
                                f,
                                encoding="utf-8",
                                errors="replace"
                            ),
                            delimiter=";",
                            quotechar='"'
                        )

                        for row in reader:

                            fecha = row.get("fecha", "").strip()
                            run = row.get("run_anonimizado", "").strip()
                            trazas = row.get("client_id_traza", "").strip()

                            if not fecha or not run or not trazas:
                                filas_invalidas += 1
                                continue

                            servicios = [
                                s.strip()
                                for s in trazas.split(",")
                                if s.strip()
                            ]

                            if not servicios:
                                filas_invalidas += 1
                                continue

                            for s in servicios:
                                buffer.append(
                                    (anio, fecha, run, s)
                                )

                            if len(buffer) >= CHUNK:
                                flush()

flush()

# -------------------------------------------------------------
# TABLA 1 RESUMEN
# -------------------------------------------------------------
tabla1 = con.execute("""
WITH total_inst AS (
    SELECT COUNT(DISTINCT institucion) n
    FROM dim
),

obs AS (
    SELECT
        e.anio,
        COUNT(*) eventos,
        COUNT(DISTINCT e.run) usuarios,
        COUNT(DISTINCT e.servicio) servicios,
        COUNT(DISTINCT d.institucion) instituciones
    FROM eventos e
    LEFT JOIN dim d
      ON e.servicio = d.servicio
    GROUP BY 1
),

tot AS (
    SELECT
        'TOTAL' anio,
        COUNT(*) eventos,
        COUNT(DISTINCT e.run) usuarios,
        COUNT(DISTINCT e.servicio) servicios,
        COUNT(DISTINCT d.institucion) instituciones
    FROM eventos e
    LEFT JOIN dim d
      ON e.servicio = d.servicio
)

SELECT
    anio,
    eventos,
    usuarios,
    servicios,
    instituciones,
    (SELECT n FROM total_inst) instituciones_claveunica,
    ROUND(
        100.0 * instituciones /
        NULLIF((SELECT n FROM total_inst),0),2
    ) cobertura_pct
FROM (
    SELECT * FROM obs
    UNION ALL
    SELECT * FROM tot
)
ORDER BY
CASE
 WHEN anio='2024' THEN 1
 WHEN anio='2025' THEN 2
 ELSE 3
END
""").df()

# -------------------------------------------------------------
# TABLA ANUAL
# -------------------------------------------------------------
tabla_anual = con.execute("""
SELECT
    anio,
    COUNT(*) eventos,
    COUNT(DISTINCT run) usuarios_unicos,
    COUNT(DISTINCT servicio) servicios_distintos
FROM eventos
GROUP BY 1
ORDER BY 1
""").df()

# -------------------------------------------------------------
# TABLA MENSUAL
# -------------------------------------------------------------
tabla_mensual = con.execute("""
SELECT
    anio,
    strftime(fecha,'%Y-%m') mes,
    COUNT(*) eventos,
    COUNT(DISTINCT run) usuarios_unicos
FROM eventos
GROUP BY 1,2
ORDER BY 2
""").df()

# -------------------------------------------------------------
# EXPORTAR
# -------------------------------------------------------------
tabla1.to_csv("tabla1_resumen.csv", index=False)
tabla_anual.to_csv("tabla1_anual.csv", index=False)
tabla_mensual.to_csv("tabla1_mensual.csv", index=False)

# -------------------------------------------------------------
# OUTPUT
# -------------------------------------------------------------
print("\n==============================")
print("TABLA 1 RESUMEN")
print("==============================")
print(tabla1.to_string(index=False))

print("\nFilas inválidas omitidas:", f"{filas_invalidas:,}")

print("\n==============================")
print("TABLA ANUAL")
print("==============================")
print(tabla_anual.to_string(index=False))

print("\n==============================")
print("TABLA MENSUAL")
print("==============================")
print(tabla_mensual.head(24).to_string(index=False))

print("\nArchivos generados:")
print("tabla1_resumen.csv")
print("tabla1_anual.csv")
print("tabla1_mensual.csv")

con.close()
