"""
Camino A · fase multi-mes — Paso 1: ingesta (AWS + DuckDB)
=============================================================
Extiende la precedencia de Camino A mas alla de la ventana de 24h/mismo-dia
(ver camino_a_precedencia/aws/paso_full_precedencia.py, limitado a orden
intra-dia). Este script solo aplana los zips de datos_2026/ a una tabla
DuckDB persistente `eventos(run, fecha, servicio, rango_run)` -- una fila
por persona-dia-servicio, YA DEDUPLICADA dentro del dia (corrige el bug de
multiplicidad encontrado en paso_full_precedencia.py: ahi se contaba cada
combinacion de eventos repetidos, no cada servicio distinto una vez por dia).

Por que DuckDB y no streaming+dict (patron de paso_full_precedencia.py):
el analisis (paso 2) necesita un GROUP BY (run, servicio) sobre ~700-800M
filas para sacar la primera fecha de uso de cada servicio por persona, en
TODO el periodo 2024-2025 (no solo dentro de una fila/dia). Eso es una
agregacion demasiado grande para un dict de Python en una maquina de 15GB
RAM -- ya vimos que un simple Counter reservento memoria a 1/10 de esta
escala en la validacion chica (ver ../validar_rsh_fuas.py). DuckDB agrega
out-of-core (usa disco via PRAGMA temp_directory cuando no cabe en RAM).

CHECKPOINTING: se guarda que (outer_zip, inner_zip) ya se proceso en la
tabla `procesados` del mismo archivo .duckdb -- persiste directo a disco,
no necesita pickle aparte. Si se corta el proceso, correr de nuevo retoma
solo lo que falta. Cada inner zip se inserta en una transaccion (insert +
marca en `procesados` juntos) para que no se pueda "marcar procesado" sin
haber insertado, o viceversa.

Requiere: pip install duckdb pandas tqdm

AJUSTAR ANTES DE CORRER: ZIP_PATHS y DIM_PATH a las rutas reales en AWS.
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
from tqdm import tqdm

csv.field_size_limit(sys.maxsize)

# ── CONFIGURACIÓN — AWS ──────────────────────────────────────────────────────
# Ruta real en la maquina AWS (confirmada por el usuario 2026-07-13): ~/codigos/python/data
# -- la MISMA que usa paso_full_precedencia.py (la ruta con "pyhton" duplicado no existia).
# Nombres de archivo confirmados con GUION, no guion bajo: cu_user_client-2026-07-02-001.zip
# (paso_full_precedencia.py usa guion_bajo en su glob -- si no matcheo nada ahi, es el mismo bug).
ZIP_PATHS = sorted(Path("/home/ubuntu/codigos/python/data").glob("cu_user_client-2026-*.zip"))
DIM_PATH = "/home/ubuntu/codigos/python/data/dim_integracion_cu.csv"

OUTPUT_DIR = Path("output_multimes")
OUTPUT_DIR.mkdir(exist_ok=True)
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"
TEMP_DIR.mkdir(exist_ok=True)

THREADS = 4                 # 4 vCPU en la maquina AWS
MEMORY_LIMIT = "10GB"       # deja margen sobre 15GB RAM (hay 15GB swap de respaldo)
CHUNK = 200_000              # filas persona-dia acumuladas antes de flush a duckdb


def parse_servicios_del_dia(raw: str) -> set[str]:
    """'20:24->ae4c...,20:28->d602...,20:24->ae4c...' -> {'ae4c...','d602...'} (deduplicado)."""
    servicios = set()
    for token in raw.strip().strip('"').split(","):
        token = token.strip()
        if "->" not in token:
            continue
        _, servicio = token.split("->", 1)
        servicio = servicio.strip()
        if servicio:
            servicios.add(servicio)
    return servicios


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    con.execute("""
        CREATE TABLE IF NOT EXISTS eventos(
            run VARCHAR,
            fecha DATE,
            servicio VARCHAR,
            rango_run VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS procesados(
            outer_zip VARCHAR,
            inner_zip VARCHAR,
            PRIMARY KEY (outer_zip, inner_zip)
        )
    """)
    return con


def ya_procesado(con, outer_name: str, inner_name: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM procesados WHERE outer_zip = ? AND inner_zip = ?",
        [outer_name, inner_name],
    ).fetchone()
    return r is not None


def flush(con, buffer: list, outer_name: str, inner_name: str) -> None:
    """Inserta TODO el buffer de un inner zip + marca 'procesados', en una sola
    transaccion. Se llama UNA VEZ por inner zip (no por chunk intermedio) para que
    un corte a mitad de archivo nunca deje filas insertadas sin marcar (lo que
    causaria duplicados al reintentar) ni una marca sin sus filas."""
    con.execute("BEGIN TRANSACTION")
    try:
        if buffer:
            # Insertar en batches de CHUNK filas (mismo INSERT, misma transaccion)
            # solo para no crear un unico DataFrame gigante en RAM de una sentada.
            for i in range(0, len(buffer), CHUNK):
                lote = buffer[i:i + CHUNK]
                df = pd.DataFrame(lote, columns=["run", "fecha", "servicio", "rango_run"])
                con.register("tmp_df", df)
                con.execute("INSERT INTO eventos SELECT * FROM tmp_df")
        con.execute("INSERT INTO procesados VALUES (?, ?)", [outer_name, inner_name])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def procesar_inner_csv(txt, outer_name: str, inner_name: str, con) -> int:
    reader = csv.reader(txt, delimiter=";")
    header = next(reader)
    assert header == ["fecha", "run_anonimizado", "rango_run", "hora_client_id"], header

    buffer = []
    n_filas = 0
    for row in reader:
        if len(row) != 4:
            continue
        n_filas += 1
        fecha = row[0].strip()[:10]
        run = row[1]
        rango_run = row[2]
        servicios = parse_servicios_del_dia(row[3])
        for s in servicios:
            buffer.append((run, fecha, s, rango_run))
    flush(con, buffer, outer_name, inner_name)
    return n_filas


def main():
    if not ZIP_PATHS:
        raise SystemExit("ZIP_PATHS vacío — ajustar la ruta a datos_2026/ en este archivo antes de correr.")

    con = conectar()
    n_procesados_antes = con.execute("SELECT COUNT(*) FROM procesados").fetchone()[0]
    n_eventos_antes = con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
    print(f"Estado cargado: {n_procesados_antes:,} inner zips ya procesados, {n_eventos_antes:,} filas en eventos")

    for outer_path in ZIP_PATHS:
        with zipfile.ZipFile(outer_path) as outer_zip:
            inner_names = sorted(n for n in outer_zip.namelist() if n.lower().endswith(".zip"))
            for inner_name in tqdm(inner_names, desc=outer_path.name):
                if ya_procesado(con, outer_path.name, inner_name):
                    continue
                inner_bytes = outer_zip.read(inner_name)
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_name = next((n for n in inner_zip.namelist() if n.lower().endswith(".csv")), None)
                    if csv_name is not None:
                        with inner_zip.open(csv_name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                            procesar_inner_csv(txt, outer_path.name, inner_name, con)
                    else:
                        con.execute("INSERT INTO procesados VALUES (?, ?)", [outer_path.name, inner_name])
            n_eventos = con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
            print(f"  ✓ {outer_path.name} completo | {n_eventos:,} filas en eventos")

    # Copiar dim_integracion_cu.csv a la BD como tabla, para usarla en el paso 2
    con.execute(f"""
        CREATE OR REPLACE TABLE dim AS
        SELECT client_id AS servicio, nombre_app, institucion,
               nombre_app || ' - ' || institucion AS nombre
        FROM read_csv_auto('{DIM_PATH}')
    """)

    n_total = con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
    n_runs = con.execute("SELECT COUNT(DISTINCT run) FROM eventos").fetchone()[0]
    n_servicios = con.execute("SELECT COUNT(DISTINCT servicio) FROM eventos").fetchone()[0]
    print(f"\nIngesta completa: {n_total:,} filas persona-dia-servicio, {n_runs:,} runs distintos, {n_servicios:,} servicios distintos")
    print(f"Base de datos: {DB_PATH}")
    con.close()


if __name__ == "__main__":
    main()
