"""
Camino A · fase multi-mes — Paso 2a: diagnostico (AWS + DuckDB)
====================================================================
Corre DESPUES de 1_ingesta.py. Rapido (minutos): calcula la distribucion de
volumen y de servicios-distintos por run, y una ESTIMACION del tamano del
self-join de 2b_pares.py -- para poder ajustar PCTL_BOT / MAX_SERVICIOS_DISTINTOS_RUN
en 2b_pares.py ANTES de correr el self-join pesado (potencialmente cientos de
millones de filas), no despues.

Deliberadamente separado de 2b_pares.py: revisar la salida de este script a
mano antes de lanzar 2b_pares.py, no encadenarlos automaticamente.

Requiere: pip install duckdb
"""

import zipfile
from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

# Mismas rutas que 1_ingesta.py -- se usan solo para CONTAR cuantos inner zips hay en
# total (namelist es barato, no descomprime nada) y verificar que la ingesta termino.
ZIP_PATHS = sorted(Path("/home/ubuntu/codigos/python/data").glob("cu_user_client-2026-*.zip"))

THREADS = 4
MEMORY_LIMIT = "10GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"

# Deben coincidir con los mismos nombres de constante en 2b_pares.py -- si se
# ajustan aca los umbrales candidatos a probar, ajustar tambien alla.
PCTL_BOT = 0.999
MAX_SERVICIOS_DISTINTOS_RUN = 100


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    return con


def verificar_ingesta_completa(con) -> bool:
    """Cuenta cuantos inner zips deberia haber en total (namelist es barato, no
    descomprime nada) y lo compara contra la tabla `procesados`. Sin esto, nada
    impide correr el diagnostico o el self-join sobre una ingesta a medio terminar
    sin que se note -- los numeros saldrian silenciosamente basados en datos parciales."""
    if not ZIP_PATHS:
        print("ADVERTENCIA: no se pudo verificar completitud -- ZIP_PATHS vacio en este script.")
        return False

    n_esperados = 0
    for outer_path in ZIP_PATHS:
        with zipfile.ZipFile(outer_path) as outer_zip:
            n_esperados += sum(1 for n in outer_zip.namelist() if n.lower().endswith(".zip"))

    n_procesados = con.execute("SELECT COUNT(*) FROM procesados").fetchone()[0]
    completa = n_procesados >= n_esperados
    estado = "COMPLETA" if completa else "INCOMPLETA"
    print(f"Ingesta {estado}: {n_procesados:,} de {n_esperados:,} inner zips procesados.")
    if not completa:
        print("*** Los numeros de este diagnostico (y cualquier self-join en 2b_pares.py) ***")
        print("*** estarian basados en datos PARCIALES. Retomar 1_ingesta.py antes de seguir. ***")
    return completa


def main():
    con = conectar()
    verificar_ingesta_completa(con)
    print()

    con.execute("""
        CREATE OR REPLACE TABLE resumen_run AS
        SELECT
            run,
            COUNT(*) AS n_eventos_totales,
            COUNT(DISTINCT servicio) AS n_servicios_distintos
        FROM eventos
        GROUP BY run
    """)

    n_runs = con.execute("SELECT COUNT(*) FROM resumen_run").fetchone()[0]
    print(f"Runs totales: {n_runs:,}")

    print("\nDistribucion de eventos totales por run (2024-2025 completo):")
    pct = con.execute("""
        SELECT
            quantile_cont(n_eventos_totales, 0.50) AS p50,
            quantile_cont(n_eventos_totales, 0.90) AS p90,
            quantile_cont(n_eventos_totales, 0.99) AS p99,
            quantile_cont(n_eventos_totales, 0.999) AS p999,
            MAX(n_eventos_totales) AS maximo
        FROM resumen_run
    """).fetchone()
    print(f"  p50={pct[0]}  p90={pct[1]}  p99={pct[2]}  p99.9={pct[3]}  max={pct[4]}")

    print("\nDistribucion de servicios DISTINTOS por run (2024-2025 completo):")
    pct2 = con.execute("""
        SELECT
            quantile_cont(n_servicios_distintos, 0.50) AS p50,
            quantile_cont(n_servicios_distintos, 0.90) AS p90,
            quantile_cont(n_servicios_distintos, 0.99) AS p99,
            quantile_cont(n_servicios_distintos, 0.999) AS p999,
            MAX(n_servicios_distintos) AS maximo
        FROM resumen_run
    """).fetchone()
    print(f"  p50={pct2[0]}  p90={pct2[1]}  p99={pct2[2]}  p99.9={pct2[3]}  max={pct2[4]}")

    # NOTA: pares_run en 2b_pares.py usa "a.servicio < b.servicio" (NO ordenado) --
    # cada run con n servicios distintos aporta n*(n-1)/2 filas, no n*(n-1). Verificado
    # empiricamente: sin dividir por 2 la estimacion salio 2.09x el tamano real de
    # pares_run en la prueba local (2.350.094 estimado vs 1.122.832 real).
    est = con.execute(f"""
        SELECT
            SUM(n_servicios_distintos * (n_servicios_distintos - 1) / 2) AS pares_sin_filtro,
            SUM(CASE WHEN n_servicios_distintos <= {MAX_SERVICIOS_DISTINTOS_RUN}
                     THEN n_servicios_distintos * (n_servicios_distintos - 1) / 2 ELSE 0 END) AS pares_con_tope,
            COUNT(*) FILTER (WHERE n_servicios_distintos > {MAX_SERVICIOS_DISTINTOS_RUN}) AS runs_excluidos_por_tope
        FROM resumen_run
    """).fetchone()
    print(f"\nEstimacion de filas del self-join en 2b_pares.py (pares NO ordenados, antes de filtro de bots):")
    print(f"  sin tope de servicios-distintos: {est[0]:,}")
    print(f"  con MAX_SERVICIOS_DISTINTOS_RUN={MAX_SERVICIOS_DISTINTOS_RUN}: {est[1]:,} ({est[2]:,} runs excluidos por el tope)")
    print("\nSi 'con tope' luce demasiado grande para la RAM/disco disponible en esta maquina,")
    print("subir PCTL_BOT (mas estricto) o bajar MAX_SERVICIOS_DISTINTOS_RUN, y correr de nuevo")
    print("este script (barato) antes de lanzar 2b_pares.py (el self-join, caro).")

    con.close()


if __name__ == "__main__":
    main()
