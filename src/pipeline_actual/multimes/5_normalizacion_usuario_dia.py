"""
Camino A · fase multi-mes — Paso 5: normalización usuario-día (ALERTA B1)
==========================================================================
Completa la tercera de las tres comparaciones de normalización prometidas en
la Sección 5.5 del paper (usuario único ya está en todo el estudio; eventos
ya se comparó localmente en 6.4 contra distribucion_eventos_por_servicio.csv;
falta usuario-día). Las otras dos NO necesitaron esta corrida porque ya
estaban materializadas; esta sí, porque ninguna tabla local conserva la
fecha de cada evento individual -- `primera_fecha` (2b_pares.py) colapsa al
primer uso de cada persona por servicio, perdiendo exactamente la información
de "día calendario" que esta comparación necesita.

Insight clave que hace esto barato en vez de repetir el riesgo de OOM de
2b_pares.py: la tabla `eventos` (creada por 1_ingesta.py) YA está a grano
(run, fecha, servicio) -- una fila por persona-día-servicio, deduplicada
dentro del día en la ingesta misma (ver 1_ingesta.py líneas 8-10). Por lo
tanto el soporte "usuario-día" de un servicio es simplemente COUNT(*) sobre
esa tabla agrupando por servicio -- un solo GROUP BY de paso único sobre la
tabla ya persistida en disco, sin self-join y sin agregación por franjas.

Alcance: solo soporte marginal por servicio (no pares), para replicar
exactamente la comparación ya hecha localmente entre usuario-único y eventos
(Sección 6.4) -- correlación de Spearman + solapamiento del top-15. Extender
a pares usuario-día quedaría como trabajo futuro si esta primera comparación
mostrara una divergencia sustantiva que lo justifique.

Requiere: pip install duckdb
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

THREADS = 4
MEMORY_LIMIT = "10GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"

# Deben coincidir con 2a_diagnostico.py / 2b_pares.py -- mismo filtro de bots
# usado en todo el resto del pipeline, para que esta comparación sea sobre la
# misma población, no una población distinta.
PCTL_BOT = 0.999
MAX_SERVICIOS_DISTINTOS_RUN = 100


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    for tabla in ("eventos", "resumen_run", "dim"):
        r = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [tabla]
        ).fetchone()
        if r[0] == 0:
            raise SystemExit(f"Falta la tabla '{tabla}' -- correr 1_ingesta.py y 2a_diagnostico.py primero.")
    return con


def main():
    con = conectar()

    print("=" * 70)
    print("Filtro de bots (mismo criterio que 2b_pares.py fase B)")
    print("=" * 70)
    umbral_volumen = con.execute(f"""
        SELECT quantile_cont(n_eventos_totales, {PCTL_BOT}) FROM resumen_run
    """).fetchone()[0]
    print(f"Umbral de volumen (percentil {PCTL_BOT}): {umbral_volumen:,.0f} filas persona-dia-servicio en 2024-2025")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE runs_validos AS
        SELECT run FROM resumen_run
        WHERE n_eventos_totales < {umbral_volumen}
          AND n_servicios_distintos <= {MAX_SERVICIOS_DISTINTOS_RUN}
    """)
    n_validos = con.execute("SELECT COUNT(*) FROM runs_validos").fetchone()[0]
    n_total_runs = con.execute("SELECT COUNT(*) FROM resumen_run").fetchone()[0]
    print(f"Runs validos tras filtro: {n_validos:,} de {n_total_runs:,} ({n_validos/n_total_runs*100:.2f}%)")

    print("\n" + "=" * 70)
    print("Soporte usuario-dia por servicio -- un GROUP BY de paso unico")
    print("=" * 70)
    # Solo COUNT(*) -- nada de COUNT(DISTINCT e.run) aqui: con ~1.266 grupos y
    # algunos servicios con decenas de millones de runs distintos, un distinct
    # count exacto por grupo obligaria a DuckDB a mantener un set de valores
    # por grupo en vez de un acumulador escalar, multiplicando la RAM en uso
    # sin necesidad -- el soporte usuario-unico ya existe en otros archivos
    # (red_institucional_marginales.csv, soporte_individual_a4.csv), no hace
    # falta recalcularlo aca. Con solo COUNT(*), esta consulta es un unico
    # streaming aggregate (misma forma que el paso, ya probado, de fase B/C
    # de 2b_pares.py), no deberia necesitar mas RAM que ese paso.
    con.execute(f"""
        CREATE OR REPLACE TABLE marginal_usuario_dia AS
        SELECT
            e.servicio,
            COUNT(*) AS soporte_usuario_dia
        FROM eventos e
        JOIN runs_validos r ON e.run = r.run
        GROUP BY e.servicio
    """)
    n_servicios = con.execute("SELECT COUNT(*) FROM marginal_usuario_dia").fetchone()[0]
    print(f"Servicios con soporte usuario-dia calculado: {n_servicios:,}")

    con.execute(f"""
        COPY (
            SELECT
                m.servicio,
                d.nombre,
                m.soporte_usuario_dia
            FROM marginal_usuario_dia m
            LEFT JOIN dim d ON m.servicio = d.servicio
            ORDER BY m.soporte_usuario_dia DESC
        ) TO '{OUTPUT_DIR}/marginal_usuario_dia.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Exportado a {OUTPUT_DIR}/marginal_usuario_dia.csv")
    con.close()


if __name__ == "__main__":
    main()
