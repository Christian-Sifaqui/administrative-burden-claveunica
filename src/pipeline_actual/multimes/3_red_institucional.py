"""
Camino A · fase multi-mes -- Paso 3: red institucional completa (ALERTA de
analisis de redes, Seccion 5.4/6.3 -- nunca se habia implementado de verdad).

A diferencia de los scripts `consulta_soporte_institucional_*.py` (que
calculan soporte marginal/co-uso para un puñado de pares curados a mano,
uno a la vez), este script construye la matriz COMPLETA de co-ocurrencia y
direccion entre TODAS las instituciones con trafico -- el insumo real para
calcular grado, centralidad de intermediacion, densidad y modularidad, en
vez de limitarse a los ~20 pares ya reportados en la Tabla A6.1.

Reusa `primera_fecha` (run, servicio, fecha), ya materializada por
2b_pares.py -- no relee zips, no recalcula el filtro de bots. Se agrega una
capa de mapeo servicio->institucion (desde cu_con_sectores.csv, el mismo
archivo ya usado para los pares curados de A4/A5/A6) y se hace UN self-join
a nivel de institucion en vez de a nivel de servicio.

Por que esto es mas liviano que el self-join de servicios de 2b_pares.py
(que murio dos veces por OOM y requirio particionar en 50 franjas): el
espacio de GROUP BY aca tiene como maximo C(458,2) ~ 104.653 grupos
posibles (instituciones con trafico, no 1.266 servicios), y no se calculan
cuantiles de lag por grupo (el problema de memoria original eran los
sketches de cuantil por grupo, no las filas en si) -- por eso se corre en
una sola pasada, sin sharding.

Outputs (`output_multimes/`):
    red_institucional_pares.csv       -- inst_a, inst_b, soporte, a_antes_b,
                                          b_antes_a, mismo_dia (TODOS los
                                          pares con soporte > 0, no filtrado
                                          por umbral)
    red_institucional_marginales.csv  -- institucion, marginal, n_total

Con estos dos CSV se puede reconstruir localmente (sin mas consultas a AWS)
el grafo dirigido y no dirigido completo, y calcular lift hipergeometrico,
grado, betweenness, densidad y modularidad con networkx.

Requiere: pip install duckdb
"""

import csv
import re
import unicodedata
from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"
MAPA_CSV = Path("/home/ubuntu/codigos/python/data/cu_con_sectores.csv")

THREADS = 4
MEMORY_LIMIT = "10GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"


def normalizar(s: str) -> str:
    """Mismo criterio documentado en apendice_metodologico.md Seccion 2:
    NFKD -> quitar tildes -> mayusculas -> colapsar espacios."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def cargar_mapa_institucion() -> list[tuple[str, str]]:
    filas = []
    with open(MAPA_CSV, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            client_id = row["client_id"].strip()
            inst = normalizar(row["institucion"].strip())
            if client_id and inst:
                filas.append((client_id, inst))
    return filas


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    r = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'primera_fecha'"
    ).fetchone()
    if r[0] == 0:
        raise SystemExit("Falta la tabla 'primera_fecha' -- correr 2b_pares.py (fase B/C) primero.")
    return con


def construir_mapa_institucion(con):
    print("=" * 70)
    print("PASO 3 -- red institucional completa")
    print("=" * 70)
    filas = cargar_mapa_institucion()
    print(f"Mapeo client_id -> institucion normalizada: {len(filas):,} filas "
          f"({len(set(i for _, i in filas)):,} instituciones distintas tras normalizar)")

    con.execute("CREATE OR REPLACE TABLE mapa_institucion (client_id VARCHAR, institucion VARCHAR)")
    con.executemany("INSERT INTO mapa_institucion VALUES (?, ?)", filas)


def construir_primera_fecha_institucion(con):
    print("\nConstruyendo primera_fecha_institucion (run, institucion) -- "
          "MIN(fecha) sobre todos los servicios de esa institucion que la persona uso...")
    con.execute("""
        CREATE OR REPLACE TABLE primera_fecha_institucion AS
        SELECT pf.run, m.institucion, MIN(pf.fecha) AS fecha
        FROM primera_fecha pf
        JOIN mapa_institucion m ON pf.servicio = m.client_id
        GROUP BY pf.run, m.institucion
    """)
    n = con.execute("SELECT COUNT(*) FROM primera_fecha_institucion").fetchone()[0]
    n_inst = con.execute("SELECT COUNT(DISTINCT institucion) FROM primera_fecha_institucion").fetchone()[0]
    n_servicios_sin_mapa = con.execute("""
        SELECT COUNT(DISTINCT pf.servicio) FROM primera_fecha pf
        LEFT JOIN mapa_institucion m ON pf.servicio = m.client_id
        WHERE m.client_id IS NULL
    """).fetchone()[0]
    print(f"Filas (run, institucion): {n:,} | instituciones con trafico: {n_inst:,} | "
          f"servicios sin institucion mapeada (excluidos): {n_servicios_sin_mapa:,}")


def construir_marginales(con):
    print("\nCalculando marginal institucional (COUNT DISTINCT run por institucion)...")
    con.execute("""
        CREATE OR REPLACE TABLE marginal_institucion AS
        SELECT institucion, COUNT(DISTINCT run) AS marginal
        FROM primera_fecha_institucion
        GROUP BY institucion
    """)
    n_total = con.execute("SELECT COUNT(DISTINCT run) FROM primera_fecha").fetchone()[0]
    con.execute(f"""
        COPY (
            SELECT institucion, marginal, {n_total} AS n_total
            FROM marginal_institucion
            ORDER BY marginal DESC
        ) TO '{OUTPUT_DIR}/red_institucional_marginales.csv' (HEADER, DELIMITER ',')
    """)
    print(f"N total (mismo universo que Tablas A4.1/A6.1): {n_total:,}")
    print(f"Guardado en {OUTPUT_DIR}/red_institucional_marginales.csv")


def construir_pares_institucion(con):
    print("\nSelf-join a nivel de institucion (a.institucion < b.institucion) -- "
          "puede tardar varios minutos, sin sharding (ver docstring: espacio de "
          "grupos ~100x mas chico que el self-join de servicios)...")
    con.execute("""
        CREATE OR REPLACE TABLE pares_institucion AS
        SELECT
            a.institucion AS inst_a,
            b.institucion AS inst_b,
            COUNT(*) AS soporte,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) > 0 THEN 1 ELSE 0 END) AS a_antes_b,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) < 0 THEN 1 ELSE 0 END) AS b_antes_a,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) = 0 THEN 1 ELSE 0 END) AS mismo_dia
        FROM primera_fecha_institucion a
        JOIN primera_fecha_institucion b
          ON a.run = b.run AND a.institucion < b.institucion
        GROUP BY a.institucion, b.institucion
    """)
    n_pares = con.execute("SELECT COUNT(*) FROM pares_institucion").fetchone()[0]
    print(f"Pares institucion-institucion con soporte > 0: {n_pares:,}")

    con.execute(f"""
        COPY (
            SELECT * FROM pares_institucion ORDER BY soporte DESC
        ) TO '{OUTPUT_DIR}/red_institucional_pares.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Guardado en {OUTPUT_DIR}/red_institucional_pares.csv")


def main():
    con = conectar()
    construir_mapa_institucion(con)
    construir_primera_fecha_institucion(con)
    construir_marginales(con)
    construir_pares_institucion(con)
    con.close()
    print("\nListo. Bajar red_institucional_pares.csv y red_institucional_marginales.csv "
          "para construir el grafo y calcular metricas de red en local (networkx).")


if __name__ == "__main__":
    main()
