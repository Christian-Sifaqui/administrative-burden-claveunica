"""
Consulta para responder la nota "VER: hay una caida consistente en 2025" (Tabla 4,
seccion 6.2 del paper). Compara el top de instituciones por EVENTOS en 2024 vs 2025
por separado, para distinguir entre dos hipotesis:
  (a) diversificacion real del ecosistema (mas instituciones, reparto mas parejo)
  (b) eventos puntuales en 2024 que inflaron una o pocas instituciones especificas

Reusa la tabla `eventos` ya cargada en el .duckdb del pipeline multimes (tiene columna
`fecha`, de ahi se extrae el anio) -- no relee zips.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"
DIM_PATH = "/home/ubuntu/codigos/python/data/dim_integracion_cu.csv"


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim AS
        SELECT client_id AS servicio, institucion
        FROM read_csv_auto('{DIM_PATH}', header=True)
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE eventos_por_institucion_anio AS
        SELECT YEAR(e.fecha) AS anio, d.institucion, COUNT(*) AS eventos
        FROM eventos e
        JOIN dim d ON e.servicio = d.servicio
        GROUP BY YEAR(e.fecha), d.institucion
    """)

    for anio in (2024, 2025):
        total_anio = con.execute(
            "SELECT SUM(eventos) FROM eventos_por_institucion_anio WHERE anio = ?", [anio]
        ).fetchone()[0]
        print(f"=== TOP 10 instituciones {anio} (total eventos: {total_anio:,}) ===")
        filas = con.execute("""
            SELECT institucion, eventos, 100.0*eventos/? AS share_pct
            FROM eventos_por_institucion_anio
            WHERE anio = ?
            ORDER BY eventos DESC
            LIMIT 10
        """, [total_anio, anio]).fetchall()
        for inst, ev, pct in filas:
            print(f"  {inst:60s} {ev:>14,}  {pct:>6.2f}%")
        print()

    # comparacion directa: instituciones con mayor CAIDA absoluta de eventos 2024->2025
    print("=== Instituciones con mayor caida de eventos 2024 -> 2025 ===")
    caida = con.execute("""
        WITH p2024 AS (SELECT institucion, eventos AS ev2024 FROM eventos_por_institucion_anio WHERE anio=2024),
             p2025 AS (SELECT institucion, eventos AS ev2025 FROM eventos_por_institucion_anio WHERE anio=2025)
        SELECT COALESCE(p2024.institucion, p2025.institucion) AS institucion,
               COALESCE(ev2024,0) AS ev2024, COALESCE(ev2025,0) AS ev2025,
               COALESCE(ev2025,0) - COALESCE(ev2024,0) AS diferencia
        FROM p2024 FULL OUTER JOIN p2025 ON p2024.institucion = p2025.institucion
        ORDER BY diferencia ASC
        LIMIT 15
    """).fetchall()
    for inst, e24, e25, diff in caida:
        print(f"  {inst:60s} 2024={e24:>12,}  2025={e25:>12,}  diff={diff:>12,}")

    con.close()


if __name__ == "__main__":
    main()
