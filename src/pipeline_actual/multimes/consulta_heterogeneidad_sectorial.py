"""
Consulta chica para ALERTA A3: heterogeneidad intra-sector.

Reusa la tabla `eventos` ya cargada en el .duckdb de camino_a_precedencia/multimes/
(no relee zips, no recalcula la ingesta). Para cada sector (ministerio/agrupacion),
calcula:
  - eventos totales del sector
  - cuantas instituciones distintas componen el sector
  - la participacion de la institucion dominante dentro del sector
  - el indice de Herfindahl-Hirschman (HHI) de concentracion institucional intra-sector
    (suma de las participaciones al cuadrado; HHI=1 si una sola institucion concentra
    todo el trafico del sector, HHI bajo si el trafico esta repartido parejo entre varias)

Requiere `cu_con_sectores.csv` (client_id,nombre_app,institucion,sector) en
output_multimes/ (junto al .duckdb), o ajustar SECTORES_PATH abajo.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"
SECTORES_PATH = OUTPUT_DIR / "cu_con_sectores.csv"


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sectores AS
        SELECT client_id AS servicio, institucion, sector
        FROM read_csv_auto('{SECTORES_PATH}', header=True)
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE eventos_por_institucion AS
        SELECT s.sector, s.institucion, COUNT(*) AS eventos
        FROM eventos e
        JOIN sectores s ON e.servicio = s.servicio
        GROUP BY s.sector, s.institucion
    """)

    resultado = con.execute("""
        WITH sector_total AS (
            SELECT sector, SUM(eventos) AS eventos_sector, COUNT(DISTINCT institucion) AS n_instituciones
            FROM eventos_por_institucion
            GROUP BY sector
        ),
        con_participacion AS (
            SELECT
                e.sector, e.institucion, e.eventos,
                e.eventos::DOUBLE / t.eventos_sector AS participacion
            FROM eventos_por_institucion e
            JOIN sector_total t ON e.sector = t.sector
        ),
        dominante AS (
            SELECT sector, institucion AS institucion_dominante, participacion AS participacion_dominante,
                   ROW_NUMBER() OVER (PARTITION BY sector ORDER BY eventos DESC) AS rn
            FROM con_participacion
        ),
        hhi AS (
            SELECT sector, SUM(participacion * participacion) AS hhi
            FROM con_participacion
            GROUP BY sector
        )
        SELECT
            t.sector, t.eventos_sector, t.n_instituciones,
            d.institucion_dominante, d.participacion_dominante, h.hhi
        FROM sector_total t
        JOIN dominante d ON t.sector = d.sector AND d.rn = 1
        JOIN hhi h ON t.sector = h.sector
        ORDER BY t.eventos_sector DESC
    """).fetchdf()

    resultado.to_csv(OUTPUT_DIR / "heterogeneidad_sectorial.csv", index=False)
    print(resultado.to_string(index=False))
    print(f"\nGuardado en {OUTPUT_DIR / 'heterogeneidad_sectorial.csv'}")

    con.close()


if __name__ == "__main__":
    main()
