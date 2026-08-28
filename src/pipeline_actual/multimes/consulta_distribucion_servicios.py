"""
Consulta para construir la curva de Lorenz de desigualdad de trafico entre servicios
(pendiente marcada en la Seccion 6.2 del paper, Tabla 5).

Obtiene el total de eventos por CADA servicio (los 1.266, no solo el top 20), usando
la tabla `eventos` ya cargada en el .duckdb del pipeline multimes -- no relee zips.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    filas = con.execute("""
        SELECT servicio, COUNT(*) AS eventos
        FROM eventos
        GROUP BY servicio
        ORDER BY eventos DESC
    """).fetchall()

    con.close()

    out_path = OUTPUT_DIR / "distribucion_eventos_por_servicio.csv"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("servicio,eventos\n")
        for servicio, eventos in filas:
            f.write(f"{servicio},{eventos}\n")

    print(f"Servicios distintos: {len(filas):,}")
    print(f"Guardado en {out_path}")


if __name__ == "__main__":
    main()
