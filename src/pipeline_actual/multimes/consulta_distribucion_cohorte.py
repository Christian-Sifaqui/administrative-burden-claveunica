"""
Consulta chica: distribucion de PERSONAS distintas (no eventos) por grupo etario A-F,
usando la tabla `run_grupo` ya creada y persistida por 2b_pares.py (fase R) en el mismo
.duckdb -- no relee zips, no recalcula nada, solo cuenta filas ya existentes.

Sirve para comparar contra estadisticas externas del INE (ALERTA A8): los grupos A-F
tienen fronteras solapadas a proposito (un run puede contar en 2 grupos), asi que el
denominador correcto es el total de runs UNICOS con grupo asignado, no la suma de los
6 grupos (que se solapa).
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    r = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'run_grupo'"
    ).fetchone()
    if r[0] == 0:
        raise SystemExit("No existe la tabla 'run_grupo' -- correr 2b_pares.py primero.")

    total_runs_con_grupo = con.execute(
        "SELECT COUNT(DISTINCT run) FROM run_grupo"
    ).fetchone()[0]
    print(f"Runs distintos con grupo A-F asignado: {total_runs_con_grupo:,}")
    print()

    print("Distribucion de PERSONAS distintas por grupo (fronteras solapadas -- la suma")
    print("de los 6 grupos puede superar el total, un run cuenta en hasta 2 grupos):")
    print(f"{'grupo':6s} {'edad_aprox':10s} {'n_personas':>14s} {'% del total':>12s}")
    filas = con.execute("""
        SELECT grupo, edad_aprox, COUNT(DISTINCT run) AS n_personas
        FROM run_grupo
        GROUP BY grupo, edad_aprox
        ORDER BY grupo
    """).fetchall()
    for grupo, edad_aprox, n in filas:
        pct = 100 * n / total_runs_con_grupo
        print(f"{grupo:6s} {edad_aprox:10s} {n:>14,} {pct:>11.2f}%")

    con.close()


if __name__ == "__main__":
    main()
