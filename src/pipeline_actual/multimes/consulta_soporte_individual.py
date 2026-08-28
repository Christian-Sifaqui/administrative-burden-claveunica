"""
Consulta chica para ALERTA A4 (baseline de permutacion / significancia de pares).

Reusa `primera_fecha` (ya calculada por 2b_pares.py, fase B/C) para obtener el soporte
INDIVIDUAL (personas distintas que usaron el servicio, dentro del mismo universo
filtrado de bots que se uso para calcular los soportes de pares en pares_multimes.csv).
No relee zips, no recalcula nada nuevo -- un COUNT(DISTINCT run) por servicio.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

SERVICIOS_DE_INTERES = {
    "SII (Servicio de Impuestos Internos)": "2549b51828ce40af920a932dd2102217",
    "AFC (Sucursal Virtual de AFC)": "e8dde9d26979498786c6ba5a82f70ad3",
    "Registro Civil - SRCeI": "123",
    "FONASA - Portal Beneficiario": "b4206ac5e3224dfd900588a8f7caf95e",
    "RSH (Registro Social de Hogares)": "672f76fa24044d36b28fd11b50d7a4ec",
    "FUAS-Mineduc": "0a750e36725e495ba32bd43a5f170724",
}


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    n_total = con.execute("SELECT COUNT(DISTINCT run) FROM primera_fecha").fetchone()[0]
    print(f"N total (runs validos, mismo universo que pares_multimes.csv): {n_total:,}")
    print()

    filas = []
    for nombre, cid in SERVICIOS_DE_INTERES.items():
        n = con.execute(
            "SELECT COUNT(DISTINCT run) FROM primera_fecha WHERE servicio = ?", [cid]
        ).fetchone()[0]
        filas.append((nombre, cid, n))
        print(f"{nombre:40s} ({cid}): {n:,} personas")

    con.close()

    with open(OUTPUT_DIR / "soporte_individual_a4.csv", "w", encoding="utf-8") as f:
        f.write("nombre,client_id,n_personas\n")
        for nombre, cid, n in filas:
            f.write(f'"{nombre}",{cid},{n}\n')
    print(f"\nGuardado en {OUTPUT_DIR / 'soporte_individual_a4.csv'}")


if __name__ == "__main__":
    main()
