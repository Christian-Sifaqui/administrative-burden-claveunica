"""
Consulta para ALERTA A4 (extension): baseline de permutacion / hipergeometrico
para el par institucional Poder Judicial <-> Ministerio Publico (Tabla A5.1,
Seccion 6.5), que quedo con "No calculado" en la columna "Lift vs. azar".

A diferencia de consulta_soporte_individual.py (que opera a nivel de UN solo
servicio), esta consulta calcula:
  - el soporte marginal institucional (usuarios con AL MENOS UN servicio de
    cada institucion, agregando todos los client_id de esa institucion segun
    data/cu_con_sectores.csv)
  - el co-uso institucional REAL (usuarios con al menos un servicio de AMBAS
    instituciones), deduplicado a nivel de persona

Esto es distinto de la cifra ya reportada en la Tabla A5.1 (81.159), que es
la SUMA de los soportes de los 2 pares de servicios corroborantes
(Denuncia en Linea<->PJUD + Escritorio de Aplicaciones<->PJUD) y puede tener
un pequenio doble conteo si alguna persona aparece en ambos pares. Esta
consulta da el numero deduplicado correcto para el test hipergeometrico.

Reusa `primera_fecha` (ya calculada por 2b_pares.py, fase B/C), igual que
consulta_soporte_individual.py -- no relee zips, no recalcula nada nuevo.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

# client_id -> institucion, desde data/cu_con_sectores.csv (institucion == "Poder Judicial" exacto)
PJUD_IDS = [
    "8ccf6c97b6eb4670bafec186fbe64f85",  # Buscador de Jurisprudencia
    "cc28da1df13d4147b5420ccc231a458e",  # Canal de denuncias
    "ae4caa1cb2154cac89440f3fe18251cb",  # Ingreso de causas y escritos
    "d602a0071f3f4db8b37a87cffd89bf23",  # PJUD
]

# client_id -> institucion, desde data/cu_con_sectores.csv (institucion == "Ministerio Público" exacto)
MP_IDS = [
    "165139dad00243e695f7f3a695563ae2",  # Agendamiento EIVG
    "01b56ab5b9be43e19fa06300994787ec",  # Bitácora Web
    "399e7fc266894562850261cfbc06b992",  # Denuncia en Linea
    "56da1c80cfea4a5b83326bfef271fc3e",  # Escritorio de Aplicaciones
    "d0bb6506628a4f37bbee7e2aa4097b92",  # Fiscalía Atiende
    "26b4ed3b679045889fdc5539a4c2d4fc",  # Incautación de Vehículos
    "347178b052294153af290d6a84589868",  # Mi Fiscalía en Línea
    "fb74fb1e98d447cc875898434e8135bc",  # SigesPass
    "f15bd4421f604ffbae3d627bef601546",  # Transferencia de Archivos
]


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    n_total = con.execute("SELECT COUNT(DISTINCT run) FROM primera_fecha").fetchone()[0]
    print(f"N total (mismo universo que pares_multimes.csv): {n_total:,}")

    pjud_ph = ",".join(["?"] * len(PJUD_IDS))
    mp_ph = ",".join(["?"] * len(MP_IDS))

    n_pjud = con.execute(
        f"SELECT COUNT(DISTINCT run) FROM primera_fecha WHERE servicio IN ({pjud_ph})",
        PJUD_IDS,
    ).fetchone()[0]
    print(f"Usuarios con >=1 servicio de Poder Judicial: {n_pjud:,}")

    n_mp = con.execute(
        f"SELECT COUNT(DISTINCT run) FROM primera_fecha WHERE servicio IN ({mp_ph})",
        MP_IDS,
    ).fetchone()[0]
    print(f"Usuarios con >=1 servicio de Ministerio Público: {n_mp:,}")

    n_both = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT run FROM primera_fecha WHERE servicio IN ({pjud_ph})
            INTERSECT
            SELECT run FROM primera_fecha WHERE servicio IN ({mp_ph})
        )
        """,
        PJUD_IDS + MP_IDS,
    ).fetchone()[0]
    print(f"Usuarios con >=1 servicio de AMBAS instituciones (co-uso real, deduplicado): {n_both:,}")

    con.close()

    out_path = OUTPUT_DIR / "soporte_institucional_pjud_mp_a4.csv"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("n_total,n_pjud,n_mp,n_both\n")
        f.write(f"{n_total},{n_pjud},{n_mp},{n_both}\n")
    print(f"\nGuardado en {out_path}")


if __name__ == "__main__":
    main()
