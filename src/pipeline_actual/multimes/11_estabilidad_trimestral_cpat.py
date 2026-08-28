"""
Camino A · fase multi-mes -- Paso 11 (AWS, requiere duckdb): estabilidad
trimestral para los 3 pares "limpios" del cruce CPAT-vs-logs (Seccion 20 de
este apendice / Seccion 6.5 del paper) -- el ultimo criterio que les falta
para poder promoverlos formalmente a "Pathway" con el mismo estandar de las
Tablas A5.1/A5.2 (pendiente 16).

Los 3 pares (proveedora del documento -> institucion que lo exige, segun
CPAT):
  1. Direccion de Compras y Contratacion Publica (ChileCompra)
     -> Direccion de Contabilidad y Finanzas
  2. Direccion General del Territorio Maritimo y de Marina Mercante (DIRECTEMAR)
     -> Servicio Nacional de Pesca y Acuicultura (SERNAPESCA)
  3. Gendarmeria de Chile -> Direccion de Prevision de Carabineros de Chile (DIPRECA)

Metodo: exactamente el mismo que 2b_pares.py usa para pares_por_trimestre
(Tabla A6.1/A5.1) pero a nivel INSTITUCION en vez de servicio -- reutiliza
la tabla `primera_fecha_institucion` (run, institucion, fecha) ya
materializada por 3_red_institucional.py (Seccion 9 del apendice, ALERTA de
red institucional). No se relee ningun zip ni se recalcula el mapeo
servicio->institucion.

A diferencia de red_institucional_pares.csv (agregado sobre los 24 meses
completos, sin desglose), esta consulta agrupa por date_trunc('quarter',
fecha_b) -- el mismo criterio de "trimestre_b" que ya usa pares_por_trimestre
-- para poder aplicar el mismo chequeo de estabilidad que el resto del
estudio: (a) presencia con soporte razonable en los 8 trimestres 2024T1-2025T4
(no el patron "cero antes de 2025T2, masivo despues" que ya excluyo
Ventanilla Unica Social/Sucursal Virtual FONASA de la Tabla A6.1), y (b) que
la fraccion proveedora-antes-de-dueña no oscile sin patron de un trimestre a
otro (el criterio que ya reclasifico SII<->AFC como "Inestable" en la
Seccion 6.5 del paper).

Requiere: pip install duckdb
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

THREADS = 4
MEMORY_LIMIT = "12GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"

# (proveedora, dueña) -- mismo nombre normalizado (NFKD, sin tildes,
# mayusculas) que ya aparece en red_institucional_marginales.csv
PARES = [
    ("DIRECCION DE COMPRAS Y CONTRATACION PUBLICA", "DIRECCION DE CONTABILIDAD Y FINANZAS"),
    ("DIRECCION GENERAL DEL TERRITORIO MARITIMO Y DE MARINA MERCANTE", "SERVICIO NACIONAL DE PESCA Y ACUICULTURA"),
    ("GENDARMERIA DE CHILE", "DIRECCION DE PREVISION DE CARABINEROS DE CHILE"),
]


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    r = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'primera_fecha_institucion'"
    ).fetchone()
    if r[0] == 0:
        raise SystemExit(
            "Falta la tabla 'primera_fecha_institucion' -- correr 3_red_institucional.py primero "
            "(deberia seguir en el .duckdb de la sesion 2026-07-14, ALERTA B2)."
        )
    return con


def par_trimestral(con, proveedora: str, dueña: str):
    return con.execute(
        """
        SELECT
            date_trunc('quarter', b.fecha) AS trimestre,
            COUNT(*) AS soporte,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) > 0 THEN 1 ELSE 0 END) AS proveedora_antes,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) < 0 THEN 1 ELSE 0 END) AS dueña_antes,
            SUM(CASE WHEN date_diff('day', a.fecha, b.fecha) = 0 THEN 1 ELSE 0 END) AS mismo_dia
        FROM primera_fecha_institucion a
        JOIN primera_fecha_institucion b
          ON a.run = b.run AND a.institucion = ? AND b.institucion = ?
        GROUP BY trimestre
        ORDER BY trimestre
        """,
        [proveedora, dueña],
    ).fetchall()


def main():
    con = conectar()
    filas_out = []

    for proveedora, dueña in PARES:
        print("\n" + "=" * 70)
        print(f"{proveedora} -> {dueña}")
        print("=" * 70)
        filas = par_trimestral(con, proveedora, dueña)
        if not filas:
            print("  SIN CO-USO en ningun trimestre (revisar nombres normalizados).")
            continue
        for trimestre, soporte, prov_antes, dueña_antes, mismo_dia in filas:
            total_dir = prov_antes + dueña_antes
            frac_prov = prov_antes / total_dir if total_dir > 0 else float("nan")
            print(
                f"  {trimestre.date()}  soporte={soporte:>7,}  "
                f"proveedora-antes={frac_prov:6.1%}  "
                f"(prov_antes={prov_antes:,} dueña_antes={dueña_antes:,} mismo_dia={mismo_dia:,})"
            )
            filas_out.append((proveedora, dueña, trimestre.date().isoformat(), soporte,
                               prov_antes, dueña_antes, mismo_dia, frac_prov))

    con.close()

    out_path = OUTPUT_DIR / "cpat_pares_trimestral.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        f.write("proveedora,dueña,trimestre,soporte,proveedora_antes,dueña_antes,mismo_dia,frac_proveedora_antes\n")
        for row in filas_out:
            f.write(",".join(str(x) for x in row) + "\n")
    print(f"\nGuardado en {out_path}. Bajar este CSV para evaluar estabilidad localmente.")


if __name__ == "__main__":
    main()
