"""
Cruza el resultado de precedencia temporal (Camino A, escala completa) contra
las tablas de soporte transaccional sin orden de FP-Growth y LCM, para armar
la tabla definitiva de la ALERTA A6 del paper.

Input:
  - output_full/pares_orden_lags_full.csv   (Camino A, con dirección/lag)
  - ../../computador_aws/fpgrowth/2024_2025/final_output_traducido_2024_2025.jsonl
  - ../../computador_aws/lcm/final_output_lcm_closed_traducido_2024_2025.jsonl

Output:
  - output_full/tabla_a6.csv
"""
import csv
import json
import re

FP_PATH = "../../computador_aws/fpgrowth/2024_2025/final_output_traducido_2024_2025.jsonl"
LCM_PATH = "../../computador_aws/lcm/final_output_lcm_closed_traducido_2024_2025.jsonl"
PRECEDENCIA_PATH = "output_full/pares_orden_lags_full.csv"
OUT_PATH = "output_full/tabla_a6.csv"

# El pipeline de FP-Growth/LCM tradujo estos 4 client_id usando una versión vieja
# de dim_integracion_cu.csv donde nombre_app era el literal "undefined". Verificado
# por client_id exacto (no por nombre) contra la copia de dim_integracion_cu.csv
# traída de AWS el 2026-07-12 (camino_a_precedencia/aws/output_full/dim_integracion_cu.csv),
# que ya trae el nombre_app corregido para esos mismos client_id:
#   2549b51828ce40af920a932dd2102217 -> SII (Servicio de Impuestos Internos)
#   495c261c0f484bd08e232f7358452c4d -> CMF (Comisión para el Mercado Financiero)
#   e8dde9d26979498786c6ba5a82f70ad3 -> Sucursal Virtual de AFC (AFC Chile)
#   f883bd6d70b043a09837e8b888fe3f65 -> Registro de Empresas y Sociedades (Subsecretaría de Economía)
# No es una suposición sobre agrupación de servicios: es el mismo client_id en ambas tablas.
UNDEFINED_FPGROWTH_POR_INSTITUCION = {
    "servicio de impuestos internos": "sii",
    "comisión para el mercado financiero": "cmf",
    "sociedad administradora de fondos de cesantía de chile ii s.a., afc chile": "sucursal virtual de afc",
    "subsecretaría de economía y empresas de menor tamaño": "registro de empresas y sociedades",
}


def normaliza(nombre: str) -> str:
    """Nombre de servicio -> clave normalizada para cruzar entre fuentes con formatos distintos."""
    # fpgrowth/lcm: "Servicio: Institución"; precedencia: "Servicio - Institución"
    partes = re.split(r":| - ", nombre, maxsplit=1)
    servicio = partes[0].strip().lower()
    institucion = partes[1].strip().lower() if len(partes) > 1 else ""
    if servicio == "undefined" and institucion in UNDEFINED_FPGROWTH_POR_INSTITUCION:
        return UNDEFINED_FPGROWTH_POR_INSTITUCION[institucion]
    return servicio


def carga_itemsets_pares(path: str) -> dict:
    pares = {}
    with open(path) as f:
        for line in f:
            items, soporte = json.loads(line)
            if len(items) != 2:
                continue
            a, b = normaliza(items[0]), normaliza(items[1])
            pares[frozenset((a, b))] = soporte
    return pares


def main():
    fp_pares = carga_itemsets_pares(FP_PATH)
    lcm_pares = carga_itemsets_pares(LCM_PATH)

    filas = []
    with open(PRECEDENCIA_PATH) as f:
        r = csv.DictReader(f)
        for row in r:
            a = normaliza(row["nombre_origen"])
            b = normaliza(row["nombre_destino"])
            clave = frozenset((a, b))
            row["soporte_fpgrowth"] = fp_pares.get(clave, "")
            row["soporte_lcm"] = lcm_pares.get(clave, "")
            row["en_fpgrowth"] = clave in fp_pares
            row["en_lcm"] = clave in lcm_pares
            institucion_origen = row["nombre_origen"].split(" - ", 1)[-1].strip().lower()
            institucion_destino = row["nombre_destino"].split(" - ", 1)[-1].strip().lower()
            row["nombre_corregido_undefined"] = (
                institucion_origen in UNDEFINED_FPGROWTH_POR_INSTITUCION
                or institucion_destino in UNDEFINED_FPGROWTH_POR_INSTITUCION
            )
            filas.append(row)

    # Ordena por soporte de co-ocurrencia (Camino A) descendente
    filas.sort(key=lambda x: -int(x["soporte_co_ocurrencia"]))

    campos = [
        "nombre_origen", "nombre_destino",
        "soporte_co_ocurrencia", "n_origen_antes_destino", "frac_direccion_dominante",
        "clasificacion", "lag_min_mediana", "lag_min_p25", "lag_min_p75",
        "soporte_fpgrowth", "soporte_lcm", "en_fpgrowth", "en_lcm", "nombre_corregido_undefined",
    ]
    with open(OUT_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        w.writeheader()
        w.writerows(filas)

    n_en_fp = sum(1 for x in filas if x["en_fpgrowth"])
    n_en_lcm = sum(1 for x in filas if x["en_lcm"])
    n_consistente = sum(1 for x in filas if x["clasificacion"] == "consistente")
    print(f"Total pares (Camino A, soporte>=50): {len(filas):,}")
    print(f"  presentes tambien en FP-Growth (2024-2025, minsupp 0.0005): {n_en_fp:,}")
    print(f"  presentes tambien en LCM: {n_en_lcm:,}")
    print(f"  clasificados 'consistente' (>=90% direccion dominante): {n_consistente:,}")
    print()
    print("Top 20 pares por soporte de co-ocurrencia, con cruce FP-Growth/LCM:")
    for row in filas[:20]:
        marca_fp = "FP" if row["en_fpgrowth"] else "  "
        marca_lcm = "LCM" if row["en_lcm"] else "   "
        print(
            f"{row['nombre_origen'][:30]:30s} -> {row['nombre_destino'][:30]:30s} "
            f"sop={int(row['soporte_co_ocurrencia']):>10,} frac={float(row['frac_direccion_dominante']):.3f} "
            f"{row['clasificacion']:11s} lagmed={row['lag_min_mediana']:>5} {marca_fp} {marca_lcm}"
        )
    print()
    print("Top 15 pares CONSISTENTES por soporte, con cruce FP-Growth/LCM:")
    consistentes = [x for x in filas if x["clasificacion"] == "consistente"]
    for row in consistentes[:15]:
        marca_fp = "FP" if row["en_fpgrowth"] else "  "
        marca_lcm = "LCM" if row["en_lcm"] else "   "
        print(
            f"{row['nombre_origen'][:30]:30s} -> {row['nombre_destino'][:30]:30s} "
            f"sop={int(row['soporte_co_ocurrencia']):>10,} frac={float(row['frac_direccion_dominante']):.3f} "
            f"lagmed={row['lag_min_mediana']:>5} {marca_fp} {marca_lcm}"
        )


if __name__ == "__main__":
    main()
