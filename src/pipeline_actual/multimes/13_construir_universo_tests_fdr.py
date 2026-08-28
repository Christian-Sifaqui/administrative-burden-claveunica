"""
Camino A · fase multi-mes -- Paso 13 (LOCAL, no toca AWS): construir el CSV
de entrada para el control de multiplicidad de comparaciones (validacion
estadistica, Problema 1).

Motivacion (2026-08-03): el control de tasa de falso descubrimiento (BH/BY)
necesita el universo COMPLETO de los 87.465 pares de servicios evaluados por
el pipeline secuencial -- no el subconjunto de 5.113 "candidatos fuertes"
que ya produce `9_extender_candidatos_pathway.py` (ese archivo ya esta
prefiltrado por soporte>=50, lift>=1.2, direccion>=70% y estabilidad
completa, exactamente los criterios que se quieren contrastar contra la
correccion FDR -- usarlo aqui seria circular).

Este script reutiliza la MISMA formula, el MISMO N_TOTAL y los MISMOS 3
archivos fuente que el script 9 (documentados ahi), pero NO filtra nada al
escribir la salida: calcula lift/p_valor/misma_institucion para las 87.465
filas completas de `pares_multimes_clasificado.csv`.

Formula (identica a la del script 9, verificada contra el mismo N_TOTAL ya
usado en el resto del estudio):
  lift    = soporte / ((marginal_a * marginal_b) / N_TOTAL)
  p_valor = P(X >= soporte) bajo Hipergeometrica(N_TOTAL, marginal_a, marginal_b)
            (cola superior: scipy.stats.hypergeom.sf(soporte - 1, N_TOTAL, marginal_a, marginal_b))

Salida: `output_multimes/universo_tests_fdr.csv`, con exactamente las
columnas que pide el prompt de validacion estadistica (Problema 1):
  servicio_a, servicio_b, soporte, lift, direccion_pct, p_valor,
  estabilidad_completa, misma_institucion
mas columnas descriptivas extra (nombre_a, nombre_b, marginal_a, marginal_b,
institucion_a, institucion_b) para trazabilidad -- no se piden en el prompt
pero no estorban y permiten auditar el resultado sin volver a cruzar nada.

Requiere: pip install scipy (usar venv desechable si no esta instalado
globalmente, ej. /tmp/venv_scipy_fdr/bin/python)
"""

import csv
from pathlib import Path

from scipy.stats import hypergeom

OUTPUT_DIR = Path("output_multimes")
N_TOTAL = 14_081_161  # mismo universo que el resto del estudio (ver script 9)


def cargar_marginales():
    m = {}
    with open(OUTPUT_DIR / "marginal_usuario_dia.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m[row["servicio"]] = (row["nombre"], int(row["n_usuarios_distintos_control"]))
    return m


def cargar_institucion_por_servicio():
    """client_id -> institucion, desde cu_con_sectores.csv. No todos los
    servicio_a/b de pares_multimes_clasificado.csv aparecen aca -- se marca
    explicitamente cuando no se puede resolver, no se adivina."""
    m = {}
    with open("cu_con_sectores.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cid = row["client_id"].strip()
            inst = row["institucion"].strip()
            if cid and inst:
                m[cid] = inst
    return m


def main():
    marginales = cargar_marginales()
    instituciones = cargar_institucion_por_servicio()
    print(f"Marginales de servicio cargados: {len(marginales):,}")
    print(f"Servicios con institucion resoluble (cu_con_sectores.csv): {len(instituciones):,}")

    n_procesados = 0
    n_sin_marginal = 0
    n_sin_institucion_a = 0
    n_sin_institucion_b = 0
    n_soporte_bajo_50 = 0  # solo para verificar que el archivo fuente ya cumple, no filtra
    filas_salida = []

    with open(OUTPUT_DIR / "pares_multimes_clasificado.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n_procesados += 1
            soporte = int(row["soporte"])
            if soporte < 50:
                n_soporte_bajo_50 += 1  # no deberia ocurrir (HAVING COUNT(*)>=50 en 2b_pares.py)

            ma_info = marginales.get(row["servicio_a"])
            mb_info = marginales.get(row["servicio_b"])
            if ma_info is None or mb_info is None:
                n_sin_marginal += 1
                continue
            _, marginal_a = ma_info
            _, marginal_b = mb_info

            inst_a = instituciones.get(row["servicio_a"])
            inst_b = instituciones.get(row["servicio_b"])
            if inst_a is None:
                n_sin_institucion_a += 1
            if inst_b is None:
                n_sin_institucion_b += 1

            if inst_a is not None and inst_b is not None:
                misma_institucion = (inst_a == inst_b)
                institucion_conocida = True
            else:
                misma_institucion = ""  # desconocido, no se adivina
                institucion_conocida = False

            lift = soporte / ((marginal_a * marginal_b) / N_TOTAL)
            p_valor = hypergeom.sf(soporte - 1, N_TOTAL, marginal_a, marginal_b)

            frac_direccion = float(row["frac_direccion_dominante"])
            estabilidad_completa = (row["clase_estabilidad"] == "estable_completo")

            filas_salida.append({
                "servicio_a": row["servicio_a"],
                "servicio_b": row["servicio_b"],
                "nombre_a": row["nombre_a"],
                "nombre_b": row["nombre_b"],
                "soporte": soporte,
                "marginal_a": marginal_a,
                "marginal_b": marginal_b,
                "lift": lift,
                "p_valor": p_valor,
                "direccion_pct": frac_direccion * 100,
                "estabilidad_completa": estabilidad_completa,
                "clase_estabilidad": row["clase_estabilidad"],
                "institucion_a": inst_a if inst_a is not None else "",
                "institucion_b": inst_b if inst_b is not None else "",
                "misma_institucion": misma_institucion,
                "institucion_conocida": institucion_conocida,
                "lag_dias_mediana": row["lag_dias_mediana"],
            })

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / "universo_tests_fdr.csv"
    campos = [
        "servicio_a", "servicio_b", "nombre_a", "nombre_b", "soporte",
        "marginal_a", "marginal_b", "lift", "p_valor", "direccion_pct",
        "estabilidad_completa", "clase_estabilidad",
        "institucion_a", "institucion_b", "misma_institucion", "institucion_conocida",
        "lag_dias_mediana",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(filas_salida)

    print()
    print(f"Filas procesadas (pares_multimes_clasificado.csv): {n_procesados:,}")
    print(f"  con soporte < 50 (no deberia ocurrir):            {n_soporte_bajo_50:,}")
    print(f"  sin marginal resoluble (excluidas de la salida):  {n_sin_marginal:,}")
    print(f"Filas escritas en universo_tests_fdr.csv:           {len(filas_salida):,}")
    print(f"  con institucion_a NO resoluble:                   {n_sin_institucion_a:,}")
    print(f"  con institucion_b NO resoluble:                   {n_sin_institucion_b:,}")
    n_ambas_conocidas = sum(1 for r in filas_salida if r["institucion_conocida"])
    n_misma = sum(1 for r in filas_salida if r["misma_institucion"] is True)
    print(f"  con AMBAS instituciones resolubles:                {n_ambas_conocidas:,}")
    print(f"  de esas, misma_institucion=True:                   {n_misma:,}")
    print(f"\nSalida: {out_path}")


if __name__ == "__main__":
    main()
