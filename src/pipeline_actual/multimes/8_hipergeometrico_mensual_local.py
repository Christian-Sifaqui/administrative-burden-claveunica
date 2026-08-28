"""
Camino A · fase multi-mes -- Paso 8 (LOCAL, no toca AWS): lift y p-valor
hipergeometrico por mes, a partir de los 4 CSV que produce
7_hipergeometrico_mensual.py.

Mismo patron que 4_analisis_red_local.py: el paso pesado (SQL sobre 544M
eventos) corre en AWS y produce CSV chicos; este script solo lee esos CSV y
calcula metricas derivadas -- no requiere duckdb ni acceso a los datos
crudos, solo scipy.

Para cada par de grupos y cada mes, calcula:
  esperado_mes = (marginal_a_mes * marginal_b_mes) / n_total
  lift_mes     = co_uso_mes / esperado_mes
  p_valor_mes  = P(X >= co_uso_mes) bajo Hipergeometrica(n_total, marginal_a_mes,
                 marginal_b_mes) -- cola derecha, scipy.stats.hypergeom.sf con
                 el ajuste -1 estandar para incluir el valor observado.

Y lo mismo para el agregado del periodo completo (hipergeometrico_total_*.csv),
como referencia directa comparable a las cifras ya reportadas en la Tabla
A4.1/A5.1 del paper (mismo N=14.081.161, mismo metodo).

La lectura sustantiva (no calculada aca, se hace a mano sobre la salida):
si un par muestra lift alto SOLO en el/los mes(es) de pico administrativo
compartido (ej. ambos suben en abril) y lift ~1 el resto del anio, eso es
evidencia de estacionalidad compartida, no de dependencia real -- el efecto
que la revision editorial senala como riesgo para el test agregado. Si el
lift se mantiene >1 de forma mas o menos pareja mes a mes (no concentrado
en uno o dos meses), eso respalda que el enriquecimiento del test agregado
no es un artefacto puramente estacional.

Requiere: pip install scipy
"""

import csv
from collections import defaultdict
from pathlib import Path

from scipy.stats import hypergeom

OUTPUT_DIR = Path("output_multimes")


def cargar_marginales_mensuales():
    m = {}  # (grupo, mes) -> (marginal, n_total)
    with open(OUTPUT_DIR / "hipergeometrico_mensual_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m[(row["grupo"], row["mes"])] = (int(row["marginal"]), int(row["n_total"]))
    return m


def cargar_marginales_totales():
    m = {}  # grupo -> (marginal, n_total)
    with open(OUTPUT_DIR / "hipergeometrico_total_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m[row["grupo"]] = (int(row["marginal"]), int(row["n_total"]))
    return m


def lift_y_pvalor(co_uso, marginal_a, marginal_b, n_total):
    if marginal_a == 0 or marginal_b == 0 or n_total == 0:
        return float("nan"), float("nan")
    esperado = (marginal_a * marginal_b) / n_total
    lft = co_uso / esperado if esperado > 0 else float("nan")
    # P(X >= co_uso) bajo Hipergeometrica(N=n_total, K=marginal_a, n=marginal_b)
    p = hypergeom.sf(co_uso - 1, n_total, marginal_a, marginal_b)
    return lft, p


def procesar_mensual(marginales_mes):
    filas_por_par = defaultdict(list)
    with open(OUTPUT_DIR / "hipergeometrico_mensual_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ga, gb, mes, co_uso = row["grupo_a"], row["grupo_b"], row["mes"], int(row["co_uso"])
            ma, n_total = marginales_mes.get((ga, mes), (0, 0))
            mb, _ = marginales_mes.get((gb, mes), (0, 0))
            lft, p = lift_y_pvalor(co_uso, ma, mb, n_total)
            filas_por_par[(ga, gb)].append((mes, co_uso, ma, mb, lft, p))

    out_path = OUTPUT_DIR / "hipergeometrico_mensual_resultado.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["grupo_a", "grupo_b", "mes", "co_uso", "marginal_a", "marginal_b", "lift", "p_valor"])
        for (ga, gb), filas in filas_por_par.items():
            for mes, co_uso, ma, mb, lft, p in sorted(filas):
                w.writerow([ga, gb, mes, co_uso, ma, mb, f"{lft:.4f}", f"{p:.3e}"])
    print(f"Guardado {out_path}")
    return filas_por_par


def procesar_total(marginales_total):
    out_path = OUTPUT_DIR / "hipergeometrico_total_resultado.csv"
    filas_out = []
    with open(OUTPUT_DIR / "hipergeometrico_total_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ga, gb, co_uso = row["grupo_a"], row["grupo_b"], int(row["co_uso"])
            ma, n_total = marginales_total.get(ga, (0, 0))
            mb, _ = marginales_total.get(gb, (0, 0))
            lft, p = lift_y_pvalor(co_uso, ma, mb, n_total)
            filas_out.append((ga, gb, co_uso, ma, mb, lft, p))
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["grupo_a", "grupo_b", "co_uso", "marginal_a", "marginal_b", "lift", "p_valor"])
        for ga, gb, co_uso, ma, mb, lft, p in sorted(filas_out, key=lambda r: -r[5] if r[5] == r[5] else 0):
            w.writerow([ga, gb, co_uso, ma, mb, f"{lft:.4f}", f"{p:.3e}"])
    print(f"Guardado {out_path} (referencia directa, mismo metodo que Tabla A4.1/A5.1 del paper)")
    return filas_out


def resumen_consola(filas_por_par):
    print("\n" + "=" * 70)
    print("RESUMEN -- lift mensual por par (min / mediana / max a lo largo de los meses "
          "con marginal>0 en ambos grupos)")
    print("=" * 70)
    for (ga, gb), filas in sorted(filas_por_par.items()):
        lifts = sorted(l for _, _, _, _, l, _ in filas if l == l)  # descarta NaN
        if not lifts:
            continue
        mediana = lifts[len(lifts) // 2]
        meses_pico = [mes for mes, _, _, _, l, _ in filas if l == l and l >= 2 * mediana]
        print(f"{ga} <-> {gb}: lift min={lifts[0]:.2f} mediana={mediana:.2f} max={lifts[-1]:.2f}"
              + (f" | meses con lift >=2x la mediana: {', '.join(sorted(meses_pico))}" if meses_pico else ""))


def main():
    marginales_mes = cargar_marginales_mensuales()
    marginales_total = cargar_marginales_totales()
    filas_por_par = procesar_mensual(marginales_mes)
    procesar_total(marginales_total)
    resumen_consola(filas_por_par)


if __name__ == "__main__":
    main()
