"""
Camino A · fase multi-mes -- Paso 14 (LOCAL, no toca AWS): control de tasa de
falso descubrimiento (FDR) sobre el universo completo de tests hipergeometricos
del pipeline secuencial. Implementa el Problema 1 de `validacion_estadistica.md`.

SUPUESTOS SOBRE EL FORMATO DE ENTRADA (declarados antes de escribir el resto
del script, tal como pide el encargo):
  - El CSV de entrada ya trae p_valor precalculado (columna `p_valor`), mas
    `soporte`, `lift`, `direccion_pct` (0-100, NO 0-1), `estabilidad_completa`
    (booleano Python "True"/"False" como string), y `misma_institucion`
    (booleano, o cadena vacia "" cuando la institucion de alguno de los dos
    lados no es resoluble contra el catalogo -- ver columna auxiliar
    `institucion_conocida`). Ambas columnas booleanas se parsean de forma
    tolerante ("True"/"true"/"1" -> True).
  - `p_valor` viene de hypergeom.sf(soporte-1, N_TOTAL, marginal_a, marginal_b)
    (cola superior, ver `13_construir_universo_tests_fdr.py`) -- este script
    NO recalcula p-valores por defecto (--modo-p precalculado), pero soporta
    --modo-p calcular si se le dan columnas n_ambos,n_a,n_b,n_total en vez de
    p_valor, para reproducir el encargo original que preveia ambos modos.
  - El "family" de tests para BH/BY es exactamente el conjunto de filas del
    CSV de entrada -- si se quiere excluir pares con institucion desconocida
    o intra-institucion del family completo, eso se hace ANTES de invocar
    este script (filtrando el CSV), no dentro de el. Asi el mismo script
    sirve para correr dos veces sobre dos universos distintos y comparar,
    que es exactamente lo que pidio Christian el 2026-08-03 en vez de decidir
    unilateralmente que hacer con los ~6.307 pares de institucion desconocida.

DECISIONES DISCUTIBLES (declaradas, no escondidas):
  - Storey pi_0: se implementa el metodo de spline cubico sobre una grilla de
    lambda de 0 a 0.90 en pasos de 0.05 (Storey & Tibshirani, 2003), evaluado
    en lambda=1 y truncado a [0,1]. No es la unica variante publicada; se
    documenta la formula exacta en el codigo para que sea auditable.
  - El "esperado por azar" del punto 2 se calcula como 0.05 * N sobre el N
    del archivo de entrada tal cual llega (ya filtrado a soporte>=50 en el
    origen del pipeline) -- no sobre el universo combinatorio completo de
    pares de servicios posibles. Esto se declara explicitamente en el TXT de
    salida para que no se lea como "el azar esperado si no hubiera filtro
    ex ante de soporte", que seria un numero distinto y mas alto.
  - Analisis de sensibilidad (punto 9): el archivo de entrada YA esta
    filtrado a soporte>=50 en el origen (HAVING COUNT(*)>=50 en la query de
    AWS que genera pares_multimes.csv) -- no existen filas con soporte 25-49
    en ningun archivo local. El umbral soporte>=25 pedido en el encargo NO
    se puede evaluar con estos datos sin volver a la maquina AWS a rehacer
    el self-join con un HAVING mas bajo. Este script lo reporta como "no
    disponible" en vez de inventar un numero o fallar en silencio.
"""

import argparse
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from scipy.interpolate import UnivariateSpline
from statsmodels.stats.multitest import multipletests

UMBRALES_SOPORTE_SENSIBILIDAD = [25, 50, 100, 200, 500, 1000]

# --------------------------------------------------------------------------
# Exclusion de los 17 pares ya evaluados a mano en la Seccion 7.1 del paper
# (decision de Christian, 2026-08-05): sin esta exclusion el conteo de
# candidatos que superan los cuatro criterios computables da 5.826, un TERCER
# numero distinto de los 5.113 publicados en la Seccion 4.6 y de los 5.902 que
# la Seccion 4.9 ya explica por estas mismas dos exclusiones. Replicarla aqui
# evita tener que explicar dos veces la misma discrepancia.
#
# Transcrito literalmente de 9_extender_candidatos_pathway.py (no
# reimplementado): misma lista PARES_YA_EVALUADOS, mismo mapeo _ETIQUETAS por
# substring, misma funcion normalizar/etiqueta/ya_evaluado. Si esa lista
# cambia alla, debe cambiar aca -- estan deliberadamente duplicadas porque los
# dos scripts corren en contextos distintos (AWS/local) y no comparten modulo.
# --------------------------------------------------------------------------
PARES_YA_EVALUADOS = {
    frozenset(["SII", "AFC"]),
    frozenset(["REGISTRO CIVIL", "FONASA"]),
    frozenset(["RSH", "FUAS"]),
    frozenset(["PODER JUDICIAL", "MINISTERIO PUBLICO"]),
    frozenset(["PORTAL MIDT", "FONASA"]),
    frozenset(["SENCE", "FONASA"]),
    frozenset(["CMF", "AFC"]),
    frozenset(["CMF", "SII"]),
    frozenset(["SENCE", "AFC"]),
    frozenset(["PORTAL MIDT", "AFC"]),
    frozenset(["CHILECOMPRA", "SUBSECRETARIA DE ECONOMIA Y EMPRESAS DE MENOR TAMANO"]),
    frozenset(["MDS", "FOSIS"]),
    frozenset(["INE", "CARABINEROS"]),
    frozenset(["CHILECOMPRA", "CARABINEROS"]),
    frozenset(["INE", "SUBSECRETARIA DE EDUCACION"]),
    frozenset(["INE", "SERVICIO ELECTORAL"]),
    frozenset(["FONASA", "AFP UNO"]),
}

_ETIQUETAS = [
    ("SERVICIO DE IMPUESTOS INTERNOS", "SII"),
    (" SII", "SII"),
    ("AFC", "AFC"),
    ("SERCEI", "REGISTRO CIVIL"),
    ("SRCEI", "REGISTRO CIVIL"),
    ("REGISTRO CIVIL", "REGISTRO CIVIL"),
    ("FONASA", "FONASA"),
    ("REGISTRO SOCIAL DE HOGARES", "RSH"),
    (" RSH", "RSH"),
    ("FUAS", "FUAS"),
    ("PODER JUDICIAL", "PODER JUDICIAL"),
    ("PJUD", "PODER JUDICIAL"),
    ("MINISTERIO PUBLICO", "MINISTERIO PUBLICO"),
    ("FISCALIA", "MINISTERIO PUBLICO"),
    ("MIDT", "PORTAL MIDT"),
    ("DIRECCION DEL TRABAJO", "PORTAL MIDT"),
    ("SENCE", "SENCE"),
    ("MERCADO FINANCIERO", "CMF"),
    ("CHILECOMPRA", "CHILECOMPRA"),
    ("COMPRAS Y CONTRATACION PUBLICA", "CHILECOMPRA"),
    ("ECONOMIA Y EMPRESAS DE MENOR TAMANO", "SUBSECRETARIA DE ECONOMIA Y EMPRESAS DE MENOR TAMANO"),
    ("DESARROLLO SOCIAL", "MDS"),
    ("FOSIS", "FOSIS"),
    ("INSTITUTO NACIONAL DE ESTADISTICAS", "INE"),
    ("CARABINEROS", "CARABINEROS"),
    ("SUBSECRETARIA DE EDUCACION", "SUBSECRETARIA DE EDUCACION"),
    ("SERVICIO ELECTORAL", "SERVICIO ELECTORAL"),
    ("AFP UNO", "AFP UNO"),
]


def _normalizar(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.upper()


def _etiqueta(nombre: str):
    n = _normalizar(nombre)
    for substr, et in _ETIQUETAS:
        if substr in n:
            return et
    return None


def ya_evaluado(nombre_a: str, nombre_b: str) -> bool:
    ea, eb = _etiqueta(nombre_a), _etiqueta(nombre_b)
    if ea is None or eb is None or ea == eb:
        return False
    return frozenset([ea, eb]) in PARES_YA_EVALUADOS


def parse_bool(x):
    if isinstance(x, bool):
        return x
    if pd.isna(x):
        return None
    s = str(x).strip().lower()
    if s in ("true", "1", "yes", "si"):
        return True
    if s in ("false", "0", "no", ""):
        return False if s != "" else None
    return None


def cargar_datos(path_csv: Path, modo_p: str, n_total: int | None) -> pd.DataFrame:
    df = pd.read_csv(path_csv, dtype={"servicio_a": str, "servicio_b": str})

    if modo_p == "calcular":
        requeridas = {"n_ambos", "n_a", "n_b", "n_total"}
        faltan = requeridas - set(df.columns)
        if faltan:
            sys.exit(f"--modo-p calcular requiere columnas {sorted(faltan)} ausentes en el CSV")
        # Cola superior: interesa el enriquecimiento (mas coocurrencia de la
        # esperada por azar), no el empobrecimiento -- P(X >= soporte).
        df["p_valor"] = stats.hypergeom.sf(
            df["n_ambos"] - 1, df["n_total"], df["n_a"], df["n_b"]
        )
    else:
        if "p_valor" not in df.columns:
            sys.exit("--modo-p precalculado requiere columna p_valor en el CSV")

    for col in ("estabilidad_completa", "misma_institucion"):
        if col in df.columns:
            df[col] = df[col].apply(parse_bool)

    if "direccion_pct" not in df.columns and "direccion" in df.columns:
        df["direccion_pct"] = df["direccion"] * 100

    return df


def descriptivos_pvalores(df: pd.DataFrame, outdir: Path, log):
    p = df["p_valor"].to_numpy()
    n_cero = int((p == 0.0).sum())
    pct_cero = 100 * n_cero / len(p)

    log(f"\n[1] Descriptivos de p-valores (N={len(p):,})")
    log(f"    min={p.min():.6g}  max={p.max():.6g}")
    log(f"    p_valor == 0.0 exacto (underflow de punto flotante): {n_cero:,} ({pct_cero:.2f}%)")
    if pct_cero > 1.0:
        log(f"    ADVERTENCIA: más del 1% de los p-valores son cero por underflow "
            f"({pct_cero:.2f}%). El ranking usa -log10(p) vía hypergeom.logsf para estos "
            f"casos, no el p-valor lineal.")

    # Trabajar en escala log cuando hay underflow: hypergeom no expone logsf
    # publico de forma directa en todas las versiones de scipy, así que se
    # reconstruye a partir de logsf si existe, y si no, se usa como proxy
    # -log10(soporte/marginal) ... en vez de eso, aqui se usa directamente
    # scipy.stats.hypergeom(...).logsf(k), que SI esta disponible (metodo de
    # instancia de la distribucion congelada) y evita el underflow del sf().
    fig_hist = outdir / "fig_histograma_pvalores.png"
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(p, bins=50, color="#4C72B0", edgecolor="white")
    ax.set_xlabel("p-valor")
    ax.set_ylabel("N pares")
    ax.set_title(f"Histograma de p-valores (N={len(p):,}, {n_cero:,} en 0.0 exacto)")
    fig.tight_layout()
    fig.savefig(fig_hist, dpi=150)
    plt.close(fig)
    log(f"    Figura: {fig_hist}")

    return n_cero, pct_cero


def logsf_robusto(df: pd.DataFrame) -> np.ndarray:
    """log10(p-valor) robusto a underflow, usado solo para RANKING, no para
    los q-valores en si (esos siguen usando p_valor lineal como pide el
    encargo, via statsmodels).

    Optimizacion importante: scipy.stats.hypergeom.logsf, cuando los
    parametros de forma (M, n, N) varian fila a fila (no estan "congelados"
    en una sola distribucion), NO esta vectorizado internamente en C para
    este caso -- itera en Python por dentro, tan lento como un bucle propio
    (~87.465 filas tardaron >10 min sin terminar en la primera corrida).
    Como el p_valor lineal YA es exacto y ordena correctamente en todas las
    filas donde no hizo underflow a 0.0, este calculo caro solo hace falta
    para las filas con p_valor==0.0 exacto (en este dataset, ~14.983 de
    87.465, 17,1%) -- las demas usan -log10(p_valor) directo, sin volver a
    llamar a scipy. Recorta el trabajo real en ~83%."""
    log10p = -np.log10(np.maximum(df["p_valor"].to_numpy(), 1e-300))

    underflow = df["p_valor"].to_numpy() == 0.0
    n_underflow = int(underflow.sum())
    if n_underflow == 0 or not {"soporte", "marginal_a", "marginal_b"}.issubset(df.columns):
        return log10p

    if "n_total" in df.columns:
        n_total_arr = df.loc[underflow, "n_total"].to_numpy()
    else:
        n_total_arr = np.full(n_underflow, 14_081_161)

    k = df.loc[underflow, "soporte"].to_numpy()
    na = df.loc[underflow, "marginal_a"].to_numpy()
    nb = df.loc[underflow, "marginal_b"].to_numpy()

    log10p_exacto = np.empty(n_underflow)
    for i in range(n_underflow):
        log10p_exacto[i] = stats.hypergeom.logsf(k[i] - 1, n_total_arr[i], na[i], nb[i]) / np.log(10)

    log10p[underflow] = -log10p_exacto  # logsf da log(p) negativo; log10p aqui se define como -log10(p) > 0
    return log10p


def storey_pi0(pvals: np.ndarray) -> float:
    """Storey & Tibshirani (2003), 'Statistical significance for genomewide
    studies', PNAS 100(16), 9440-9445. pi_0(lambda) = #{p_i > lambda} /
    (N*(1-lambda)); se ajusta un spline cubico sobre una grilla de lambda y
    se evalua en lambda=1."""
    n = len(pvals)
    lambdas = np.arange(0.0, 0.91, 0.05)
    pi0_lambda = np.array([np.mean(pvals > lam) / (1 - lam) for lam in lambdas])
    try:
        spline = UnivariateSpline(lambdas, pi0_lambda, k=3, s=len(lambdas))
        pi0 = float(spline(1.0))
    except Exception:
        pi0 = float(pi0_lambda[-1])  # fallback: pi0 en el lambda mas alto de la grilla
    return float(np.clip(pi0, 0.0, 1.0))


def storey_qvalues(pvals: np.ndarray, pi0: float) -> np.ndarray:
    n = len(pvals)
    orden = np.argsort(pvals)
    p_ord = pvals[orden]
    q_ord = np.empty(n)
    q_ord[-1] = pi0 * p_ord[-1]
    for i in range(n - 2, -1, -1):
        cand = pi0 * n * p_ord[i] / (i + 1)
        q_ord[i] = min(cand, q_ord[i + 1])
    q = np.empty(n)
    q[orden] = np.clip(q_ord, 0, 1)
    return q


def verificar_bh_manual(pvals: np.ndarray, q: float) -> np.ndarray:
    """Reimplementacion directa de la formula BH (1995) para verificar contra
    statsmodels: p_(i) <= (i/m)*q, tomar el mayor i que cumple, todos los
    i' <= i sobreviven."""
    n = len(pvals)
    orden = np.argsort(pvals)
    p_ord = pvals[orden]
    i = np.arange(1, n + 1)
    umbral = (i / n) * q
    cumple = p_ord <= umbral
    if not cumple.any():
        return np.zeros(n, dtype=bool)
    i_max = np.max(np.where(cumple)[0])
    sobrevive_ord = np.zeros(n, dtype=bool)
    sobrevive_ord[: i_max + 1] = True
    sobrevive = np.empty(n, dtype=bool)
    sobrevive[orden] = sobrevive_ord
    return sobrevive


def main():
    ap = argparse.ArgumentParser(description="Control FDR sobre el universo de tests hipergeometricos (Problema 1, validacion_estadistica.md)")
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--modo-p", choices=["precalculado", "calcular"], default="precalculado")
    ap.add_argument("--pathways", type=Path, help="CSV con columnas pathway_nombre,nombre_a,nombre_b a verificar")
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    log(f"=== Control FDR — {args.input.name} ===")
    df = cargar_datos(args.input, args.modo_p, None)
    n = len(df)
    log(f"Filas cargadas: {n:,}")

    # --- [1] Descriptivos ---
    n_cero, pct_cero = descriptivos_pvalores(df, args.outdir, log)

    # Ranking robusto a underflow (para la columna 'rango' de salida y para
    # el punto 8, posicion en el ranking)
    log10p = logsf_robusto(df)
    df["_log10p"] = log10p
    # ascending=False: el p-valor MAS PEQUEÑO (mayor -log10(p), mas significativo)
    # obtiene el rango 1, igual que el indice i=1..m de la formula BH clasica.
    df["rango"] = df["_log10p"].rank(method="min", ascending=False).astype(int)

    pvals = df["p_valor"].to_numpy()

    # --- [2] p<0.05 sin corregir vs. esperado por azar ---
    n_sig_sin_corregir = int((pvals < 0.05).sum())
    esperado_azar = 0.05 * n
    log(f"\n[2] p<0,05 sin corregir: {n_sig_sin_corregir:,} de {n:,}")
    log(f"    Esperado por azar (0,05×N, N=universo de ENTRADA ya filtrado a soporte>=50): {esperado_azar:,.0f}")
    if n_sig_sin_corregir < esperado_azar:
        log("    ADVERTENCIA: el número de pares p<0,05 sin corregir es MENOR que el "
            "esperado por azar — señal de posible error en el cálculo de p-valores, revisar.")

    # --- [3] BH q=0.05, 0.01, 0.001 ---
    log("\n[3] Benjamini-Hochberg (1995)")
    resultados_bh = {}
    for q in (0.05, 0.01, 0.001):
        rechaza, p_ajustado, _, _ = multipletests(pvals, alpha=q, method="fdr_bh")
        rechaza_manual = verificar_bh_manual(pvals, q)
        discrepancias = int((rechaza != rechaza_manual).sum())
        if discrepancias > 0:
            log(f"    ADVERTENCIA: statsmodels y la implementación propia de BH difieren en "
                f"{discrepancias} filas para q={q} — revisar antes de reportar.")
        n_sobrevive = int(rechaza.sum())
        p_umbral = float(pvals[rechaza].max()) if n_sobrevive > 0 else float("nan")
        resultados_bh[q] = (rechaza, p_ajustado)
        df[f"q_valor_BH"] = p_ajustado if q == 0.05 else df.get("q_valor_BH", p_ajustado)
        df[f"sobrevive_BH_q{str(q).replace('0.', '')}"] = rechaza
        log(f"    q={q}: {n_sobrevive:,} sobreviven, p-valor umbral ≈ {p_umbral:.4g} "
            f"(verificado contra fórmula manual: {'OK' if discrepancias == 0 else 'DISCREPA'})")

    # --- [4] BY ---
    log("\n[4] Benjamini-Yekutieli (no asume independencia)")
    rechaza_by, p_adj_by, _, _ = multipletests(pvals, alpha=0.05, method="fdr_by")
    n_sobrevive_by = int(rechaza_by.sum())
    df["q_valor_BY"] = p_adj_by
    df["sobrevive_BY_q05"] = rechaza_by
    log(f"    q=0,05: {n_sobrevive_by:,} sobreviven "
        f"({100 * n_sobrevive_by / n:.2f}% del universo; comparar con BH q=0,05 arriba — "
        f"BY es más conservador por diseño)")

    # --- [5] Bonferroni ---
    log("\n[5] Bonferroni (referencia extrema)")
    rechaza_bonf, p_adj_bonf, _, _ = multipletests(pvals, alpha=0.05, method="bonferroni")
    umbral_bonf = 0.05 / n
    n_sobrevive_bonf = int(rechaza_bonf.sum())
    df["sobrevive_Bonferroni"] = rechaza_bonf
    log(f"    Umbral: {umbral_bonf:.4g}  →  {n_sobrevive_bonf:,} sobreviven")

    # --- [6] Storey ---
    log("\n[6] Estimación de pi_0 (Storey & Tibshirani, 2003)")
    pi0 = storey_pi0(pvals)
    q_storey = storey_qvalues(pvals, pi0)
    df["q_valor_Storey"] = q_storey
    n_storey_05 = int((q_storey <= 0.05).sum())
    df["sobrevive_Storey_q05"] = q_storey <= 0.05
    log(f"    pi_0 estimado: {pi0:.4f}  (proporción estimada de hipótesis nulas verdaderas)")
    log(f"    Sobrevivientes q_Storey<=0,05: {n_storey_05:,}  "
        f"(comparar con BH q=0,05: {int(resultados_bh[0.05][0].sum()):,})")

    # --- [7] Intersección con los otros criterios ---
    log("\n[7] Intersección con los cuatro criterios computables")
    tiene_cols = {"lift", "direccion_pct", "estabilidad_completa"}.issubset(df.columns)
    if not tiene_cols:
        log("    ADVERTENCIA: faltan columnas lift/direccion_pct/estabilidad_completa — se omite este bloque.")
        tabla_interseccion = pd.DataFrame()
    else:
        misma_inst = df["misma_institucion"] if "misma_institucion" in df.columns else pd.Series(False, index=df.index)
        # Exclusion de los 17 pares ya evaluados a mano (Seccion 7.1) -- misma
        # logica que 9_extender_candidatos_pathway.py, para que el conteo calce
        # con los 5.113 publicados en vez de producir un tercer numero.
        if {"nombre_a", "nombre_b"}.issubset(df.columns):
            mask_ya_evaluado = df.apply(
                lambda r: ya_evaluado(r["nombre_a"], r["nombre_b"]), axis=1)
            log(f"    excluidos por estar entre los 17 pares ya evaluados a mano "
                f"(Sección 7.1): {int(mask_ya_evaluado.sum()):,} filas del universo")
        else:
            mask_ya_evaluado = pd.Series(False, index=df.index)
            log("    ADVERTENCIA: faltan columnas nombre_a/nombre_b — no se pudo aplicar "
                "la exclusión de los 17 pares ya evaluados; el conteo NO calzará con los 5.113 publicados.")
        base = (
            (df["soporte"] >= 50)
            & (df["lift"] >= 1.2)
            & (df["direccion_pct"] >= 70)
            & (df["estabilidad_completa"] == True)  # noqa: E712
            & (misma_inst != True)  # noqa: E712  -- excluye misma_institucion=True; deja pasar None/False
            & (~mask_ya_evaluado)
        )
        combinaciones = {
            "4_criterios_sin_correccion": base,
            "4_criterios_+_BH_q05": base & df["sobrevive_BH_q05"],
            "4_criterios_+_BH_q01": base & df["sobrevive_BH_q01"],
            "4_criterios_+_BY_q05": base & df["sobrevive_BY_q05"],
        }
        n_base = int(base.sum())
        filas = []
        for nombre, mask in combinaciones.items():
            cnt = int(mask.sum())
            caida_pct = 100 * (1 - cnt / n_base) if n_base > 0 else float("nan")
            filas.append({"combinacion": nombre, "n_pares": cnt, "caida_pct_vs_sin_correccion": round(caida_pct, 2)})
            log(f"    {nombre}: {cnt:,} ({-caida_pct:+.2f}% vs. sin corrección)")
        tabla_interseccion = pd.DataFrame(filas)

    # --- [8] Verificación de pathways específicos ---
    tabla_pathways = pd.DataFrame()
    if args.pathways and args.pathways.exists():
        log(f"\n[8] Verificación de pathways publicados ({args.pathways.name})")
        pw = pd.read_csv(args.pathways)

        def norm(s):
            return str(s).strip().lower()

        df["_na_norm"] = df["nombre_a"].map(norm)
        df["_nb_norm"] = df["nombre_b"].map(norm)

        filas = []
        for _, row in pw.iterrows():
            na, nb = norm(row["nombre_a"]), norm(row["nombre_b"])
            match = df[(df["_na_norm"] == na) & (df["_nb_norm"] == nb)]
            if match.empty:
                match = df[(df["_na_norm"] == nb) & (df["_nb_norm"] == na)]
            if match.empty:
                log(f"    NO ENCONTRADO: {row['pathway_nombre']} ({row['nombre_a']} / {row['nombre_b']})")
                filas.append({"pathway_nombre": row["pathway_nombre"], "encontrado": False})
                continue
            r = match.iloc[0]
            fila = {
                "pathway_nombre": row["pathway_nombre"],
                "encontrado": True,
                "p_valor": r["p_valor"],
                "rango": int(r["rango"]),
                "sobrevive_BH_q05": bool(r["sobrevive_BH_q05"]),
                "sobrevive_BH_q01": bool(r["sobrevive_BH_q01"]),
                "sobrevive_BY_q05": bool(r["sobrevive_BY_q05"]),
                "sobrevive_Bonferroni": bool(r["sobrevive_Bonferroni"]),
            }
            filas.append(fila)
            estado = "OK" if fila["sobrevive_BH_q05"] else "NO SOBREVIVE BH q=0,05"
            log(f"    {row['pathway_nombre']}: p={r['p_valor']:.4g}, rango {int(r['rango']):,}/{n:,}, {estado}")
            if not fila["sobrevive_BH_q05"]:
                log(f"    ADVERTENCIA: {row['pathway_nombre']} no sobrevive a BH q=0,05.")
        tabla_pathways = pd.DataFrame(filas)
        df.drop(columns=["_na_norm", "_nb_norm"], inplace=True)
    else:
        log("\n[8] Sin lista de pathways (--pathways no provisto) — se omite.")

    # --- [9] Sensibilidad al umbral de soporte ---
    log("\n[9] Sensibilidad de BH q=0,05 al umbral de soporte")
    filas_sens = []
    soporte_min_disponible = int(df["soporte"].min())
    for umb in UMBRALES_SOPORTE_SENSIBILIDAD:
        if umb < soporte_min_disponible:
            log(f"    soporte>={umb}: NO DISPONIBLE — el archivo de entrada ya viene "
                f"filtrado a soporte>={soporte_min_disponible} en el origen (AWS/DuckDB, "
                f"HAVING COUNT(*)>={soporte_min_disponible}); no existen filas por debajo de ese piso "
                f"en ningún CSV local.")
            filas_sens.append({"umbral_soporte": umb, "n_tests": None, "n_sobrevive_BH_q05": None, "nota": "no disponible en datos locales"})
            continue
        sub = df[df["soporte"] >= umb]
        if len(sub) == 0:
            filas_sens.append({"umbral_soporte": umb, "n_tests": 0, "n_sobrevive_BH_q05": 0, "nota": ""})
            continue
        rechaza_sub, _, _, _ = multipletests(sub["p_valor"], alpha=0.05, method="fdr_bh")
        n_sob = int(rechaza_sub.sum())
        filas_sens.append({"umbral_soporte": umb, "n_tests": len(sub), "n_sobrevive_BH_q05": n_sob, "nota": ""})
        log(f"    soporte>={umb}: {len(sub):,} tests, {n_sob:,} sobreviven BH q=0,05")
    tabla_sensibilidad = pd.DataFrame(filas_sens)

    # --- Salidas ---
    df.drop(columns=["_log10p"], inplace=True)
    df.to_csv(args.outdir / "universo_con_qvalores.csv", index=False)
    if not tabla_interseccion.empty:
        tabla_interseccion.to_csv(args.outdir / "tabla_interseccion_criterios.csv", index=False)
    if not tabla_pathways.empty:
        tabla_pathways.to_csv(args.outdir / "verificacion_pathways.csv", index=False)
    tabla_sensibilidad.to_csv(args.outdir / "sensibilidad_umbral_soporte.csv", index=False)

    # Figura BH clásica: p-valor ordenado vs línea crítica
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    p_ord = np.sort(pvals)
    i = np.arange(1, n + 1)
    linea_critica_005 = (i / n) * 0.05
    fig, ax = plt.subplots(figsize=(7, 5))
    zoom = min(n, max(200, int(2 * (df["sobrevive_BH_q05"]).sum())))
    ax.plot(i[:zoom], p_ord[:zoom], ".", ms=3, label="p-valor ordenado", color="#4C72B0")
    ax.plot(i[:zoom], linea_critica_005[:zoom], "-", color="#C44E52", label="línea crítica BH q=0,05")
    ax.set_xlabel("rango (i)")
    ax.set_ylabel("p-valor")
    ax.set_title("Gráfico de Benjamini-Hochberg (zoom a la región relevante)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.outdir / "fig_bh_grafico.png", dpi=150)
    plt.close(fig)

    with open(args.outdir / "resumen.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))

    log(f"\nSalidas escritas en {args.outdir}/")


if __name__ == "__main__":
    main()
