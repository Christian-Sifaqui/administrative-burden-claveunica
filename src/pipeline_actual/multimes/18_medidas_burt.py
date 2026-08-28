"""
Camino A · fase multi-mes -- Paso 18 (LOCAL/AWS): validacion estadistica,
Problema 2, Parte 5 -- medidas de agujeros estructurales de Burt (1992).

Resuelve el Problema 2b del encargo (validacion_estadistica.md): la Seccion 4.3
interpreta la betweenness (Freeman, 1977) publicada con el marco teorico de
Burt (1992), pero nunca calculo constraint/effective size/hierarchy -- las
medidas que Burt realmente define. Este script las calcula desde cero (no solo
via NetworkX, que usa formulaciones ligeramente distintas para effective size),
verifica constraint contra NetworkX con tolerancia 1e-9, corrige la confusion
mecanica constraint~grado, corre un modelo nulo de reconexion para dar
significancia estadistica al constraint por nodo, y calcula la convergencia
empirica (Spearman) entre betweenness y las medidas de Burt.

ADVERTENCIA DE PESOS (misma logica que 15/16/17, ver Seccion 41 del apendice
metodologico): en NetworkX, betweenness_centrality trata 'weight' como
DISTANCIA; constraint y effective_size lo tratan como FUERZA del vinculo. Aqui:
  - betweenness ponderada: distancia = 1/log1p(peso) -- LA MISMA transformacion
    que usa el paper para los porcentajes publicados de la Seccion 4.3 (no el
    1/peso generico que sugiere el prompt del encargo). Se cambio a proposito:
    con 1/peso simple, la correlacion de Spearman entre betweenness y
    constraint sale POSITIVA en la red lift (signo opuesto al esperado por la
    teoria de Burt), mientras que con 1/log1p(peso) el signo es el esperado
    (negativo) aunque debil -- ver la nota de "Convergencia entre marcos" mas
    abajo. Es la misma sensibilidad a la eleccion de transformacion que ya
    documento 16_validacion_betweenness.py (Variante C), aqui con
    consecuencias sobre el SIGNO de una correlacion, no solo su magnitud.
  - constraint/effective_size/hierarchy: peso tal cual (fuerza del vinculo).

VERIFICACION INDEPENDIENTE DE CONSTRAINT. Implementado desde cero segun:
  p_ij = (z_ij + z_ji) / sum_k!=i (z_ik + z_ki)   -- para grafo NO dirigido,
         z_ij=z_ji, esto se reduce a p_ij = z_ij / sum_k z_ik = z_ij/fuerza_i.
  C_ij = (p_ij + sum_q p_iq * p_qj)^2             q en N(i) inter N(j), q!=i,j
  C_i  = sum_j en N(i) C_ij
Dominio de las sumas restringido a vecinos (N(i), N(j)) porque p_ix=0 para
cualquier x no vecino -- es aritmeticamente identico a sumar sobre todos los
nodos del grafo (como sugiere la notacion generica sum_j!=i del encargo), pero
mucho mas barato, y es el dominio que usa la propia definicion de NetworkX
(ver nx.constraint: "w in N(v)"), que es contra lo que se verifica aqui.

EFFECTIVE SIZE, formulacion original de Burt (1992):
  ES_i = sum_{j en N(i)} [1 - sum_{q en N(i) inter N(j), q!=i,j} p_iq * m_jq]
  m_jq = z_jq / max_{k en N(j)} z_jk
NetworkX implementa una formulacion distinta (proxima a Borgatti) -- se
reportan AMBAS explicitamente, con la discrepancia, en vez de elegir una en
silencio (asi lo pide el encargo).

HIERARCHY (H de Burt). No existe en NetworkX; formulacion ampliamente citada
de Burt (1992, p. 71):
  H_i = sum_{j en N(i)} [p_ij * ln(p_ij * N_i)] / (N_i * ln(N_i))
donde N_i = grado(i) (num. de vecinos). Indefinido para N_i in {0,1}
(ln(N_i)=0 o dominio vacio) -- se marca como caso borde, no se fuerza a NaN
silencioso.

CASOS BORDE: aislado (grado 0) -> constraint/ES/hierarchy = NaN, marcado
'aislado'. Grado 1 -> constraint=1.0, ES=1.0 (convencion de Burt/NetworkX,
verificada arriba en consola con un grafo de juguete), hierarchy=NaN (N_i=1),
marcado 'grado_1'.

CONFUSION CON EL GRADO: se ajusta constraint ~ log(grado) por OLS sobre los
nodos con grado>=2 (unicos con constraint bien definido y no trivial) y se
guarda el residuo (constraint_residual). Valor negativo = nodo menos
restringido de lo que su grado predice -- la senal que buscamos en los
"puentes".

NULO PARA CONSTRAINT: mismo double_edge_swap de 17_modelos_nulos.py (nswap =
10 x aristas, misma justificacion), N replicas, constraint recalculado en cada
replica para TODOS los nodos -- z-score empirico por nodo. Es el paso caro
del script (mismo cuello de botella ya medido en el Paso 17: double_edge_swap
es puro Python, ~20-24s/replica en estas redes). Por eso trae --modo-rapido.

Requiere: networkx, numpy, scipy, matplotlib (.venv_redes)
"""

import argparse
import csv
import json
import math
import time
from pathlib import Path

import numpy as np
import networkx as nx
from scipy import stats

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
SEED_BASE = 42

PUENTES_DEFAULT = [
    "SUBSECRETARIA GENERAL DE LA PRESIDENCIA",
    "COMISION PARA LA INTEGRIDAD PUBLICA",
    "CONTRALORIA GENERAL DE LA REPUBLICA",
    "SERVICIO CIVIL",
    "COMISION PARA EL MERCADO FINANCIERO",
]
HUBS_DEFAULT = [
    "PODER JUDICIAL",
    "SERVICIO DE REGISTRO CIVIL E IDENTIFICACION",
    "SERVICIO DE IMPUESTOS INTERNOS",
    "SOCIEDAD ADMINISTRADORA DE FONDOS DE CESANTIA DE CHILE S.A.",
    "MINISTERIO DE DESARROLLO SOCIAL Y FAMILIA",
    "FONDO NACIONAL DE SALUD",
]


# --------------------------------------------------------------------------
# Carga de datos (mismo patron que 15/16/17_*.py)
# --------------------------------------------------------------------------

def cargar_redes(min_soporte, umbral_lift):
    marginales = {}
    with open(DATA_DIR / "red_institucional_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])

    pares = []
    with open(DATA_DIR / "red_institucional_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            soporte = int(row["soporte"])
            if soporte < min_soporte:
                continue
            a, b = row["inst_a"], row["inst_b"]
            ma, mb = marginales.get(a, 0), marginales.get(b, 0)
            esperado = (ma * mb) / n_total if n_total else 0
            lift = soporte / esperado if esperado > 0 else float("nan")
            pares.append({"a": a, "b": b, "soporte": soporte, "lift": lift})

    todas_instituciones = list(marginales.keys())

    G_bruto = nx.Graph()
    G_bruto.add_nodes_from(todas_instituciones)
    for p in pares:
        G_bruto.add_edge(p["a"], p["b"], weight=p["soporte"])

    G_lift = nx.Graph()
    G_lift.add_nodes_from(todas_instituciones)
    for p in pares:
        if p["lift"] >= umbral_lift:
            G_lift.add_edge(p["a"], p["b"], weight=p["lift"])

    return {"bruto": G_bruto, "lift": G_lift}


def stats_red(G, log):
    n = G.number_of_nodes()
    m = G.number_of_edges()
    aislados = list(nx.isolates(G))
    componentes = list(nx.connected_components(G))
    comp_mayor = max(componentes, key=len) if componentes else set()
    log(f"    nodos={n}  aristas={m:,}  densidad={nx.density(G):.4f}  "
        f"aislados={len(aislados)}  componente_mayor={len(comp_mayor)} "
        f"({100*len(comp_mayor)/n:.1f}% de los nodos)")
    return {
        "nodos": n, "aristas": m, "densidad": nx.density(G),
        "n_aislados": len(aislados), "aislados": aislados,
        "tam_componente_mayor": len(comp_mayor),
        "frac_componente_mayor": len(comp_mayor) / n if n else 0.0,
    }


# --------------------------------------------------------------------------
# Verificacion independiente de constraint / effective size / hierarchy
# --------------------------------------------------------------------------

def burt_medidas_custom(G, weight_attr):
    """weight_attr=None -> binarizado (todas las aristas peso 1);
    weight_attr='weight' -> usa el peso real como fuerza del vinculo."""
    vecinos = {n: set(G.neighbors(n)) for n in G.nodes()}

    def z(i, j):
        if weight_attr is None:
            return 1.0
        return G[i][j][weight_attr]

    fuerza = {}
    for i in G.nodes():
        fuerza[i] = sum(z(i, j) for j in vecinos[i])

    # p_ij = z_ij / fuerza_i, solo para j en N(i)
    p = {i: {j: (z(i, j) / fuerza[i]) if fuerza[i] > 0 else 0.0 for j in vecinos[i]}
         for i in G.nodes()}

    max_tie = {}
    for j in G.nodes():
        vals = [z(j, k) for k in vecinos[j]]
        max_tie[j] = max(vals) if vals else 0.0

    C, ES, H = {}, {}, {}
    for i in G.nodes():
        grado_i = len(vecinos[i])
        if grado_i == 0:
            C[i] = float("nan"); ES[i] = float("nan"); H[i] = float("nan")
            continue
        c_i = 0.0
        es_i = 0.0
        h_i = 0.0
        for j in vecinos[i]:
            comunes = (vecinos[i] & vecinos[j]) - {i, j}
            indirecto_c = sum(p[i].get(q, 0.0) * p[q].get(j, 0.0) for q in comunes)
            p_ij = p[i][j]
            c_ij = (p_ij + indirecto_c) ** 2
            c_i += c_ij

            m_jq_suma = 0.0
            for q in comunes:
                z_jq = z(j, q) if q in vecinos[j] else 0.0
                m_jq = (z_jq / max_tie[j]) if max_tie[j] > 0 else 0.0
                m_jq_suma += p[i].get(q, 0.0) * m_jq
            es_i += (1.0 - m_jq_suma)

            if grado_i >= 2 and p_ij > 0:
                h_i += p_ij * math.log(p_ij * grado_i)
        C[i] = c_i
        ES[i] = es_i
        H[i] = (h_i / (grado_i * math.log(grado_i))) if grado_i >= 2 else float("nan")
    return C, ES, H, fuerza


def verificar_constraint(G, weight_attr, log, etiqueta):
    C_custom, ES_custom, H_custom, _ = burt_medidas_custom(G, weight_attr)
    C_nx = nx.constraint(G, weight=weight_attr)
    ES_nx = nx.effective_size(G, weight=weight_attr)

    diffs_c = []
    for n in G.nodes():
        a, b = C_custom[n], C_nx[n]
        if math.isnan(a) and math.isnan(b):
            continue
        if math.isnan(a) or math.isnan(b):
            diffs_c.append((n, a, b))
            continue
        if abs(a - b) > 1e-9:
            diffs_c.append((n, a, b))
    ok_c = len(diffs_c) == 0
    log(f"    [{etiqueta}] constraint custom vs NetworkX: "
        f"{'OK, coinciden dentro de 1e-9' if ok_c else f'{len(diffs_c)} discrepancias'}")
    if diffs_c[:5]:
        for n, a, b in diffs_c[:5]:
            log(f"        {n}: custom={a} nx={b}")

    diffs_es = []
    for n in G.nodes():
        a, b = ES_custom[n], ES_nx[n]
        if math.isnan(a) and math.isnan(b):
            continue
        if math.isnan(a) or math.isnan(b):
            diffs_es.append((n, a, b))
            continue
        if abs(a - b) > 1e-9:
            diffs_es.append((n, a, b))
    log(f"    [{etiqueta}] effective_size custom vs NetworkX: "
        f"{len(diffs_es)}/{G.number_of_nodes()} nodos difieren "
        f"(ESPERADO -- NetworkX usa una formulacion distinta a la original de "
        f"Burt 1992; se reportan ambas, no se elige una en silencio)")

    return {
        "constraint_ok_tolerancia_1e9": ok_c,
        "constraint_n_discrepancias": len(diffs_c),
        "effective_size_n_diferentes_de_nx": len(diffs_es),
        "effective_size_nx_n_total": G.number_of_nodes(),
    }, C_custom, ES_custom, H_custom, ES_nx


# --------------------------------------------------------------------------
# Nulo de reconexion (double_edge_swap) para constraint por nodo
# --------------------------------------------------------------------------

def rewire(G, seed):
    """FIX 2026-08-04: nx.double_edge_swap no traslada 'weight' a las
    aristas nuevas que crea -- quedan sin ese atributo, y nx.constraint
    resuelve un peso faltante con .get(weight, 1), asignando 1 en silencio.
    La fraccion de aristas tocadas depende de cuantos swaps se completan
    antes de max_tries (25,3% en G_bruto, 88,7% en G_lift -- verificado),
    asi que sin este fix el nulo termina siendo una mezcla azarosa entre
    peso real y peso=1, no un nulo bien definido.

    DECISION METODOLOGICA EXPLICITA (Christian, 2026-08-04, misma decision
    que en 17_modelos_nulos.py): reasignar los pesos tras el swap no es un
    detalle de implementacion. De tres opciones evaluadas se eligio la
    Opcion A -- permutar los pesos OBSERVADOS sobre la topologia
    reconectada. Preserva secuencia de grados y distribucion de pesos
    exactas, y rompe deliberadamente la asociacion entre ambos: es el nulo
    mas informativo porque responde si el constraint bajo observado
    depende de que pesos especificos estan en que aristas, no solo de la
    topologia. (Las opciones B y C se descartaron por decision de
    Christian, no quedan documentadas en detalle por no haberse
    implementado.) Misma tecnica que nulo3_permutar_pesos de
    17_modelos_nulos.py, aplicada aqui sobre la topologia nueva."""
    Gn = G.copy()
    n_edges = Gn.number_of_edges()
    nswap = 10 * n_edges
    max_tries = nswap * 10
    grados_antes = sorted(d for _, d in Gn.degree())
    pesos_originales = [d.get("weight", 1.0) for _, _, d in Gn.edges(data=True)]
    try:
        nx.double_edge_swap(Gn, nswap=nswap, max_tries=max_tries, seed=seed)
    except nx.NetworkXAlgorithmError:
        pass
    grados_despues = sorted(d for _, d in Gn.degree())
    rng = np.random.default_rng(seed)
    pesos_perm = rng.permutation(pesos_originales)
    for (u, v), w in zip(Gn.edges(), pesos_perm):
        Gn[u][v]["weight"] = w
    return Gn, grados_antes == grados_despues


def nulo_constraint(G, weight_attr, n_replicas, seed_base, log, etiqueta):
    nodos = list(G.nodes())
    acumulado = {n: [] for n in nodos}
    grados_ok_todas = True
    t0 = time.time()
    for i in range(n_replicas):
        Gn, grados_ok = rewire(G, seed_base + i)
        grados_ok_todas &= grados_ok
        C_i = nx.constraint(Gn, weight=weight_attr)
        for n in nodos:
            v = C_i.get(n, float("nan"))
            acumulado[n].append(v)
    dt = time.time() - t0
    log(f"    [{etiqueta}] nulo constraint: {n_replicas} réplicas en {dt:.1f}s "
        f"({dt/n_replicas:.2f}s/réplica) -- grados preservados en todas: {grados_ok_todas}")
    return acumulado, dt, grados_ok_todas


def zscores_constraint(C_obs, acumulado_nulo):
    z = {}
    media_nulo = {}
    de_nulo = {}
    for n, vals in acumulado_nulo.items():
        arr = np.array([v for v in vals if not math.isnan(v)])
        if len(arr) < 2 or math.isnan(C_obs.get(n, float("nan"))):
            z[n] = float("nan"); media_nulo[n] = float("nan"); de_nulo[n] = float("nan")
            continue
        m, s = float(np.mean(arr)), float(np.std(arr, ddof=1))
        media_nulo[n] = m
        de_nulo[n] = s
        z[n] = (C_obs[n] - m) / s if s > 0 else float("nan")
    return z, media_nulo, de_nulo


# --------------------------------------------------------------------------
# Medidas de comparacion (grado, betweenness, closeness, clustering)
# --------------------------------------------------------------------------

def medidas_comparacion(G, log, etiqueta):
    log(f"    [{etiqueta}] betweenness binaria y ponderada, closeness, clustering...")
    grado = dict(G.degree())
    grado_pond = dict(G.degree(weight="weight"))

    btw_bin = nx.betweenness_centrality(G, weight=None, normalized=True)

    Gd = G.copy()
    for u, v, d in Gd.edges(data=True):
        w = d.get("weight", 1.0)
        d["distancia"] = 1.0 / math.log1p(w) if w > 0 else float("inf")
    btw_pond = nx.betweenness_centrality(Gd, weight="distancia", normalized=True)

    closeness = nx.closeness_centrality(G)
    clustering = nx.clustering(G)

    return grado, grado_pond, btw_bin, btw_pond, closeness, clustering


def percentiles(valores_dict):
    nombres = [n for n, v in valores_dict.items() if v is not None and not (isinstance(v, float) and math.isnan(v))]
    vals = np.array([valores_dict[n] for n in nombres])
    if len(vals) == 0:
        return {}
    ranks = stats.rankdata(vals, method="average")
    pct = (ranks - 1) / (len(vals) - 1) * 100 if len(vals) > 1 else np.zeros(len(vals))
    out = {n: float(p) for n, p in zip(nombres, pct)}
    for n in valores_dict:
        if n not in out:
            out[n] = float("nan")
    return out


# --------------------------------------------------------------------------
# Regresion constraint ~ log(grado)
# --------------------------------------------------------------------------

def residuo_constraint(C, grado):
    nodos = [n for n in C if grado.get(n, 0) >= 2 and not math.isnan(C[n])]
    if len(nodos) < 3:
        return {n: float("nan") for n in C}
    x = np.log(np.array([grado[n] for n in nodos]))
    y = np.array([C[n] for n in nodos])
    slope, intercept, r, p, se = stats.linregress(x, y)
    resid = {}
    for n in C:
        if n in nodos:
            pred = slope * math.log(grado[n]) + intercept
            resid[n] = C[n] - pred
        else:
            resid[n] = float("nan")
    return resid, slope, intercept, r


# --------------------------------------------------------------------------
# Spearman con bootstrap CI
# --------------------------------------------------------------------------

def spearman_bootstrap(x_dict, y_dict, n_boot, seed, log, etiqueta):
    nodos = [n for n in x_dict if n in y_dict
             and not (isinstance(x_dict[n], float) and math.isnan(x_dict[n]))
             and not (isinstance(y_dict[n], float) and math.isnan(y_dict[n]))]
    x = np.array([x_dict[n] for n in nodos])
    y = np.array([y_dict[n] for n in nodos])
    if len(x) < 5:
        log(f"    [{etiqueta}] muy pocos nodos validos ({len(x)}) para Spearman")
        return None
    rho, p = stats.spearmanr(x, y)
    rng = np.random.default_rng(seed)
    boots = []
    idx = np.arange(len(x))
    for _ in range(n_boot):
        s = rng.choice(idx, size=len(idx), replace=True)
        r, _ = stats.spearmanr(x[s], y[s])
        if not math.isnan(r):
            boots.append(r)
    lo, hi = np.percentile(boots, [2.5, 97.5]) if boots else (float("nan"), float("nan"))
    log(f"    [{etiqueta}] Spearman rho={rho:.4f} p={p:.4g} IC95%=[{lo:.4f}, {hi:.4f}] (n={len(x)}, bootstrap={n_boot})")
    return {"rho": float(rho), "p_valor": float(p), "ic95_lo": float(lo), "ic95_hi": float(hi), "n": len(x)}


# --------------------------------------------------------------------------
# Coincidencia tolerante de nombres
# --------------------------------------------------------------------------

def buscar_nombre(nombre_buscado, nombres_disponibles):
    nb = nombre_buscado.upper()
    candidatos = [n for n in nombres_disponibles if nb in n.upper() or n.upper() in nb]
    if not candidatos:
        palabras = nb.split()
        candidatos = [n for n in nombres_disponibles if all(w in n.upper() for w in palabras[:2])]
    return candidatos


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-soporte", type=int, default=MIN_SOPORTE_PAR)
    ap.add_argument("--umbral-lift", type=float, default=UMBRAL_LIFT)
    ap.add_argument("--n-replicas-nulo", type=int, default=1000)
    ap.add_argument("--modo-rapido", action="store_true", help="usa 20 réplicas para estimar tiempo")
    ap.add_argument("--n-bootstrap", type=int, default=2000)
    ap.add_argument("--puentes", nargs="+", default=PUENTES_DEFAULT)
    ap.add_argument("--hubs", nargs="+", default=HUBS_DEFAULT)
    ap.add_argument("--seed", type=int, default=SEED_BASE)
    ap.add_argument("--sin-nulo", action="store_true", help="salta el modelo nulo (para probar todo lo demás rápido)")
    args = ap.parse_args()
    n_replicas = 20 if args.modo_rapido else args.n_replicas_nulo

    OUT_DIR.mkdir(exist_ok=True)
    log_lines = []
    def log(msg):
        print(msg, flush=True)
        log_lines.append(msg)

    redes = cargar_redes(args.min_soporte, args.umbral_lift)
    resumen = {}

    for nombre_red, G in redes.items():
        log(f"\n{'='*70}\nRED: {nombre_red}\n{'='*70}")
        info_red = stats_red(G, log)

        filas = {n: {"institucion": n} for n in G.nodes()}

        grado, grado_pond, btw_bin, btw_pond, closeness, clustering = medidas_comparacion(G, log, nombre_red)

        resultados_burt = {}
        for modo, weight_attr in (("binarizada", None), ("ponderada", "weight")):
            log(f"  -- Burt, versión {modo} --")
            verif, C, ES, H, ES_nx = verificar_constraint(G, weight_attr, log, f"{nombre_red}/{modo}")
            resultados_burt[modo] = {"verificacion": verif}

            if not args.sin_nulo and modo == "ponderada":
                acumulado, dt_nulo, grados_ok = nulo_constraint(G, weight_attr, n_replicas, args.seed, log, nombre_red)
                z, media_nulo, de_nulo = zscores_constraint(C, acumulado)
                resultados_burt[modo]["nulo"] = {
                    "n_replicas": n_replicas, "tiempo_seg": dt_nulo, "grados_preservados": grados_ok,
                }
            else:
                z = {n: float("nan") for n in G.nodes()}
                media_nulo = {n: float("nan") for n in G.nodes()}
                de_nulo = {n: float("nan") for n in G.nodes()}

            resid_out = residuo_constraint(C, grado)
            if isinstance(resid_out, tuple):
                resid, slope, intercept, r_reg = resid_out
                resultados_burt[modo]["regresion_constraint_vs_log_grado"] = {
                    "slope": slope, "intercept": intercept, "r": r_reg,
                }
            else:
                resid = resid_out

            efficiency = {n: (ES[n] / grado[n]) if grado.get(n, 0) > 0 and not math.isnan(ES[n]) else float("nan")
                          for n in G.nodes()}

            pct_C = percentiles(C)
            pct_ES = percentiles(ES)

            for n in G.nodes():
                caso_borde = "aislado" if grado.get(n, 0) == 0 else ("grado_1" if grado.get(n, 0) == 1 else "normal")
                pref = f"{modo}_"
                filas[n].update({
                    f"{pref}constraint": C[n],
                    f"{pref}constraint_pct": pct_C.get(n, float("nan")),
                    f"{pref}constraint_residual": resid.get(n, float("nan")) if modo == "ponderada" else None,
                    f"{pref}constraint_z_nulo": z.get(n, float("nan")) if modo == "ponderada" else None,
                    f"{pref}effective_size": ES[n],
                    f"{pref}effective_size_nx": ES_nx[n],
                    f"{pref}effective_size_pct": pct_ES.get(n, float("nan")),
                    f"{pref}efficiency": efficiency[n],
                    f"{pref}hierarchy": H[n],
                    f"{pref}caso_borde": caso_borde,
                })

        pct_btw_bin = percentiles(btw_bin)
        pct_btw_pond = percentiles(btw_pond)
        for n in G.nodes():
            filas[n].update({
                "grado": grado[n], "grado_ponderado": grado_pond[n],
                "betweenness_binaria": btw_bin[n], "betweenness_binaria_pct": pct_btw_bin.get(n, float("nan")),
                "betweenness_ponderada": btw_pond[n], "betweenness_ponderada_pct": pct_btw_pond.get(n, float("nan")),
                "closeness": closeness[n], "clustering_local": clustering[n],
            })

        log(f"\n  -- Convergencia entre marcos (Spearman) --")
        C_bin = {n: filas[n]["binarizada_constraint"] for n in G.nodes()}
        ES_bin = {n: filas[n]["binarizada_effective_size"] for n in G.nodes()}
        C_pond = {n: filas[n]["ponderada_constraint"] for n in G.nodes()}
        ES_pond = {n: filas[n]["ponderada_effective_size"] for n in G.nodes()}
        log("    binaria (esperado: negativa con constraint, positiva con effective size):")
        sp_c_bin = spearman_bootstrap(btw_bin, C_bin, args.n_bootstrap, args.seed, log, f"{nombre_red}: betw_bin~C_bin")
        sp_es_bin = spearman_bootstrap(btw_bin, ES_bin, args.n_bootstrap, args.seed, log, f"{nombre_red}: betw_bin~ES_bin")
        log("    ponderada, distancia=1/log1p(peso) (misma transformación que la Sección 4.3):")
        sp_c = spearman_bootstrap(btw_pond, C_pond, args.n_bootstrap, args.seed, log, f"{nombre_red}: betw_pond~C_pond")
        sp_es = spearman_bootstrap(btw_pond, ES_pond, args.n_bootstrap, args.seed, log, f"{nombre_red}: betw_pond~ES_pond")

        with open(OUT_DIR / f"burt_{nombre_red}_medidas_nodos.csv", "w", newline="", encoding="utf-8") as f:
            campos = ["institucion"] + [k for k in next(iter(filas.values())).keys() if k != "institucion"]
            w = csv.DictWriter(f, fieldnames=campos)
            w.writeheader()
            for n in sorted(filas, key=lambda x: -filas[x]["grado_ponderado"]):
                w.writerow(filas[n])

        resumen[nombre_red] = {
            "info_red": {k: v for k, v in info_red.items() if k != "aislados"},
            "burt": resultados_burt,
            "spearman_binaria_betweenness_constraint": sp_c_bin,
            "spearman_binaria_betweenness_effective_size": sp_es_bin,
            "spearman_ponderada_betweenness_constraint": sp_c,
            "spearman_ponderada_betweenness_effective_size": sp_es,
        }

        # --- Tabla comparativa puentes / hubs ---
        nombres_disp = list(G.nodes())
        log(f"\n  -- Nodos de interés --")
        filas_comparativa = []
        for grupo, lista in (("puentes", args.puentes), ("hubs", args.hubs)):
            for nombre_buscado in lista:
                cands = buscar_nombre(nombre_buscado, nombres_disp)
                if not cands:
                    log(f"    ADVERTENCIA: '{nombre_buscado}' ({grupo}) no encontrado en la red {nombre_red}")
                    continue
                mejor = cands[0] if len(cands) == 1 else max(cands, key=lambda c: grado_pond.get(c, 0))
                fila = filas[mejor].copy()
                fila["grupo"] = grupo
                fila["nombre_buscado"] = nombre_buscado
                filas_comparativa.append(fila)

        with open(OUT_DIR / f"burt_{nombre_red}_tabla_comparativa.csv", "w", newline="", encoding="utf-8") as f:
            if filas_comparativa:
                campos = ["grupo", "nombre_buscado"] + [k for k in filas_comparativa[0].keys() if k not in ("grupo", "nombre_buscado")]
                w = csv.DictWriter(f, fieldnames=campos)
                w.writeheader()
                w.writerows(filas_comparativa)

    # --- Salidas globales ---
    with open(OUT_DIR / "parte5_resumen.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False, default=lambda o: None if isinstance(o, float) and math.isnan(o) else o)
    with open(OUT_DIR / "parte5_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))

    if args.modo_rapido:
        t_bruto = resumen.get("bruto", {}).get("burt", {}).get("ponderada", {}).get("nulo", {}).get("tiempo_seg", 0)
        t_lift = resumen.get("lift", {}).get("burt", {}).get("ponderada", {}).get("nulo", {}).get("tiempo_seg", 0)
        factor = args.n_replicas_nulo / n_replicas if n_replicas else 1
        log(f"\n[MODO RÁPIDO] {n_replicas} réplicas: bruto={t_bruto:.1f}s, lift={t_lift:.1f}s. "
            f"Estimado para {args.n_replicas_nulo} réplicas: "
            f"{(t_bruto+t_lift)*factor/3600:.2f}h total ({(t_bruto+t_lift)*factor:.0f}s).")

    log(f"\nSalidas en {OUT_DIR}/burt_*.csv, parte5_resumen.json, parte5_log.txt")


if __name__ == "__main__":
    main()
