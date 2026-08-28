"""
Camino A · fase multi-mes -- Paso 17 (LOCAL): validacion estadistica,
Problema 2, Parte 2 -- modelos nulos para modularidad.

Para CADA red (G_bruto, G_lift) y CADA uno de 3 nulos, genera N replicas,
corre Louvain sobre cada una (semilla derivada de la semilla base + indice
de replica, determinista), y guarda Q. Reporta media/DE/z-score/p-valor
empirico + asimetria/curtosis de la distribucion nula.

NULOS:
  1. double_edge_swap -- preserva secuencia de grados EXACTA. nswap = 10 x
     n_aristas (heuristica estandar para mezclar bien un grafo de este
     tamano: cada arista participa en ~10 intentos de swap en promedio).
     max_tries = nswap x 10 porque muchos intentos fallan (crearian
     self-loop o arista duplicada) y NetworkX los descarta sin contarlos
     como swap exitoso.
     CORRECCION 2026-08-04 (bug encontrado validando la Parte 5): NetworkX
     no traslada el atributo 'weight' a las aristas nuevas que crea al
     reconectar -- quedan sin ese atributo, y community_louvain resuelve
     un peso faltante con .get('weight', 1), es decir, les asigna 1 EN
     SILENCIO. Como la fraccion de aristas tocadas por al menos un swap
     depende de cuantos intentos logran completarse antes de max_tries,
     el efecto no es un binarizado limpio ni controlado: en la corrida
     original, 25,3% de las aristas de G_bruto y 88,7% de G_lift quedaron
     en peso=1 mientras el resto conservaba su peso real -- una mezcla
     azarosa, no un nulo bien definido. Los resultados de este nulo en
     output_multimes/validacion_red/parte2_modelos_nulos.json (corrida del
     2026-08-03/04 en AWS) estan afectados y deben recalcularse.
     DECISION METODOLOGICA EXPLICITA (Christian, 2026-08-04): reasignar los
     pesos tras el swap no es un detalle de implementacion, es una eleccion
     con consecuencias distintas segun la opcion. Se evaluaron tres:
       A. Permutar los pesos OBSERVADOS sobre la topologia reconectada
          (elegida). Preserva secuencia de grados y distribucion de pesos
          exactas, y rompe deliberadamente la asociacion entre ambos. Es el
          nulo mas informativo para el argumento del articulo, porque es el
          que responde si la estructura depende de que pesos estan donde --
          no solo de cuantos hay o de la topologia.
       B, C. Consideradas y descartadas (no quedan documentadas en detalle
          aqui porque no se implementaron; el criterio de descarte fue de
          Christian, no tecnico de este script).
     Con la Opcion A: tras reconectar la topologia (que si preserva grado
     exacto correctamente), se redistribuye el MULTISET de pesos original
     de la red sobre las aristas nuevas -- la misma tecnica del nulo 3,
     aplicada sobre la topologia ya reconectada en vez de la original.
  2. configuration_model -- mismo grado por nodo, pero sin preservar que
     casos self-loop/multi-arista se eliminen (se reporta cuanta densidad
     se pierde en esa limpieza). Ya era binario por diseno declarado (ver
     nulo2_configuration_model) -- el bug de arriba no lo afecta.
  3. permutacion de pesos -- MISMA topologia (aristas fijas), pesos
     barajados entre ellas. Aisla si la estructura viene de los pesos o de
     la topologia -- la pregunta sustantiva del articulo. No usa
     double_edge_swap -- el bug de arriba no lo afecta.

P-VALOR EMPIRICO: (1 + #replicas con Q_nulo >= Q_obs) / (1 + N) -- nunca da
cero, a diferencia de contar directamente.

Requiere: networkx, python-louvain, numpy, scipy, matplotlib (.venv_redes)
"""

import argparse
import csv
import json
import time
from pathlib import Path

import networkx as nx
import community as community_louvain
import numpy as np
from scipy import stats

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
SEED_BASE = 42


def cargar_redes():
    marginales = {}
    with open(DATA_DIR / "red_institucional_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])

    pares = []
    with open(DATA_DIR / "red_institucional_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            soporte = int(row["soporte"])
            if soporte < MIN_SOPORTE_PAR:
                continue
            a, b = row["inst_a"], row["inst_b"]
            ma, mb = marginales.get(a, 0), marginales.get(b, 0)
            esperado = (ma * mb) / n_total if n_total else 0
            lft = soporte / esperado if esperado > 0 else float("nan")
            pares.append({"a": a, "b": b, "soporte": soporte, "lift": lft})

    G_bruto = nx.Graph()
    for p in pares:
        G_bruto.add_edge(p["a"], p["b"], weight=p["soporte"])

    G_lift = nx.Graph()
    for p in pares:
        if p["lift"] >= UMBRAL_LIFT:
            G_lift.add_edge(p["a"], p["b"], weight=p["lift"])

    return {"bruto": G_bruto, "lift": G_lift}


def Q_de(G):
    part = community_louvain.best_partition(G, weight="weight", random_state=SEED_BASE)
    return community_louvain.modularity(part, G, weight="weight")


def nulo1_double_edge_swap(G, seed):
    Gn = G.copy()
    n_edges = Gn.number_of_edges()
    nswap = 10 * n_edges
    max_tries = nswap * 10
    grados_antes = sorted(d for _, d in Gn.degree())
    pesos_originales = [d.get("weight", 1.0) for _, _, d in Gn.edges(data=True)]
    try:
        nx.double_edge_swap(Gn, nswap=nswap, max_tries=max_tries, seed=seed)
    except nx.NetworkXAlgorithmError:
        pass  # llego a max_tries sin completar todos los swaps -- se usa lo logrado
    grados_despues = sorted(d for _, d in Gn.degree())
    grados_ok = grados_antes == grados_despues
    # FIX 2026-08-04: nx.double_edge_swap no traslada 'weight' a las aristas
    # nuevas (quedarian en peso=1 por el default silencioso de
    # community_louvain). Se redistribuye el multiset de pesos ORIGINAL
    # sobre la topologia ya reconectada -- preserva grado exacto (arriba) y
    # distribucion de pesos exacta (aqui), sin mezclas azarosas.
    rng = np.random.default_rng(seed)
    pesos_perm = rng.permutation(pesos_originales)
    for (u, v), w in zip(Gn.edges(), pesos_perm):
        Gn[u][v]["weight"] = w
    return Gn, grados_ok


def nulo2_configuration_model(G, seed):
    deg_seq = [d for _, d in G.degree()]
    Gm = nx.configuration_model(deg_seq, seed=seed)
    edges_multigraph = Gm.number_of_edges()
    Gs = nx.Graph(Gm)  # colapsa multi-aristas
    Gs.remove_edges_from(nx.selfloop_edges(Gs))
    edges_final = Gs.number_of_edges()
    perdida_pct = 100 * (1 - edges_final / edges_multigraph) if edges_multigraph else 0.0
    # Reasignar pesos: como configuration_model no preserva pesos originales
    # (son aristas nuevas), se asignan pesos = 1 (grafo binario) -- documentado
    # explicitamente, es la limitacion conocida de este nulo para redes ponderadas.
    for u, v in Gs.edges():
        Gs[u][v]["weight"] = 1
    return Gs, perdida_pct


def nulo3_permutar_pesos(G, seed):
    rng = np.random.default_rng(seed)
    Gn = G.copy()
    pesos = [d["weight"] for _, _, d in Gn.edges(data=True)]
    pesos_perm = rng.permutation(pesos)
    for (u, v), w in zip(Gn.edges(), pesos_perm):
        Gn[u][v]["weight"] = w
    return Gn


def correr_nulo(G, nombre_nulo, n_replicas, seed_base, log):
    Qs = []
    t0 = time.time()
    grados_ok_todas = True
    perdidas_pct = []
    for i in range(n_replicas):
        seed = seed_base + i
        if nombre_nulo == "double_edge_swap":
            Gn, grados_ok = nulo1_double_edge_swap(G, seed)
            grados_ok_todas &= grados_ok
        elif nombre_nulo == "configuration_model":
            Gn, perdida_pct = nulo2_configuration_model(G, seed)
            perdidas_pct.append(perdida_pct)
        elif nombre_nulo == "permutacion_pesos":
            Gn = nulo3_permutar_pesos(G, seed)
        else:
            raise ValueError(nombre_nulo)
        Qs.append(Q_de(Gn))
    dt = time.time() - t0
    log(f"    {nombre_nulo}: {n_replicas} réplicas en {dt:.1f}s ({dt/n_replicas*1000:.0f} ms/réplica)")
    if nombre_nulo == "double_edge_swap":
        log(f"      Secuencia de grados preservada en TODAS las réplicas: {grados_ok_todas}")
    if nombre_nulo == "configuration_model":
        log(f"      Densidad perdida al limpiar self-loops/multi-aristas: "
            f"media={np.mean(perdidas_pct):.2f}%, max={np.max(perdidas_pct):.2f}%")
    return np.array(Qs), dt


def resumen_nulo(Q_obs, Qs_nulo):
    media = float(np.mean(Qs_nulo))
    de = float(np.std(Qs_nulo, ddof=1))
    z = (Q_obs - media) / de if de > 0 else float("nan")
    p_emp = (1 + int(np.sum(Qs_nulo >= Q_obs))) / (1 + len(Qs_nulo))
    skew = float(stats.skew(Qs_nulo))
    kurt = float(stats.kurtosis(Qs_nulo))
    return {
        "Q_observado": float(Q_obs), "media_Q_nulo": media, "de_Q_nulo": de,
        "z_score": z, "p_valor_empirico": p_emp,
        "asimetria": skew, "curtosis": kurt,
        "n_replicas": len(Qs_nulo),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-replicas", type=int, default=1000)
    ap.add_argument("--redes", nargs="+", default=["bruto", "lift"], choices=["bruto", "lift"])
    ap.add_argument("--nulos", nargs="+", default=["double_edge_swap", "configuration_model", "permutacion_pesos"])
    ap.add_argument("--modo-rapido", action="store_true", help="usa 20 réplicas para estimar tiempo antes de la corrida completa")
    args = ap.parse_args()
    n_replicas = 20 if args.modo_rapido else args.n_replicas

    OUT_DIR.mkdir(exist_ok=True)
    log_lines = []
    def log(msg):
        print(msg, flush=True)
        log_lines.append(msg)

    redes = cargar_redes()
    resultados = {}

    for nombre_red in args.redes:
        G = redes[nombre_red]
        Q_obs = Q_de(G)
        log(f"\n{'='*70}\nRED: {nombre_red} ({G.number_of_nodes()} nodos, {G.number_of_edges():,} aristas) — Q observado = {Q_obs:.4f}\n{'='*70}")
        resultados[nombre_red] = {"Q_observado": Q_obs, "nulos": {}}
        for nombre_nulo in args.nulos:
            Qs, dt = correr_nulo(G, nombre_nulo, n_replicas, SEED_BASE, log)
            resumen = resumen_nulo(Q_obs, Qs)
            resultados[nombre_red]["nulos"][nombre_nulo] = resumen
            resultados[nombre_red]["nulos"][nombre_nulo]["tiempo_seg"] = dt
            resultados[nombre_red]["nulos"][nombre_nulo]["Qs_muestra"] = Qs[:50].tolist()
            log(f"      Q_nulo: media={resumen['media_Q_nulo']:.4f} DE={resumen['de_Q_nulo']:.4f} "
                f"| z={resumen['z_score']:.2f} | p_empírico={resumen['p_valor_empirico']:.4g} "
                f"| asimetría={resumen['asimetria']:.2f} curtosis={resumen['curtosis']:.2f}")

    if args.modo_rapido:
        tiempo_total_20 = sum(
            resultados[r]["nulos"][n]["tiempo_seg"]
            for r in resultados for n in resultados[r]["nulos"]
        )
        factor = args.n_replicas / n_replicas if n_replicas else 1
        log(f"\n[MODO RÁPIDO] Con {n_replicas} réplicas tardó {tiempo_total_20:.1f}s total. "
            f"Estimado para {args.n_replicas} réplicas: {tiempo_total_20*factor/60:.1f} min "
            f"({tiempo_total_20*factor:.0f}s).")

    sufijo = "_rapido" if args.modo_rapido else ""
    with open(OUT_DIR / f"parte2_modelos_nulos{sufijo}.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)
    with open(OUT_DIR / f"parte2_log{sufijo}.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))

    log(f"\nSalidas en {OUT_DIR}/parte2_modelos_nulos{sufijo}.json")


if __name__ == "__main__":
    main()
