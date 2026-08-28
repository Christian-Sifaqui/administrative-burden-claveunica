"""
Camino A · fase multi-mes -- Paso 21 (LOCAL): validacion estadistica,
Problema 2 -- estabilidad de la red de control (Parte 3, control decisivo).

`15_validacion_red_parte3.py` reporto Q=0,0164 para G_control (mismo numero de
aristas que G_lift, 16.052, seleccionadas por soporte en vez de lift) con una
UNICA corrida de Louvain (seed=42, sin reordenar). A diferencia de G_bruto y
G_lift (ya tratadas con 100 corridas en 20_parte1_estabilidad.py), este numero
quedaba sin tratamiento de estabilidad -- se corrige aqui, mismo protocolo:
100 corridas, variando semilla Y orden de insercion de nodos/aristas por
corrida, reportando media y desviacion estandar de Q.

Requiere: networkx, python-louvain, numpy (.venv_redes)
"""

import csv
import random
from pathlib import Path

import networkx as nx
import community as community_louvain
import numpy as np

DATA_DIR = Path("output_multimes")
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT_BASE = 1.2
SEED_BASE = 42
N_CORRIDAS = 100


def cargar_G_control():
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
            lift = soporte / esperado if esperado > 0 else float("nan")
            pares.append({"a": a, "b": b, "soporte": soporte, "lift": lift})

    # mismo procedimiento exacto que 15_validacion_red_parte3.py: fija el
    # numero de aristas objetivo con G_lift (umbral 1.2), luego arma el
    # control con esa misma cantidad de aristas, tomadas por soporte mas alto
    pares_lift_base = [p for p in pares if p["lift"] >= UMBRAL_LIFT_BASE]
    n_aristas_objetivo = len(pares_lift_base)

    pares_por_soporte = sorted(pares, key=lambda p: -p["soporte"])[:n_aristas_objetivo]
    G_control = nx.Graph()
    for p in pares_por_soporte:
        G_control.add_edge(p["a"], p["b"], weight=p["soporte"])
    return G_control, n_aristas_objetivo


def grafo_reordenado(G, seed):
    rng = random.Random(seed)
    nodos = list(G.nodes())
    rng.shuffle(nodos)
    aristas = list(G.edges(data=True))
    rng.shuffle(aristas)
    Gr = nx.Graph()
    Gr.add_nodes_from(nodos)
    for u, v, d in aristas:
        Gr.add_edge(u, v, **d)
    return Gr


def main():
    G_control, n_aristas_objetivo = cargar_G_control()
    print(f"G_control: {G_control.number_of_nodes()} nodos, {G_control.number_of_edges():,} aristas "
          f"(objetivo {n_aristas_objetivo:,}, igual a G_lift)")

    Q_original = community_louvain.modularity(
        community_louvain.best_partition(G_control, weight="weight", random_state=SEED_BASE),
        G_control, weight="weight")
    print(f"Q con la corrida original (seed=42, sin reordenar, la publicada): {Q_original:.6f}")

    Qs = []
    n_coms = []
    for i in range(N_CORRIDAS):
        seed = SEED_BASE + i
        Gr = grafo_reordenado(G_control, seed)
        part = community_louvain.best_partition(Gr, weight="weight", random_state=seed)
        Q = community_louvain.modularity(part, Gr, weight="weight")
        Qs.append(Q)
        n_coms.append(len(set(part.values())))
    Qs = np.array(Qs)
    n_coms = np.array(n_coms)

    print(f"\n{N_CORRIDAS} corridas (semilla y orden de nodos/aristas variados):")
    print(f"  Q: media={Qs.mean():.4f} DE={Qs.std(ddof=1):.4f} min={Qs.min():.4f} max={Qs.max():.4f} "
          f"p2.5={np.percentile(Qs,2.5):.4f} p97.5={np.percentile(Qs,97.5):.4f}")
    print(f"  num. comunidades: media={n_coms.mean():.2f} DE={n_coms.std(ddof=1):.2f} "
          f"min={n_coms.min()} max={n_coms.max()}")

    # razon contra Q_lift media de 100 corridas (Parte 1), para actualizar el veredicto
    Q_lift_100_media = 0.548245534629986
    print(f"\nRazón Q_control/Q_lift usando medias de 100 corridas: "
          f"{Qs.mean()/Q_lift_100_media:.4f} (original con corridas únicas: 0,0164/0,5483=0,0299)")

    with open(DATA_DIR / "validacion_red" / "parte3_control_estabilidad.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["corrida", "seed", "Q", "n_comunidades"])
        for i in range(N_CORRIDAS):
            w.writerow([i, SEED_BASE + i, Qs[i], n_coms[i]])
    print(f"\nSalida en output_multimes/validacion_red/parte3_control_estabilidad.csv")


if __name__ == "__main__":
    main()
