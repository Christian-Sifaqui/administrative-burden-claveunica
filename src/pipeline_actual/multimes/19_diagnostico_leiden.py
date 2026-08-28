"""
Camino A · fase multi-mes -- Paso 19 (LOCAL): diagnostico de la discrepancia
Leiden (Q~0.01) vs Louvain (Q=0.548) sobre G_lift, antes de invertir en la
Parte 1 completa (100 corridas, ARI/NMI, matriz de coasignacion).

NO CORRIGE NADA -- solo diagnostica, en el orden que establecio Christian:
  0. Misma funcion de modularidad para ambas particiones (¿problema de medicion
     o de particion?).
  1. Integridad del traspaso NetworkX -> igraph (nodos, aristas, dirigido/no
     dirigido, atributo de peso, suma de pesos).
  2. Tres configuraciones de leidenalg (ModularityVertexPartition,
     RBConfigurationVertexPartition resolution=1.0, y sin pesos) para aislar
     perdida de pesos vs parametro de resolucion.
  3. ARI/NMI entre la mejor particion de Leiden y la de Louvain, mas
     distribucion de tamanos de comunidad de cada una.
  4. Diagnostico en lenguaje llano.

Requiere: networkx, python-igraph, leidenalg, python-louvain, numpy,
scikit-learn (.venv_redes)
"""

import csv
from pathlib import Path

import networkx as nx
import community as community_louvain
import igraph as ig
import leidenalg
import numpy as np
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

DATA_DIR = Path("output_multimes")
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
SEED = 42


def cargar_G_lift():
    marginales = {}
    with open(DATA_DIR / "red_institucional_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])
    G = nx.Graph()
    with open(DATA_DIR / "red_institucional_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            soporte = int(row["soporte"])
            if soporte < MIN_SOPORTE_PAR:
                continue
            a, b = row["inst_a"], row["inst_b"]
            ma, mb = marginales.get(a, 0), marginales.get(b, 0)
            esperado = (ma * mb) / n_total if n_total else 0
            lift = soporte / esperado if esperado > 0 else float("nan")
            if lift >= UMBRAL_LIFT:
                G.add_edge(a, b, weight=lift)
    return G


def main():
    print("Cargando G_lift...")
    G = cargar_G_lift()
    print(f"G_lift: {G.number_of_nodes()} nodos, {G.number_of_edges():,} aristas")
    pesos_nx = [d["weight"] for _, _, d in G.edges(data=True)]
    print(f"  peso: min={min(pesos_nx):.3f} max={max(pesos_nx):.3f} suma={sum(pesos_nx):.3f}")

    print(f"\n{'='*70}\n[0] Particion de Louvain, Q y modularidad de referencia\n{'='*70}")
    part_louvain = community_louvain.best_partition(G, weight="weight", random_state=SEED)
    Q_louvain_reportado = community_louvain.modularity(part_louvain, G, weight="weight")
    n_com_louvain = len(set(part_louvain.values()))
    print(f"Louvain: Q={Q_louvain_reportado:.4f}, {n_com_louvain} comunidades")

    # Comunidades en formato "lista de conjuntos" para nx.algorithms.community.modularity
    def part_dict_a_comunidades(part_dict):
        coms = {}
        for nodo, com in part_dict.items():
            coms.setdefault(com, set()).add(nodo)
        return list(coms.values())

    comunidades_louvain = part_dict_a_comunidades(part_louvain)
    Q_louvain_nx = nx.algorithms.community.modularity(G, comunidades_louvain, weight="weight")
    print(f"Louvain, recalculado con nx.algorithms.community.modularity: Q={Q_louvain_nx:.4f} "
          f"({'coincide' if abs(Q_louvain_nx - Q_louvain_reportado) < 1e-6 else 'NO COINCIDE -- revisar'})")

    print(f"\n{'='*70}\n[1] Traspaso NetworkX -> igraph\n{'='*70}")
    g_ig = ig.Graph.from_networkx(G)
    print(f"igraph: {g_ig.vcount()} nodos, {g_ig.ecount()} aristas")
    print(f"  nodos coinciden: {g_ig.vcount() == G.number_of_nodes()}")
    print(f"  aristas coinciden: {g_ig.ecount() == G.number_of_edges()}")
    print(f"  es dirigido: {g_ig.is_directed()} (deberia ser False)")
    print(f"  'weight' en g.es.attributes(): {'weight' in g_ig.es.attributes()}")
    if "weight" in g_ig.es.attributes():
        suma_ig = sum(w for w in g_ig.es["weight"] if w is not None)
        print(f"  suma de pesos en igraph: {suma_ig:.3f}  vs. NetworkX: {sum(pesos_nx):.3f}  "
              f"({'coincide' if abs(suma_ig - sum(pesos_nx)) < 1e-3 else 'NO COINCIDE'})")
        n_sin_peso = sum(1 for w in g_ig.es["weight"] if w is None)
        print(f"  aristas sin peso (None): {n_sin_peso}")
    else:
        print("  ADVERTENCIA: no existe atributo 'weight' en las aristas de igraph")

    # mapeo de nombres: igraph guarda el nombre original en el atributo '_nx_name' o 'name'
    nombre_attr = "_nx_name" if "_nx_name" in g_ig.vs.attributes() else (
        "name" if "name" in g_ig.vs.attributes() else None)
    print(f"  atributo de nombre de nodo en igraph: {nombre_attr!r}")
    if nombre_attr:
        nombres_ig = set(g_ig.vs[nombre_attr])
        nombres_nx = set(G.nodes())
        print(f"  nombres de nodo coinciden exactamente: {nombres_ig == nombres_nx}")

    print(f"\n{'='*70}\n[2] leidenalg -- tres configuraciones\n{'='*70}")

    def correr_leiden(partition_type, pasar_pesos, resolution_parameter=None, etiqueta=""):
        kwargs = {"seed": SEED}
        if pasar_pesos:
            kwargs["weights"] = "weight"
        if resolution_parameter is not None:
            kwargs["resolution_parameter"] = resolution_parameter
        part = leidenalg.find_partition(g_ig, partition_type, **kwargs)
        # mapear indices de igraph -> nombres de nodo -> dict estilo louvain
        membership = part.membership
        if nombre_attr:
            part_dict = {g_ig.vs[i][nombre_attr]: membership[i] for i in range(g_ig.vcount())}
        else:
            part_dict = {i: membership[i] for i in range(g_ig.vcount())}
        comunidades = part_dict_a_comunidades(part_dict)
        Q_nx = nx.algorithms.community.modularity(G, comunidades, weight="weight")
        n_com = len(set(membership))
        print(f"  {etiqueta}: Q(nx)={Q_nx:.4f}  {n_com} comunidades  "
              f"(Q reportado por igraph internamente: {part.quality():.4f} -- ojo, esta "
              f"escala puede no ser directamente comparable a modularidad clasica "
              f"segun el tipo de particion)")
        return part_dict, Q_nx

    print("Con pesos, ModularityVertexPartition (equivalente directo de Louvain):")
    part_mod_w, Q_mod_w = correr_leiden(
        leidenalg.ModularityVertexPartition, pasar_pesos=True,
        etiqueta="ModularityVertexPartition + weights='weight'")

    print("\nCon pesos, RBConfigurationVertexPartition, resolution_parameter=1.0:")
    part_rb_w, Q_rb_w = correr_leiden(
        leidenalg.RBConfigurationVertexPartition, pasar_pesos=True, resolution_parameter=1.0,
        etiqueta="RBConfigurationVertexPartition(res=1.0) + weights='weight'")

    print("\nSIN pesos, ModularityVertexPartition (para confirmar/descartar perdida de pesos):")
    part_mod_sin, Q_mod_sin = correr_leiden(
        leidenalg.ModularityVertexPartition, pasar_pesos=False,
        etiqueta="ModularityVertexPartition SIN weights")

    print(f"\n{'='*70}\n[3] Comparacion Leiden (mejor configuracion) vs Louvain\n{'='*70}")
    candidatos = {
        "ModularityVertexPartition+pesos": (part_mod_w, Q_mod_w),
        "RBConfigurationVertexPartition(res=1.0)+pesos": (part_rb_w, Q_rb_w),
        "ModularityVertexPartition sin pesos": (part_mod_sin, Q_mod_sin),
    }
    mejor_nombre = max(candidatos, key=lambda k: candidatos[k][1])
    part_leiden_mejor, Q_leiden_mejor = candidatos[mejor_nombre]
    print(f"Mejor configuracion de Leiden: {mejor_nombre} (Q={Q_leiden_mejor:.4f})")

    nodos_orden = list(G.nodes())
    labels_louvain = [part_louvain[n] for n in nodos_orden]
    labels_leiden = [part_leiden_mejor[n] for n in nodos_orden]
    ari = adjusted_rand_score(labels_louvain, labels_leiden)
    nmi = normalized_mutual_info_score(labels_louvain, labels_leiden)
    print(f"ARI(Louvain, Leiden mejor) = {ari:.4f}")
    print(f"NMI(Louvain, Leiden mejor) = {nmi:.4f}")

    def tamanos(part_dict):
        coms = {}
        for n, c in part_dict.items():
            coms[c] = coms.get(c, 0) + 1
        return sorted(coms.values(), reverse=True)

    print(f"Tamaños de comunidad, Louvain: {tamanos(part_louvain)}")
    print(f"Tamaños de comunidad, Leiden ({mejor_nombre}): {tamanos(part_leiden_mejor)}")

    print(f"\n{'='*70}\n[4] Diagnostico\n{'='*70}")
    if abs(Q_louvain_nx - Q_louvain_reportado) > 1e-6:
        print("CAUSA: discrepancia de MEDICION incluso dentro de Louvain -- revisar antes de mirar Leiden.")
    elif Q_mod_sin > 0.3 and Q_mod_w < 0.1:
        print("CAUSA CONFIRMADA: perdida de pesos en la llamada a find_partition -- "
              "sin pasar 'weights' explicitamente, Leiden particiona la red binaria.")
    elif Q_mod_w > 0.4:
        print("CAUSA: el problema NO esta en Leiden con la configuracion correcta "
              "(ModularityVertexPartition + pesos) -- da Q comparable a Louvain. "
              "El Q~0.01 original vino de otra configuracion (probablemente "
              "RBConfigurationVertexPartition con resolution_parameter != 1.0, o CPM).")
    elif Q_rb_w > 0.4 and Q_mod_w <= 0.1:
        print("CAUSA: ModularityVertexPartition sigue dando Q bajo incluso con pesos -- "
              "revisar traspaso del grafo (ver seccion [1] arriba) mas de cerca.")
    else:
        print("Ninguna de las causas esperadas se confirmo limpiamente con las configuraciones "
              "probadas -- revisar manualmente los numeros de [1]-[3] arriba.")


if __name__ == "__main__":
    main()
