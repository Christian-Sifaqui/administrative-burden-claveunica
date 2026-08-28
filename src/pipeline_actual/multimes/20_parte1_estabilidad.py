"""
Camino A · fase multi-mes -- Paso 20 (LOCAL): validacion estadistica,
Problema 2, Parte 1 -- modularidad observada y su estabilidad.

Ahora que 19_diagnostico_leiden.py confirmo que la discrepancia Leiden~0.01 vs
Louvain=0.548 del 2026-07-27 era un problema de configuracion (faltaba pasar
weights explicitamente / resolution_parameter incorrecto), y que con la
configuracion correcta Leiden reproduce Louvain (Q=0.5484 vs 0.5483, ARI=0.90,
NMI=0.84), esta parte corre el analisis de estabilidad completo.

Para CADA red (G_bruto, G_lift):
  1. 100 corridas de Louvain con semilla Y orden de nodos/aristas distintos
     por corrida (Louvain depende de ambos, no solo de la semilla interna de
     community_louvain). Q ponderada y binaria en cada corrida (dos
     optimizaciones separadas, no solo dos evaluaciones de la misma
     particion), num. de comunidades.
  2. Estabilidad entre corridas: ARI y NMI para las C(100,2)=4950 parejas
     (barato a esta escala, no hace falta muestrear).
  3. Particion "modal": la de mayor ARI promedio contra las otras 99 -- sirve
     de referencia para (a) comparar con Leiden y (b) medir inestabilidad por
     nodo.
  4. Nodos inestables: se alinean las etiquetas de cada corrida contra la
     modal via el algoritmo hungaro (maximiza solapamiento arista-a-arista
     sobre la tabla de contingencia) y se mide, por nodo, en que fraccion de
     las 100 corridas su etiqueta alineada difiere de la modal. Inestable si
     >20% de las corridas.
  5. Leiden con ModularityVertexPartition y RBConfigurationVertexPartition
     (resolution=1.0), pesos pasados explicitamente (la config. que el
     diagnostico confirmo correcta) -- Q, comunidades, ARI/NMI contra la
     modal de Louvain.
  6. Patologia que motivo Leiden: se verifica si alguna comunidad de la
     particion modal de Louvain induce un subgrafo internamente desconectado.

Requiere: networkx, python-louvain, python-igraph, leidenalg, numpy, scipy,
scikit-learn (.venv_redes)
"""

import csv
import json
import random
import time
from pathlib import Path

import networkx as nx
import community as community_louvain
import igraph as ig
import leidenalg
import numpy as np
from scipy.optimize import linear_sum_assignment
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
SEED_BASE = 42
N_CORRIDAS = 100
UMBRAL_INESTABLE = 0.20


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
            lift = soporte / esperado if esperado > 0 else float("nan")
            pares.append({"a": a, "b": b, "soporte": soporte, "lift": lift})

    G_bruto = nx.Graph()
    for p in pares:
        G_bruto.add_edge(p["a"], p["b"], weight=p["soporte"])

    G_lift = nx.Graph()
    for p in pares:
        if p["lift"] >= UMBRAL_LIFT:
            G_lift.add_edge(p["a"], p["b"], weight=p["lift"])

    return {"bruto": G_bruto, "lift": G_lift}


def grafo_reordenado(G, seed):
    """Copia de G con nodos y aristas insertados en orden aleatorio -- Louvain
    depende del orden de recorrido, no solo de la semilla interna."""
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


def grafo_binario(G):
    Gb = nx.Graph()
    Gb.add_edges_from(G.edges())
    return Gb


def part_a_labels(part_dict, orden_nodos):
    return np.array([part_dict[n] for n in orden_nodos])


def alinear_a_modal(labels_run, labels_modal):
    """Algoritmo hungaro sobre la tabla de contingencia para encontrar el
    mapeo de etiquetas de 'labels_run' que maximiza el solapamiento con
    'labels_modal'. Devuelve labels_run remapeadas."""
    coms_run = sorted(set(labels_run))
    coms_modal = sorted(set(labels_modal))
    idx_run = {c: i for i, c in enumerate(coms_run)}
    idx_modal = {c: i for i, c in enumerate(coms_modal)}
    tabla = np.zeros((len(coms_run), len(coms_modal)))
    for lr, lm in zip(labels_run, labels_modal):
        tabla[idx_run[lr], idx_modal[lm]] += 1
    fila_ind, col_ind = linear_sum_assignment(-tabla)  # maximizar solapamiento
    mapeo = {}
    for fi, ci in zip(fila_ind, col_ind):
        mapeo[coms_run[fi]] = coms_modal[ci]
    # comunidades de la corrida sin pareja (mas comunidades que la modal): les
    # asigna una etiqueta nueva que nunca coincide con la modal, para que
    # cuenten como "distinto" honestamente en vez de colapsar por accidente
    siguiente = max(coms_modal) + 1 if coms_modal else 0
    for c in coms_run:
        if c not in mapeo:
            mapeo[c] = siguiente
            siguiente += 1
    return np.array([mapeo[l] for l in labels_run])


def main():
    OUT_DIR.mkdir(exist_ok=True)
    log_lines = []
    def log(msg):
        print(msg, flush=True)
        log_lines.append(msg)

    redes = cargar_redes()
    resumen = {}

    for nombre_red, G in redes.items():
        log(f"\n{'='*70}\nRED: {nombre_red} ({G.number_of_nodes()} nodos, {G.number_of_edges():,} aristas)\n{'='*70}")
        orden_nodos = sorted(G.nodes())
        Gb = grafo_binario(G)

        # --- 100 corridas ---
        t0 = time.time()
        Qs_pond, Qs_bin, n_coms = [], [], []
        particiones = []  # lista de arrays de labels, en el orden de orden_nodos
        for i in range(N_CORRIDAS):
            seed = SEED_BASE + i
            Gr = grafo_reordenado(G, seed)
            part = community_louvain.best_partition(Gr, weight="weight", random_state=seed)
            Q_p = community_louvain.modularity(part, Gr, weight="weight")

            Grb = grafo_reordenado(Gb, seed)
            part_b = community_louvain.best_partition(Grb, weight="weight", random_state=seed)
            Q_b = community_louvain.modularity(part_b, Grb, weight="weight")

            Qs_pond.append(Q_p)
            Qs_bin.append(Q_b)
            n_coms.append(len(set(part.values())))
            particiones.append(part_a_labels(part, orden_nodos))
        dt = time.time() - t0
        Qs_pond = np.array(Qs_pond)
        Qs_bin = np.array(Qs_bin)
        n_coms = np.array(n_coms)
        log(f"  {N_CORRIDAS} corridas en {dt:.1f}s")
        log(f"  Q ponderada: media={Qs_pond.mean():.4f} DE={Qs_pond.std(ddof=1):.4f} "
            f"min={Qs_pond.min():.4f} max={Qs_pond.max():.4f} "
            f"p2.5={np.percentile(Qs_pond,2.5):.4f} p97.5={np.percentile(Qs_pond,97.5):.4f}")
        log(f"  Q binaria:   media={Qs_bin.mean():.4f} DE={Qs_bin.std(ddof=1):.4f} "
            f"min={Qs_bin.min():.4f} max={Qs_bin.max():.4f} "
            f"p2.5={np.percentile(Qs_bin,2.5):.4f} p97.5={np.percentile(Qs_bin,97.5):.4f}")
        log(f"  num. comunidades: media={n_coms.mean():.2f} DE={n_coms.std(ddof=1):.2f} "
            f"min={n_coms.min()} max={n_coms.max()}")

        # --- ARI/NMI entre las 100 corridas (todas las 4950 parejas) ---
        t0 = time.time()
        aris, nmis = [], []
        for i in range(N_CORRIDAS):
            for j in range(i + 1, N_CORRIDAS):
                aris.append(adjusted_rand_score(particiones[i], particiones[j]))
                nmis.append(normalized_mutual_info_score(particiones[i], particiones[j]))
        aris = np.array(aris); nmis = np.array(nmis)
        log(f"  ARI entre corridas (4950 parejas, {time.time()-t0:.1f}s): "
            f"media={aris.mean():.4f} DE={aris.std(ddof=1):.4f} min={aris.min():.4f}")
        log(f"  NMI entre corridas: media={nmis.mean():.4f} DE={nmis.std(ddof=1):.4f} min={nmis.min():.4f}")

        # --- particion modal: mayor ARI promedio contra las otras 99 ---
        ari_matriz = np.zeros((N_CORRIDAS, N_CORRIDAS))
        idx = 0
        for i in range(N_CORRIDAS):
            for j in range(i + 1, N_CORRIDAS):
                ari_matriz[i, j] = ari_matriz[j, i] = aris[idx]
                idx += 1
        ari_promedio_por_corrida = ari_matriz.sum(axis=1) / (N_CORRIDAS - 1)
        idx_modal = int(np.argmax(ari_promedio_por_corrida))
        labels_modal = particiones[idx_modal]
        log(f"  Partición modal: corrida #{idx_modal} (ARI promedio={ari_promedio_por_corrida[idx_modal]:.4f}, "
            f"{n_coms[idx_modal]} comunidades)")

        # --- nodos inestables ---
        desacuerdos = np.zeros(len(orden_nodos), dtype=int)
        for i in range(N_CORRIDAS):
            labels_alineadas = alinear_a_modal(particiones[i], labels_modal)
            desacuerdos += (labels_alineadas != labels_modal).astype(int)
        frac_desacuerdo = desacuerdos / N_CORRIDAS
        inestables_idx = np.where(frac_desacuerdo > UMBRAL_INESTABLE)[0]
        inestables = sorted(
            [(orden_nodos[i], float(frac_desacuerdo[i])) for i in inestables_idx],
            key=lambda x: -x[1])
        log(f"  Nodos inestables (>{UMBRAL_INESTABLE*100:.0f}% de corridas en desacuerdo con la modal, "
            f"tras alinear etiquetas): {len(inestables)} de {len(orden_nodos)}")
        for nombre, frac in inestables[:15]:
            log(f"    {nombre:<55} {frac*100:.1f}%")
        if len(inestables) > 15:
            log(f"    ... y {len(inestables)-15} más (ver CSV completo)")

        # --- Leiden ---
        g_ig = ig.Graph.from_networkx(G)
        nombre_attr = "_nx_name" if "_nx_name" in g_ig.vs.attributes() else "name"
        orden_ig = [g_ig.vs[i][nombre_attr] for i in range(g_ig.vcount())]

        def leiden_a_labels(part_type, **kw):
            part = leidenalg.find_partition(g_ig, part_type, weights="weight", seed=SEED_BASE, **kw)
            membership = {orden_ig[i]: part.membership[i] for i in range(g_ig.vcount())}
            return part_a_labels(membership, orden_nodos), len(set(part.membership))

        labels_leiden_mod, n_com_leiden_mod = leiden_a_labels(leidenalg.ModularityVertexPartition)
        labels_leiden_rb, n_com_leiden_rb = leiden_a_labels(
            leidenalg.RBConfigurationVertexPartition, resolution_parameter=1.0)

        def Q_de_labels(labels):
            part_dict = {orden_nodos[i]: int(labels[i]) for i in range(len(orden_nodos))}
            coms = {}
            for n, c in part_dict.items():
                coms.setdefault(c, set()).add(n)
            return nx.algorithms.community.modularity(G, list(coms.values()), weight="weight")

        Q_leiden_mod = Q_de_labels(labels_leiden_mod)
        Q_leiden_rb = Q_de_labels(labels_leiden_rb)
        ari_mod_vs_louvain = adjusted_rand_score(labels_modal, labels_leiden_mod)
        nmi_mod_vs_louvain = normalized_mutual_info_score(labels_modal, labels_leiden_mod)
        ari_rb_vs_louvain = adjusted_rand_score(labels_modal, labels_leiden_rb)
        nmi_rb_vs_louvain = normalized_mutual_info_score(labels_modal, labels_leiden_rb)
        log(f"  Leiden ModularityVertexPartition: Q={Q_leiden_mod:.4f}, {n_com_leiden_mod} comunidades, "
            f"ARI vs. modal Louvain={ari_mod_vs_louvain:.4f}, NMI={nmi_mod_vs_louvain:.4f}")
        log(f"  Leiden RBConfigurationVertexPartition(res=1.0): Q={Q_leiden_rb:.4f}, {n_com_leiden_rb} comunidades, "
            f"ARI vs. modal Louvain={ari_rb_vs_louvain:.4f}, NMI={nmi_rb_vs_louvain:.4f}")

        # --- patologia: comunidades internamente desconectadas en la modal ---
        part_dict_modal = {orden_nodos[i]: int(labels_modal[i]) for i in range(len(orden_nodos))}
        coms_modal = {}
        for n, c in part_dict_modal.items():
            coms_modal.setdefault(c, set()).add(n)
        n_desconectadas = 0
        detalle_desconectadas = []
        for c, miembros in coms_modal.items():
            sub = G.subgraph(miembros)
            n_comp = nx.number_connected_components(sub)
            if n_comp > 1:
                n_desconectadas += 1
                tamanos_comp = sorted([len(cc) for cc in nx.connected_components(sub)], reverse=True)
                detalle_desconectadas.append({"comunidad": c, "tamano_total": len(miembros),
                                               "n_componentes": n_comp, "tamanos_componentes": tamanos_comp})
        log(f"  Comunidades de la modal internamente desconectadas: {n_desconectadas} de {len(coms_modal)}")
        for d in detalle_desconectadas:
            log(f"    comunidad {d['comunidad']} (tamaño {d['tamano_total']}): "
                f"{d['n_componentes']} componentes, tamaños {d['tamanos_componentes']}")

        # --- salidas por red ---
        with open(OUT_DIR / f"parte1_{nombre_red}_corridas.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["corrida", "seed", "Q_ponderada", "Q_binaria", "n_comunidades"])
            for i in range(N_CORRIDAS):
                w.writerow([i, SEED_BASE + i, Qs_pond[i], Qs_bin[i], n_coms[i]])

        with open(OUT_DIR / f"parte1_{nombre_red}_particion_modal.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["institucion", "comunidad_modal"])
            for i, n in enumerate(orden_nodos):
                w.writerow([n, int(labels_modal[i])])

        with open(OUT_DIR / f"parte1_{nombre_red}_nodos_inestables.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["institucion", "frac_desacuerdo_con_modal", "inestable_mayor_20pct"])
            for i, n in enumerate(orden_nodos):
                w.writerow([n, float(frac_desacuerdo[i]), frac_desacuerdo[i] > UMBRAL_INESTABLE])

        resumen[nombre_red] = {
            "n_nodos": G.number_of_nodes(), "n_aristas": G.number_of_edges(),
            "Q_ponderada": {"media": float(Qs_pond.mean()), "de": float(Qs_pond.std(ddof=1)),
                             "min": float(Qs_pond.min()), "max": float(Qs_pond.max()),
                             "p2_5": float(np.percentile(Qs_pond, 2.5)), "p97_5": float(np.percentile(Qs_pond, 97.5))},
            "Q_binaria": {"media": float(Qs_bin.mean()), "de": float(Qs_bin.std(ddof=1)),
                          "min": float(Qs_bin.min()), "max": float(Qs_bin.max()),
                          "p2_5": float(np.percentile(Qs_bin, 2.5)), "p97_5": float(np.percentile(Qs_bin, 97.5))},
            "n_comunidades": {"media": float(n_coms.mean()), "de": float(n_coms.std(ddof=1)),
                               "min": int(n_coms.min()), "max": int(n_coms.max())},
            "estabilidad_entre_corridas": {
                "ARI_media": float(aris.mean()), "ARI_de": float(aris.std(ddof=1)), "ARI_min": float(aris.min()),
                "NMI_media": float(nmis.mean()), "NMI_de": float(nmis.std(ddof=1)), "NMI_min": float(nmis.min()),
            },
            "particion_modal": {"indice_corrida": idx_modal, "n_comunidades": int(n_coms[idx_modal]),
                                 "ari_promedio_contra_otras": float(ari_promedio_por_corrida[idx_modal])},
            "nodos_inestables": {"n": len(inestables), "umbral_pct": UMBRAL_INESTABLE * 100,
                                  "lista": [{"institucion": n, "frac_desacuerdo": f} for n, f in inestables]},
            "leiden": {
                "ModularityVertexPartition": {"Q": Q_leiden_mod, "n_comunidades": n_com_leiden_mod,
                                               "ARI_vs_modal_louvain": ari_mod_vs_louvain,
                                               "NMI_vs_modal_louvain": nmi_mod_vs_louvain},
                "RBConfigurationVertexPartition_res1": {"Q": Q_leiden_rb, "n_comunidades": n_com_leiden_rb,
                                                         "ARI_vs_modal_louvain": ari_rb_vs_louvain,
                                                         "NMI_vs_modal_louvain": nmi_rb_vs_louvain},
            },
            "patologia_desconexion": {"n_comunidades_desconectadas": n_desconectadas,
                                       "n_comunidades_total": len(coms_modal),
                                       "detalle": detalle_desconectadas},
        }

    with open(OUT_DIR / "parte1_resumen.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)
    with open(OUT_DIR / "parte1_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))

    print(f"\nSalidas en {OUT_DIR}/parte1_*.csv, parte1_resumen.json, parte1_log.txt")


if __name__ == "__main__":
    main()
