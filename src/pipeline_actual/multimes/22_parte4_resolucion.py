"""
Camino A · fase multi-mes -- Paso 22 (LOCAL): validacion estadistica,
Problema 2, Parte 4 -- limite de resolucion (Fortunato y Barthelemy, 2007).

PREGUNTA: la modularidad baja de G_bruto (Q media=0,0226, DE=0,003 sobre 100
corridas) ¿se debe al limite de resolucion de la modularidad, que puede ocultar
estructura a otra escala, o a ausencia real de estructura de comunidades?

Ya esta establecido, via tres modelos nulos (Paso 17), que la baja modularidad
de G_bruto obedece a una configuracion de estrella: los 6 hubs de la Seccion
4.2 estan en el 9,9% de las aristas y concentran el 53,8% del peso. Esta parte
descarta la explicacion alternativa.

DISTINCION CRITICA (si no se respeta, los valores no son comparables entre
gammas): la FUNCION OBJETIVO que se optimiza usa gamma
(`louvain_communities(resolution=gamma)`), pero la METRICA que se reporta es
siempre la modularidad clasica con gamma=1
(`nx.algorithms.community.modularity(G, comunidades, weight='weight')`, cuyo
parametro `resolution` queda en su default de 1,0). Se reportan ambas por
separado: `Q_clasica` (comparable entre gammas) y `Q_objetivo_gamma` (el valor
de la funcion que efectivamente se optimizo, NO comparable entre gammas).

Requiere: networkx, numpy (.venv_redes)
"""

import argparse
import csv
from pathlib import Path

import networkx as nx
import numpy as np

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
GAMMAS = [0.5, 0.75, 1.0, 1.5, 2.0, 4.0]
N_CORRIDAS = 20


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-corridas", type=int, default=N_CORRIDAS)
    args = ap.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    log_lines = []
    def log(msg):
        print(msg, flush=True)
        log_lines.append(msg)

    log(f"Semilla base: {args.seed} | corridas por gamma: {args.n_corridas}")
    log("NOTA: 'Q_clasica' se mide SIEMPRE con gamma=1 (comparable entre gammas). "
        "'Q_objetivo' es el valor de la función que se optimizó con cada gamma "
        "(NO comparable entre gammas).")

    redes = cargar_redes()
    filas = []
    mejor_gamma_bruto = None
    mejor_n_no_triviales = -1
    mejor_particion_bruto = None

    for nombre_red, G in redes.items():
        log(f"\n{'='*78}\nRED: {nombre_red} ({G.number_of_nodes()} nodos, {G.number_of_edges():,} aristas)\n{'='*78}")
        log(f"{'gamma':>6} | {'Q_clásica (media±DE)':>24} | {'Q_objetivo(γ)':>14} | "
            f"{'n_com':>12} | {'com_mayor':>12} | {'%nodos en com=1':>16}")
        for gamma in GAMMAS:
            Qs_clasica, Qs_objetivo, n_coms, com_mayor, frac_singleton = [], [], [], [], []
            particiones = []
            for i in range(args.n_corridas):
                seed = args.seed + i
                comunidades = nx.community.louvain_communities(
                    G, weight="weight", resolution=gamma, seed=seed)
                # metrica comparable: modularidad clasica, resolution=1 (default)
                Q_c = nx.algorithms.community.modularity(G, comunidades, weight="weight")
                # valor de la funcion objetivo efectivamente optimizada con este gamma
                Q_o = nx.algorithms.community.modularity(
                    G, comunidades, weight="weight", resolution=gamma)
                tam = sorted((len(c) for c in comunidades), reverse=True)
                Qs_clasica.append(Q_c)
                Qs_objetivo.append(Q_o)
                n_coms.append(len(comunidades))
                com_mayor.append(tam[0])
                frac_singleton.append(sum(1 for t in tam if t == 1) / G.number_of_nodes())
                particiones.append(comunidades)

            Qs_clasica = np.array(Qs_clasica); Qs_objetivo = np.array(Qs_objetivo)
            n_coms = np.array(n_coms); com_mayor = np.array(com_mayor)
            frac_singleton = np.array(frac_singleton)
            log(f"{gamma:>6} | {Qs_clasica.mean():>10.4f} ± {Qs_clasica.std(ddof=1):<11.4f} | "
                f"{Qs_objetivo.mean():>14.4f} | {n_coms.mean():>5.1f} ± {n_coms.std(ddof=1):<4.1f} | "
                f"{com_mayor.mean():>5.1f} ± {com_mayor.std(ddof=1):<4.1f} | "
                f"{100*frac_singleton.mean():>15.1f}%")

            filas.append({
                "red": nombre_red, "gamma": gamma, "n_corridas": args.n_corridas,
                "semilla_base": args.seed,
                "Q_clasica_media": Qs_clasica.mean(), "Q_clasica_de": Qs_clasica.std(ddof=1),
                "Q_objetivo_gamma_media": Qs_objetivo.mean(), "Q_objetivo_gamma_de": Qs_objetivo.std(ddof=1),
                "n_comunidades_media": n_coms.mean(), "n_comunidades_de": n_coms.std(ddof=1),
                "tam_comunidad_mayor_media": com_mayor.mean(), "tam_comunidad_mayor_de": com_mayor.std(ddof=1),
                "frac_nodos_en_comunidades_singleton": frac_singleton.mean(),
            })

            # gamma mas informativo en bruto: mayor num. de comunidades NO triviales
            # (tamaño >= 2) -- singletons no cuentan como estructura interpretable
            if nombre_red == "bruto":
                no_triviales = [sum(1 for c in p if len(c) >= 2) for p in particiones]
                promedio_no_triviales = float(np.mean(no_triviales))
                if promedio_no_triviales > mejor_n_no_triviales:
                    mejor_n_no_triviales = promedio_no_triviales
                    mejor_gamma_bruto = gamma
                    # se guarda la particion de la primera corrida de ese gamma
                    mejor_particion_bruto = particiones[0]

    with open(OUT_DIR / "parte4_barrido_resolucion.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)

    log(f"\n{'='*78}\nGamma más informativo en G_bruto (mayor num. de comunidades no triviales): "
        f"{mejor_gamma_bruto} ({mejor_n_no_triviales:.1f} comunidades de tamaño>=2)\n{'='*78}")
    if mejor_particion_bruto is not None:
        comunidades_ordenadas = sorted(mejor_particion_bruto, key=len, reverse=True)
        with open(OUT_DIR / "parte4_bruto_comunidades_gamma_informativo.csv", "w",
                  newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["comunidad", "tamano_comunidad", "institucion", "gamma", "semilla"])
            for idx, com in enumerate(comunidades_ordenadas):
                for inst in sorted(com):
                    w.writerow([idx, len(com), inst, mejor_gamma_bruto, args.seed])
        log(f"Tamaños de las comunidades a gamma={mejor_gamma_bruto}: "
            f"{[len(c) for c in comunidades_ordenadas][:20]}"
            f"{' ...' if len(comunidades_ordenadas) > 20 else ''}")
        log("Primeras comunidades no triviales (para juzgar interpretabilidad sustantiva):")
        mostradas = 0
        for com in comunidades_ordenadas:
            if len(com) < 2:
                continue
            if mostradas >= 6:
                break
            miembros = sorted(com)
            log(f"  [{len(com)} instituciones] {', '.join(miembros[:8])}"
                f"{' ...' if len(miembros) > 8 else ''}")
            mostradas += 1

    with open(OUT_DIR / "parte4_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"\nSalidas en {OUT_DIR}/parte4_barrido_resolucion.csv, "
        f"parte4_bruto_comunidades_gamma_informativo.csv, parte4_log.txt")


if __name__ == "__main__":
    main()
