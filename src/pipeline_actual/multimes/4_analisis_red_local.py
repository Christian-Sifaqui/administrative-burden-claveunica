"""
Camino A · fase multi-mes -- Paso 4: analisis de red institucional (LOCAL).

Consume los dos CSV producidos por 3_red_institucional.py
(red_institucional_pares.csv, red_institucional_marginales.csv) y calcula,
por primera vez con datos reales, las medidas que la Seccion 5.4 del paper
prometia: grado, centralidad de intermediacion, densidad y modularidad,
sobre la red institucional completa (no una aproximacion de 20 pares).

Se filtra por MIN_SOPORTE_PAR=50 -- el mismo umbral ya usado en todo el
pipeline secuencial (2b_pares.py, documentado en apendice_metodologico.md
Seccion 3), no un umbral nuevo inventado para este analisis.

Se construyen DOS grafos no dirigidos:
  - G_bruto: todos los pares con soporte >= 50, peso = soporte (red de
    trafico/co-ocurrencia cruda).
  - G_lift: solo pares con lift >= UMBRAL_LIFT (enriquecimiento real sobre
    el azar, mismo baseline hipergeometrico de las Tablas A4.1/A4.2/A5.1/
    A6.1), peso = lift. Operacionaliza directamente la distincion central
    del paper (efecto de tasa base vs. asociacion genuina) con metricas de
    red formales, en vez de solo inspeccionar la tabla de lift a mano.

Y un grafo DIRIGIDO (peso = a_antes_b / b_antes_a) para calcular flujo neto
(fuente vs. sumidero) por institucion.

Requiere: pip install networkx python-louvain (ya instalados en .venv_redes)
"""

import csv
import math
from pathlib import Path

import networkx as nx
import community as community_louvain  # python-louvain

DATA_DIR = Path("output_multimes")
MARGINALES_CSV = DATA_DIR / "red_institucional_marginales.csv"
PARES_CSV = DATA_DIR / "red_institucional_pares.csv"

MIN_SOPORTE_PAR = 50   # mismo umbral que 2b_pares.py / apendice_metodologico.md Seccion 3
UMBRAL_LIFT = 1.2      # "enriquecimiento real pero moderado" -- mismo lenguaje ya usado
                        # en el cuerpo del paper (Seccion 6.4) para lift en ese rango
TOP_N = 15


def cargar_marginales():
    marginales = {}
    n_total = None
    with open(MARGINALES_CSV, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])
    return marginales, n_total


def cargar_pares():
    pares = []
    with open(PARES_CSV, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            pares.append({
                "inst_a": row["inst_a"],
                "inst_b": row["inst_b"],
                "soporte": int(row["soporte"]),
                "a_antes_b": int(row["a_antes_b"]),
                "b_antes_a": int(row["b_antes_a"]),
                "mismo_dia": int(row["mismo_dia"]),
            })
    return pares


def lift(soporte, marginal_a, marginal_b, n_total):
    esperado = (marginal_a * marginal_b) / n_total
    return soporte / esperado if esperado > 0 else float("nan")


def construir_grafos(pares, marginales, n_total):
    pares_filtrados = [p for p in pares if p["soporte"] >= MIN_SOPORTE_PAR]
    print(f"Pares con soporte >= {MIN_SOPORTE_PAR}: {len(pares_filtrados):,} de {len(pares):,}")

    G_bruto = nx.Graph()
    G_lift = nx.Graph()
    G_dirigido = nx.DiGraph()

    n_lift_alto = 0
    for p in pares_filtrados:
        a, b = p["inst_a"], p["inst_b"]
        ma, mb = marginales.get(a, 0), marginales.get(b, 0)
        lft = lift(p["soporte"], ma, mb, n_total)

        G_bruto.add_edge(a, b, weight=p["soporte"], soporte=p["soporte"], lift=lft)

        if lft >= UMBRAL_LIFT:
            G_lift.add_edge(a, b, weight=lft, soporte=p["soporte"], lift=lft)
            n_lift_alto += 1

        if p["a_antes_b"] > 0:
            G_dirigido.add_edge(a, b, weight=p["a_antes_b"])
        if p["b_antes_a"] > 0:
            G_dirigido.add_edge(b, a, weight=p["b_antes_a"])

    print(f"Pares con lift >= {UMBRAL_LIFT}: {n_lift_alto:,}")
    return G_bruto, G_lift, G_dirigido


def distancia_desde_peso(G):
    """networkx trata 'weight' como distancia (mas chico = mas cercano) para
    shortest-path/betweenness -- pero nuestro peso es fuerza de vinculo (mas
    grande = mas fuerte). Se agrega un atributo 'distancia' = 1/log1p(peso)
    para que pares de mayor soporte/lift sean 'caminos mas cortos'."""
    for _, _, d in G.edges(data=True):
        d["distancia"] = 1.0 / math.log1p(d["weight"]) if d["weight"] > 0 else float("inf")


def reportar_red(nombre, G, marginales):
    print(f"\n{'='*70}\n{nombre}\n{'='*70}")
    n_nodos = G.number_of_nodes()
    n_aristas = G.number_of_edges()
    densidad = nx.density(G)
    print(f"Nodos: {n_nodos:,} | Aristas: {n_aristas:,} | Densidad: {densidad:.4f}")

    componentes = list(nx.connected_components(G))
    print(f"Componentes conexas: {len(componentes)} "
          f"(la mayor: {len(max(componentes, key=len)):,} nodos)")

    distancia_desde_peso(G)

    fuerza = dict(G.degree(weight="weight"))
    grado_simple = dict(G.degree())

    print(f"\nTop {TOP_N} por FUERZA (grado ponderado, suma de pesos de aristas):")
    for inst, f in sorted(fuerza.items(), key=lambda x: -x[1])[:TOP_N]:
        print(f"  {inst:<55} fuerza={f:>14,.1f}  grado={grado_simple[inst]:>4}  "
              f"marginal={marginales.get(inst, 0):>11,}")

    print(f"\nCalculando betweenness ponderada (puede tardar unos minutos, {n_nodos} nodos)...")
    btw = nx.betweenness_centrality(G, weight="distancia", normalized=True)
    print(f"Top {TOP_N} por CENTRALIDAD DE INTERMEDIACION (betweenness):")
    for inst, b in sorted(btw.items(), key=lambda x: -x[1])[:TOP_N]:
        print(f"  {inst:<55} betweenness={b:>10.5f}  grado={grado_simple[inst]:>4}  "
              f"marginal={marginales.get(inst, 0):>11,}")

    print("\nDetectando comunidades (Louvain)...")
    particion = community_louvain.best_partition(G, weight="weight", random_state=42)
    modularidad = community_louvain.modularity(particion, G, weight="weight")
    n_comunidades = len(set(particion.values()))
    print(f"Comunidades detectadas: {n_comunidades} | Modularidad: {modularidad:.4f}")

    from collections import defaultdict
    grupos = defaultdict(list)
    for inst, c in particion.items():
        grupos[c].append(inst)
    for c, miembros in sorted(grupos.items(), key=lambda x: -len(x[1]))[:8]:
        miembros_ordenados = sorted(miembros, key=lambda i: -fuerza.get(i, 0))[:8]
        print(f"  Comunidad {c} ({len(miembros)} instituciones) -- top por fuerza: "
              f"{', '.join(miembros_ordenados)}")

    return {
        "n_nodos": n_nodos, "n_aristas": n_aristas, "densidad": densidad,
        "n_componentes": len(componentes),
        "fuerza": fuerza, "grado": grado_simple, "betweenness": btw,
        "particion": particion, "modularidad": modularidad, "n_comunidades": n_comunidades,
    }


def reportar_dirigido(G, marginales):
    print(f"\n{'='*70}\nRED DIRIGIDA -- flujo neto por institucion\n{'='*70}")
    print(f"Nodos: {G.number_of_nodes():,} | Aristas dirigidas: {G.number_of_edges():,}")

    out_strength = dict(G.out_degree(weight="weight"))
    in_strength = dict(G.in_degree(weight="weight"))
    todas = set(out_strength) | set(in_strength)
    neto = {i: out_strength.get(i, 0) - in_strength.get(i, 0) for i in todas}

    print(f"\nTop {TOP_N} FUENTES (mayor flujo neto saliente -- 'primer contacto'):")
    for inst, v in sorted(neto.items(), key=lambda x: -x[1])[:TOP_N]:
        print(f"  {inst:<55} neto={v:>+14,.0f}  marginal={marginales.get(inst,0):>11,}")

    print(f"\nTop {TOP_N} SUMIDEROS (mayor flujo neto entrante -- 'destino'):")
    for inst, v in sorted(neto.items(), key=lambda x: x[1])[:TOP_N]:
        print(f"  {inst:<55} neto={v:>+14,.0f}  marginal={marginales.get(inst,0):>11,}")


def main():
    marginales, n_total = cargar_marginales()
    pares = cargar_pares()
    print(f"Instituciones con marginal: {len(marginales):,} | N total: {n_total:,}")

    G_bruto, G_lift, G_dirigido = construir_grafos(pares, marginales, n_total)

    res_bruto = reportar_red(
        f"GRAFO BRUTO (peso=soporte, todos los pares soporte>={MIN_SOPORTE_PAR})",
        G_bruto, marginales,
    )
    res_lift = reportar_red(
        f"GRAFO DE ENRIQUECIMIENTO (peso=lift, solo pares lift>={UMBRAL_LIFT})",
        G_lift, marginales,
    )
    reportar_dirigido(G_dirigido, marginales)

    print(f"\n{'='*70}\nRESUMEN COMPARATIVO\n{'='*70}")
    print(f"Bruto:      {res_bruto['n_nodos']} nodos, {res_bruto['n_aristas']} aristas, "
          f"densidad={res_bruto['densidad']:.4f}, modularidad={res_bruto['modularidad']:.4f}, "
          f"{res_bruto['n_comunidades']} comunidades")
    print(f"Enriquecido: {res_lift['n_nodos']} nodos, {res_lift['n_aristas']} aristas, "
          f"densidad={res_lift['densidad']:.4f}, modularidad={res_lift['modularidad']:.4f}, "
          f"{res_lift['n_comunidades']} comunidades")


if __name__ == "__main__":
    main()
