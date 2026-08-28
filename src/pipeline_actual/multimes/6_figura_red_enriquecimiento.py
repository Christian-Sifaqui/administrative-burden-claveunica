"""
Camino A · fase multi-mes -- Paso 6: figura de la red de enriquecimiento (ALERTA B2)
=====================================================================================
Genera la figura que faltaba para la Seccion 6.3: un diagrama de la red de
enriquecimiento (lift>=1,2) que muestre visualmente la distincion hub-vs-puente
y las 12 comunidades ya reportadas solo en prosa/Tabla A7.1.

Reutiliza exactamente la misma construccion de grafo que 4_analisis_red_local.py
(mismos umbrales MIN_SOPORTE_PAR=50, UMBRAL_LIFT=1.2) sobre los mismos dos CSV
ya materializados -- ningun dato nuevo, solo una capa de visualizacion sobre un
calculo que ya existia y ya esta reportado en el texto.

Layout: la red tiene 330 nodos y 16.052 aristas (densidad 0,296) -- un
spring_layout global sobre todo el grafo simplemente no separa visualmente las
comunidades (probado: da una maraña con las 12 comunidades apenas distinguibles
por color, sin espacio en blanco donde debería). Se usa en cambio un layout
"consciente de comunidad", tecnica estandar: (1) las 12 comunidades se ubican
como centroides en un circulo via spring_layout sobre el grafo-resumen
(super-nodo = comunidad, peso = aristas inter-comunidad); (2) dentro de cada
comunidad, los nodos se ubican con spring_layout sobre el subgrafo inducido
(solo aristas intra-comunidad) y se trasladan al centroide de su comunidad.
Esto no cambia ninguna metrica ya reportada (fuerza, betweenness, modularidad
siguen calculandose sobre el grafo completo, sin filtrar) -- solo determina
donde se dibuja cada nodo.

Decisiones de diseño (ver skill de dataviz del proyecto):
  - Identidad NUNCA solo por color: tamaño de nodo = fuerza (hub), anillo negro
    = top-6 por intermediacion (puente) -- dos codificaciones independientes,
    exactamente la distincion que el hallazgo de 6.3 necesita mostrar.
  - De las 12 comunidades, se resaltan con color solo las 6 mas interpretables
    y ya nombradas en el texto (tribunales electorales, ambiental, fuerzas
    armadas, municipios del sur, tribunales/organismos individuales, y el
    cluster de coordinacion/probidad donde caen los puentes reales); el resto
    (incluida la comunidad 0, de 202 miembros sin tema comun) va a un gris
    neutro "otras". Paleta validada con scripts/validate_palette.js del skill
    (6 tonos, modo claro, todos los checks duros PASS; el WARN de contraste
    exige etiquetas visibles, que ya se usan aqui via leyenda + labels).
  - Aristas casi invisibles (alpha bajo): con 16.052 aristas dibujarlas con
    fuerza visual las volveria una maraña sin informacion adicional -- la
    separacion por comunidad ya es la señal real.
  - Sin modo oscuro: es una figura estatica embebida como PNG en un documento,
    no un artifact HTML con theming en vivo -- incoherente con la logica de
    doble-tema del skill, que aplica a paginas web interactivas. Se sigue la
    misma convencion de las otras 5 figuras ya en el paper (fondo blanco).

Requiere: pip install networkx python-louvain matplotlib (venv .venv_redes)
"""

import base64
import csv
import math
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import community as community_louvain

DATA_DIR = Path("output_multimes")
MARGINALES_CSV = DATA_DIR / "red_institucional_marginales.csv"
PARES_CSV = DATA_DIR / "red_institucional_pares.csv"
OUT_PNG = DATA_DIR / "figura_red_enriquecimiento.png"
OUT_B64 = DATA_DIR / "figura_red_enriquecimiento_base64.txt"

MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
TOP_N_ETIQUETAS = 5  # cuantos hubs y cuantos puentes se etiquetan con texto

# Comunidades a resaltar con color propio -- elegidas por ser las mismas ya
# nombradas en la prosa de la Seccion 6.3, no una selección nueva. El resto
# (incluida la comunidad 0, la más grande y sin tema común) va a "otras".
COMUNIDADES_DESTACADAS = {
    6: "Tribunales electorales",
    7: "Clúster ambiental",
    3: "Fuerzas armadas",
    2: "Municipios sur de Chile",
    1: "Tribunales/organismos individuales",
    9: "Coordinación y probidad (SEGPRES, Contraloría, etc.)",
}
# Paleta categórica validada (scripts/validate_palette.js, 6 tonos, modo claro:
# todos los checks duros PASS). Mismo orden fijo que el resto del proyecto usaría.
COLORES = ["#2a78d6", "#008300", "#e87ba4", "#eda100", "#1baf7a", "#eb6834"]
GRIS_OTRAS = "#c9c8bd"


def cargar_marginales():
    marginales = {}
    n_total = None
    with open(MARGINALES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])
    return marginales, n_total


def cargar_pares():
    with open(PARES_CSV, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def lift(soporte, ma, mb, n_total):
    esperado = (ma * mb) / n_total
    return soporte / esperado if esperado > 0 else float("nan")


def construir_grafo_enriquecimiento(pares, marginales, n_total):
    G = nx.Graph()
    for p in pares:
        soporte = int(p["soporte"])
        if soporte < MIN_SOPORTE_PAR:
            continue
        a, b = p["inst_a"], p["inst_b"]
        ma, mb = marginales.get(a, 0), marginales.get(b, 0)
        lft = lift(soporte, ma, mb, n_total)
        if lft >= UMBRAL_LIFT:
            G.add_edge(a, b, weight=lft, soporte=soporte, lift=lft)
    return G


def nombre_corto(inst: str, max_len: int = 26) -> str:
    inst = inst.title()
    return inst if len(inst) <= max_len else inst[: max_len - 1] + "…"


def layout_por_comunidad(G, particion, grupos):
    """Layout 'consciente de comunidad': centroides de comunidad en circulo
    (via spring_layout sobre el grafo-resumen ponderado por aristas inter-
    comunidad), nodos dentro de cada comunidad via spring_layout local sobre
    el subgrafo inducido, trasladado al centroide. Separa visualmente las 12
    comunidades pese a la alta densidad global (0,296) que hace inutil un
    spring_layout global directo."""
    # Grafo-resumen: super-nodo = comunidad, peso = suma de pesos inter-comunidad.
    G_resumen = nx.Graph()
    G_resumen.add_nodes_from(grupos.keys())
    for a, b, d in G.edges(data=True):
        ca, cb = particion[a], particion[b]
        if ca != cb:
            if G_resumen.has_edge(ca, cb):
                G_resumen[ca][cb]["weight"] += d["weight"]
            else:
                G_resumen.add_edge(ca, cb, weight=d["weight"])

    tam = {c: len(m) for c, m in grupos.items()}
    pos_resumen = nx.spring_layout(G_resumen, weight="weight", seed=7, k=2.4, iterations=300)
    # Radio de dispersion escalado por tamaño de red total, no arbitrario.
    escala_centroide = 9.0
    centroides = {c: (x * escala_centroide, y * escala_centroide) for c, (x, y) in pos_resumen.items()}

    pos_final = {}
    for c, miembros in grupos.items():
        sub = G.subgraph(miembros)
        cx, cy = centroides[c]
        # Radio local proporcional a sqrt(tamaño) -- comunidades grandes ocupan
        # mas espacio, pero no linealmente (si no, la comunidad 0 con 202
        # miembros dominaria el lienzo entero).
        radio_local = 0.55 * math.sqrt(tam[c]) + 0.3
        if sub.number_of_edges() > 0:
            pos_sub = nx.spring_layout(sub, weight="weight", seed=3, k=1.3 / math.sqrt(max(len(miembros), 2)), iterations=150)
        else:
            pos_sub = {n: (0.0, 0.0) for n in miembros}
        # Normalizar el subgrafo a radio unitario antes de escalar, para que
        # radio_local sea comparable entre comunidades de distinta densidad interna.
        xs = [p[0] for p in pos_sub.values()]
        ys = [p[1] for p in pos_sub.values()]
        max_ext = max(max(abs(min(xs)), abs(max(xs))) if xs else 1e-6,
                       max(abs(min(ys)), abs(max(ys))) if ys else 1e-6, 1e-6)
        for n, (x, y) in pos_sub.items():
            pos_final[n] = (cx + (x / max_ext) * radio_local, cy + (y / max_ext) * radio_local)
    return pos_final


def main():
    marginales, n_total = cargar_marginales()
    pares = cargar_pares()
    G = construir_grafo_enriquecimiento(pares, marginales, n_total)
    print(f"Red de enriquecimiento: {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")

    for _, _, d in G.edges(data=True):
        d["distancia"] = 1.0 / math.log1p(d["weight"]) if d["weight"] > 0 else float("inf")

    fuerza = dict(G.degree(weight="weight"))
    print("Calculando betweenness (misma métrica que 4_analisis_red_local.py)...")
    btw = nx.betweenness_centrality(G, weight="distancia", normalized=True)

    print("Detectando comunidades (Louvain, random_state=42, igual que 4_analisis_red_local.py)...")
    particion = community_louvain.best_partition(G, weight="weight", random_state=42)
    grupos = defaultdict(list)
    for inst, c in particion.items():
        grupos[c].append(inst)

    top_hubs = sorted(fuerza.items(), key=lambda x: -x[1])[:TOP_N_ETIQUETAS]
    top_puentes = sorted(btw.items(), key=lambda x: -x[1])[:TOP_N_ETIQUETAS]
    print("\nTop hubs (fuerza) en la red de enriquecimiento:")
    for inst, f in top_hubs:
        print(f"  {inst:<50} fuerza={f:.1f}  comunidad={particion[inst]}")
    print("\nTop puentes (betweenness) en la red de enriquecimiento:")
    for inst, b in top_puentes:
        print(f"  {inst:<50} betweenness={b:.5f}  comunidad={particion[inst]}")
    top_hubs = set(i for i, _ in top_hubs)
    top_puentes = set(i for i, _ in top_puentes)

    print("\nCalculando layout consciente de comunidad...")
    pos = layout_por_comunidad(G, particion, grupos)

    fig, ax = plt.subplots(figsize=(13, 11), dpi=150)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    nx.draw_networkx_edges(G, pos, ax=ax, width=0.2, alpha=0.04, edge_color="#888888")

    node_color, node_size, node_edgecolor, node_linewidth = [], [], [], []
    for inst in G.nodes():
        c = particion[inst]
        node_color.append(COLORES[list(COMUNIDADES_DESTACADAS).index(c)] if c in COMUNIDADES_DESTACADAS else GRIS_OTRAS)
        node_size.append(50 + 30 * math.sqrt(fuerza.get(inst, 0)))
        if inst in top_puentes:
            node_edgecolor.append("black")
            node_linewidth.append(2.2)
        else:
            node_edgecolor.append("white")
            node_linewidth.append(0.3)

    nx.draw_networkx_nodes(
        G, pos, ax=ax, node_color=node_color, node_size=node_size,
        edgecolors=node_edgecolor, linewidths=node_linewidth, alpha=0.9,
    )

    bbox_hub = dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="none", alpha=0.8)
    bbox_puente = dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="none", alpha=0.8)
    # Offset horizontal alternante ademas del vertical -- reduce choques entre
    # etiquetas de nodos geometricamente cercanos (ej. los 3 tribunales
    # ambientales, o los 5 puentes que caen todos en el mismo cluster de
    # coordinacion) sin depender de una libreria externa de anti-colision.
    for idx, inst in enumerate(sorted(top_hubs, key=lambda i: pos[i][1], reverse=True)):
        x, y = pos[inst]
        dx = [-75, 75, -75, 75, 0][idx % 5]
        dy = [14, 14, 34, 34, 54][idx % 5]
        ax.annotate(nombre_corto(inst), (x, y), xytext=(dx, dy), textcoords="offset points",
                    fontsize=8, color="#0b2545", ha="center", va="bottom", zorder=5, bbox=bbox_hub)
    for idx, inst in enumerate(sorted(top_puentes, key=lambda i: pos[i][1])):
        x, y = pos[inst]
        dx = [75, -75, 75, -75, 0][idx % 5]
        dy = [-14, -14, -34, -34, -54][idx % 5]
        ax.annotate(nombre_corto(inst), (x, y), xytext=(dx, dy), textcoords="offset points",
                    fontsize=8, color="#8a1414", fontweight="bold", ha="center", va="top",
                    zorder=5, bbox=bbox_puente)

    handles = []
    for i, (cid, etiqueta) in enumerate(COMUNIDADES_DESTACADAS.items()):
        handles.append(plt.Line2D([0], [0], marker="o", linestyle="", color=COLORES[i],
                                   markersize=9, label=f"{etiqueta} ({len(grupos[cid])})"))
    handles.append(plt.Line2D([0], [0], marker="o", linestyle="", color=GRIS_OTRAS,
                               markersize=9, label="Otras comunidades"))
    handles.append(plt.Line2D([0], [0], marker="o", linestyle="", markerfacecolor="white",
                               markeredgecolor="black", markeredgewidth=2, markersize=9,
                               label=f"Top-{TOP_N_ETIQUETAS} puente (betweenness)"))
    handles.append(plt.Line2D([0], [0], marker="o", linestyle="", color="#999999",
                               markersize=14, label="Tamaño = fuerza (hub)"))
    ax.legend(handles=handles, loc="upper left", fontsize=8, framealpha=0.95, borderpad=1)

    ax.set_title(
        "Red institucional de enriquecimiento (lift ≥ 1,2) — comunidades, hubs y puentes\n"
        f"{G.number_of_nodes()} instituciones, {G.number_of_edges()} pares, "
        "12 comunidades (Louvain, Q=0,548)",
        fontsize=11,
    )
    ax.axis("off")
    ax.set_aspect("equal")
    fig.tight_layout()
    fig.savefig(OUT_PNG, facecolor="white", bbox_inches="tight")
    print(f"\nFigura guardada en {OUT_PNG}")

    with open(OUT_PNG, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    OUT_B64.write_text(b64)
    print(f"Base64 guardado en {OUT_B64} ({len(b64):,} caracteres)")


if __name__ == "__main__":
    main()
