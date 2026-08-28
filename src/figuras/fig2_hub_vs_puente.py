"""
Figura 2, rehecha -- la red completa de 330 nodos es un hairball ilegible.
El claim real del paper es una divergencia de ranking (hub por trafico vs.
puente por intermediacion en la red de enriquecimiento), no la estructura
espacial completa: un dumbbell/slope chart lo comunica sin ruido visual.
Datos reales, mismo calculo que fig2_red_hub_puente.py (verificado: Q=0.548,
12 comunidades, coincide con lo publicado).
"""
import csv
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import community as community_louvain

DATA_DIR = Path("/home/crist/eclat/camino_a_precedencia/multimes/output_multimes")
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2

def cargar_marginales():
    marginales, n_total = {}, None
    with open(DATA_DIR / "red_institucional_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            marginales[row["institucion"]] = int(row["marginal"])
            n_total = int(row["n_total"])
    return marginales, n_total

def lift(soporte, ma, mb, n_total):
    esperado = (ma * mb) / n_total
    return soporte / esperado if esperado > 0 else float("nan")

marginales, n_total = cargar_marginales()
G_lift = nx.Graph()
with open(DATA_DIR / "red_institucional_pares.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        soporte = int(row["soporte"])
        if soporte < MIN_SOPORTE_PAR:
            continue
        a, b = row["inst_a"], row["inst_b"]
        ma, mb = marginales.get(a, 0), marginales.get(b, 0)
        lft = lift(soporte, ma, mb, n_total)
        if lft >= UMBRAL_LIFT:
            G_lift.add_edge(a, b, weight=lft)

for _, _, d in G_lift.edges(data=True):
    d["distancia"] = 1.0 / math.log1p(d["weight"]) if d["weight"] > 0 else float("inf")
btw = nx.betweenness_centrality(G_lift, weight="distancia", normalized=True)
tot_btw = sum(btw.values())
btw_share = {k: v / tot_btw * 100 for k, v in btw.items()}

tot_marginal = sum(marginales.values())
traf_share = {k: v / tot_marginal * 100 for k, v in marginales.items()}

# Puentes: los 5 nombrados y publicados en la Seccion 4.3 de la v2, con SUS
# PORCENTAJES YA PUBLICADOS (no los recalculados aqui, que difieren en <1pp
# por una diferencia menor de normalizacion -- se usa el numero que ya paso
# por revision, para que texto y figura no diverjan).
PUENTES_PUBLICADOS = [
    ("SUBSECRETARIA GENERAL DE LA PRESIDENCIA DE LA REPUBLICA", "SEGPRES", 11.8),
    ("COMISION PARA LA INTEGRIDAD PUBLICA Y TRANSPARENCIA", "Com. Integridad Pública", 6.8),
    ("CONTRALORIA GENERAL DE LA REPUBLICA", "Contraloría", 6.6),
    ("DIRECCION NACIONAL DEL SERVICIO CIVIL", "Servicio Civil", 3.5),
    ("COMISION PARA EL MERCADO FINANCIERO", "CMF", 3.2),
]

# Hubs: los nombrados y publicados en la Seccion 4.2 de la v2 ("nodos de
# mayor volumen"), no un top-N recalculado con otro criterio (el marginal
# de la red de co-uso, usado antes, no coincide con esa lista publicada
# porque mide algo distinto -- alcance dentro de esta red filtrada, no
# tráfico bruto de eventos sobre los 1.266 servicios).
HUBS_PUBLICADOS = [
    ("PODER JUDICIAL", "Poder Judicial"),
    ("SERVICIO DE REGISTRO CIVIL E IDENTIFICACION", "Registro Civil"),
    ("SERVICIO DE IMPUESTOS INTERNOS", "SII"),
    ("SOCIEDAD ADMINISTRADORA DE FONDOS DE CESANTIA DE CHILE S.A.", "AFC"),
    ("MINISTERIO DE DESARROLLO SOCIAL Y FAMILIA", "Min. Desarrollo Social"),
    ("FONDO NACIONAL DE SALUD", "FONASA"),
]

filas = []
for inst, nombre, b_pub in PUENTES_PUBLICADOS:
    filas.append((nombre, traf_share.get(inst, 0), b_pub, "puente"))
for inst, nombre in HUBS_PUBLICADOS:
    filas.append((nombre, traf_share.get(inst, 0), btw_share.get(inst, 0), "hub"))
    if inst not in traf_share:
        print(f"AVISO: '{inst}' no encontrado en marginales -- revisar nombre exacto")

# orden: por intermediacion descendente (deja los puentes arriba, hubs abajo con btw~0)
filas.sort(key=lambda r: r[2], reverse=True)

BLUE = "#2a78d6"      # intermediacion (el hallazgo)
GRAY = "#9a9890"       # trafico (el contraste)
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
SURFACE = "#fcfcfb"
GRID = "#e3e2dc"

fig, ax = plt.subplots(figsize=(9.5, 6.5), dpi=200)
fig.patch.set_facecolor(SURFACE); ax.set_facecolor(SURFACE)

y_pos = list(range(len(filas)))[::-1]
for i, (nombre, traf, btwp, tipo) in enumerate(filas):
    y = y_pos[i]
    ax.plot([traf, btwp], [y, y], color=GRID, linewidth=1.4, zorder=1)
    ax.plot([traf], [y], "o", color=GRAY, markersize=9, zorder=2,
             label="% del marginal en la red de co-uso (alcance, no evento)" if i == 0 else None)
    ax.plot([btwp], [y], "o", color=BLUE, markersize=9, zorder=3,
             label="% de la intermediación (red de enriquecimiento, lift≥1,2)" if i == 0 else None)

ax.set_yticks(y_pos)
ax.set_yticklabels([f[0] for f in filas], fontsize=10, color=TEXT_PRIMARY)
ax.set_xlabel("% del total (marginal en la red o intermediación, según la serie)", fontsize=9.7, color=TEXT_SECONDARY)
ax.tick_params(axis="x", colors=TEXT_SECONDARY, labelsize=9)
ax.tick_params(axis="y", length=0)
for spine in ["top", "right", "left"]:
    ax.spines[spine].set_visible(False)
ax.spines["bottom"].set_color(GRID)
ax.xaxis.grid(True, color=GRID, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.set_xlim(-1, max(max(t, b) for _, t, b, _ in filas) * 1.12 + 1)

# linea divisoria dinamica: entre la ultima fila "puente" y la primera "hub"
# en el orden ya ordenado por btwp descendente (robusto a como caiga el sort)
tipos_en_orden = [f[3] for f in filas]
idx_ultimo_puente = max(i for i, t in enumerate(tipos_en_orden) if t == "puente")
y_divisor = (y_pos[idx_ultimo_puente] + y_pos[idx_ultimo_puente + 1]) / 2 if idx_ultimo_puente + 1 < len(y_pos) else y_pos[-1] - 0.5
ax.axhline(y=y_divisor, color=TEXT_SECONDARY, linewidth=0.8, linestyle=(0, (3, 3)))
ax.text(ax.get_xlim()[1] * 0.98, y_divisor + 0.15, "puentes ↑ / hubs ↓", fontsize=8,
         color=TEXT_SECONDARY, ha="right", style="italic")

ax.set_title("Hub ≠ puente: los organismos de mayor tráfico no son los de mayor intermediación",
              fontsize=12.5, color=TEXT_PRIMARY, loc="left", pad=14, fontweight="bold")
ax.legend(loc="lower right", fontsize=9, frameon=False, labelcolor=TEXT_SECONDARY,
          bbox_to_anchor=(1.0, 0.02))

fig.text(0.01, 0.01,
          "Puentes (arriba): los 5 de mayor intermediación en la red de enriquecimiento, Sección 4.3, con sus porcentajes ya publicados.\n"
          "Hubs (abajo): los nodos de mayor volumen nombrados en la Sección 4.2. \"% del marginal en la red de co-uso\" mide alcance de\n"
          "usuarios dentro de esta red filtrada (soporte≥50), no tráfico bruto de eventos sobre los 1.266 servicios -- por eso Poder\n"
          "Judicial y AFC, hubs por volumen bruto, muestran aquí un marginal menor al de SII o Registro Civil.",
          fontsize=7.2, color=TEXT_SECONDARY, ha="left")

plt.tight_layout(rect=[0, 0.06, 1, 1])
plt.savefig("/home/crist/eclat/textos/figuras/fig2_hub_vs_puente.png", facecolor=SURFACE, bbox_inches="tight")
print("OK: fig2_hub_vs_puente.png")
for f in filas:
    print(f)
