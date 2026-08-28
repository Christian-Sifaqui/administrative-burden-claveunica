"""
Figura 2 del articulo -- Timeline de pathways con lags (should-fix #5 de
segunda_revision.md). OJO: el nombre de este archivo (fig3) no corresponde al
numero de figura en el articulo (Figura 2); fig2_hub_vs_puente.py genera la
Figura 3. Se conservan los nombres de archivo por compatibilidad con las
referencias ya existentes.

Datos: Tabla 1 del articulo (verificados, no recalculados en este script).
Un solo color categorico (slot 1 del palette de referencia), sin leyenda
(una sola serie). Rango para los cuatro destinos de ACEPTA, cada uno con su
propio rango de lag segun la Tabla 1; punto para el resto. Los 3 pathways
CPAT sin lag computado se anotan explicitamente, no se omiten en silencio.

ACTUALIZACION 2026-08-08: ACEPTA pasa de una barra agregada de 8 destinos
(89-235 d, cifras de la version anterior de la Tabla 1) a cuatro barras
separadas, una por destino verificado, tras la auditoria que redujo los
destinos de ocho a cuatro. El extremo inferior del grafico pasa a ser
ACEPTA-SII (84 d) y el superior ACEPTA-Poder Judicial (508 d).
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

BLUE = "#2a78d6"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"
SURFACE = "#fcfcfb"

# (etiqueta, lag_min, lag_max, es_rango)
datos = [
    ("ChileCompra → Subsec. Economía", 257, 257, False),
    ("MDS → FOSIS", 216, 216, False),
    ("RSH → FUAS", 185, 185, False),
    ("RSH → Sercotec", 178, 178, False),
    ("RSH → MINVU", 174, 174, False),
    ("RSH → Subsidio Familiar Automático", 163, 163, False),
    ("ACEPTA → Poder Judicial", 145, 508, True),
    ("ACEPTA → SII", 84, 424, True),
    ("ACEPTA → Dirección del Trabajo", 158, 176, True),
    ("ACEPTA → CMF", 147, 155, True),
    ("AFC → Bolsa Nacional de Empleo", 124, 124, False),
    ("Milicenciamedica.cl → SUSESO", 87, 87, False),
]
# orden descendente por el valor mas alto de cada uno
datos.sort(key=lambda d: d[2], reverse=True)

no_computado = [
    "ChileCompra → Dir. Contabilidad y Finanzas",
    "DIRECTEMAR → SERNAPESCA",
    "Gendarmería → DIPRECA",
]

fig, ax = plt.subplots(figsize=(9, 6.2), dpi=200)
fig.patch.set_facecolor(SURFACE)
ax.set_facecolor(SURFACE)

y_labels = [d[0] for d in datos] + [f"{n} *" for n in no_computado]
y_pos = list(range(len(y_labels)))[::-1]

# barras de rango / puntos para los 9 con dato
for i, (label, lo, hi, es_rango) in enumerate(datos):
    y = y_pos[i]
    if es_rango:
        ax.plot([lo, hi], [y, y], color=BLUE, linewidth=3, solid_capstyle="round", zorder=2)
        ax.plot([lo, hi], [y, y], "o", color=BLUE, markersize=6, zorder=3)
        ax.text(hi + 6, y, f"{lo}–{hi} d", va="center", ha="left",
                 fontsize=9.5, color=TEXT_SECONDARY)
    else:
        ax.plot([0, hi], [y, y], color=GRID, linewidth=1, zorder=1)
        ax.plot([hi], [y], "o", color=BLUE, markersize=8, zorder=3)
        ax.text(hi + 6, y, f"{hi} d", va="center", ha="left",
                 fontsize=9.5, color=TEXT_SECONDARY)

# filas "no computado" -- mismo eje, marcador hueco + nota
for j, n in enumerate(no_computado):
    y = y_pos[len(datos) + j]
    ax.plot([0], [y], "o", markerfacecolor="none", markeredgecolor=TEXT_SECONDARY,
             markersize=7, zorder=3)
    ax.text(6, y, "no computado (validado a nivel institución, Sección 3.4)",
             va="center", ha="left", fontsize=8.7, color=TEXT_SECONDARY, style="italic")

ax.set_yticks(y_pos)
ax.set_yticklabels(y_labels, fontsize=9.8, color=TEXT_PRIMARY)
ax.set_xlim(-2, 560)
ax.set_xlabel("Lag mediano entre el primer y el segundo trámite (días)", fontsize=10, color=TEXT_SECONDARY)
ax.set_xticks([0, 100, 200, 300, 400, 500])
ax.tick_params(axis="x", colors=TEXT_SECONDARY, labelsize=9)
ax.tick_params(axis="y", length=0)

for spine in ["top", "right", "left"]:
    ax.spines[spine].set_visible(False)
ax.spines["bottom"].set_color(GRID)
ax.xaxis.grid(True, color=GRID, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)

ax.set_title("Lag mediano de los pathways confirmados (Tabla 1)",
              fontsize=12.5, color=TEXT_PRIMARY, loc="left", pad=14, fontweight="bold")
fig.text(0.01, 0.005,
          "* Los 3 pathways confirmados vía CPAT se validaron por co-uso institucional (Sección 3.4), no por el pipeline\n"
          "secuencial por persona que calcula lag (Sección 3.3) -- no computado no equivale a lag cero.",
          fontsize=7.6, color=TEXT_SECONDARY, ha="left")

plt.tight_layout(rect=[0, 0.05, 1, 1])
plt.savefig("/home/crist/eclat/textos/figuras/fig3_timeline_lags.png", facecolor=SURFACE, bbox_inches="tight")
print("OK: fig3_timeline_lags.png")
