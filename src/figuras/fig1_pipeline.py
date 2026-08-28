"""
Figura 1 -- Diagrama del pipeline metodologico (should-fix #5). Conceptual,
no depende de datos nuevos: refleja exactamente la Seccion 3 de la v2 (dos
vias independientes -- pipeline secuencial y CPAT en direccion inversa --
convergiendo en el mismo estandar de 5 criterios).
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

BLUE = "#2a78d6"
GREEN = "#008300"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
SURFACE = "#fcfcfb"
BOX_BLUE = "#eaf1fb"
BOX_GREEN = "#e6f2e6"
BOX_FINAL = "#0b0b0b"
GRID = "#e3e2dc"

fig, ax = plt.subplots(figsize=(11.5, 7.2), dpi=200)
fig.patch.set_facecolor(SURFACE)
ax.set_facecolor(SURFACE)
ax.set_xlim(0, 100)
ax.set_ylim(0, 62)
ax.axis("off")

def box(x, y, w, h, text, edge, face, textcolor=TEXT_PRIMARY, fontsize=9.3, weight="normal"):
    b = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.6,rounding_size=3",
                        linewidth=1.6, edgecolor=edge, facecolor=face, zorder=3)
    ax.add_patch(b)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fontsize,
             color=textcolor, weight=weight, zorder=4, wrap=True, linespacing=1.3)
    return (x + w / 2, y, x + w / 2, y + h)  # (cx, ybottom, cx, ytop)

def arrow(x0, y0, x1, y1, color=TEXT_SECONDARY):
    a = FancyArrowPatch((x0, y0), (x1, y1), arrowstyle="-|>", mutation_scale=13,
                          linewidth=1.4, color=color, zorder=2, shrinkA=2, shrinkB=2)
    ax.add_patch(a)

# ---- Carril A: pipeline secuencial (izquierda, azul) ----
xa = 3
wa = 42
ya = [50, 40, 28, 16]
_, _, cxa, ya0_top = box(xa, ya[0], wa, 8, "ClaveÚnica: 544,7M eventos\n14,1M usuarios (2024-2025)",
                          BLUE, BOX_BLUE, fontsize=9)
_, ya1_bot, cxa1, ya1_top = box(xa, ya[1], wa, 8, "Pipeline secuencial: orden y lag\npor par de servicios (87.465 pares, soporte≥50)",
                                  BLUE, BOX_BLUE, fontsize=8.6)
_, ya2_bot, cxa2, ya2_top = box(xa, ya[2], wa, 8, "4 criterios computables: soporte, significancia\n(hipergeométrica), estabilidad trimestral, dirección≥70%",
                                  BLUE, BOX_BLUE, fontsize=8.3)
_, ya3_bot, cxa3, ya3_top = box(xa, ya[3], wa, 8, "5.113 candidatos → criterio 5:\nfuente administrativa verificada caso a caso",
                                  BLUE, BOX_BLUE, fontsize=8.6, weight="bold")
arrow(cxa, ya[0], cxa, ya1_top)
arrow(cxa, ya1_bot, cxa, ya2_top)
arrow(cxa, ya2_bot, cxa, ya3_top)

ax.text(xa + wa / 2, 60, "Vía 1 · Estadística (Sección 3.3)", ha="center", fontsize=10.5,
         color=BLUE, weight="bold")

# ---- Carril B: CPAT en direccion inversa (derecha, verde) ----
xb = 55
wb = 42
yb = ya
_, _, cxb, yb0_top = box(xb, yb[0], wb, 8, "Catálogo CPAT: 2.761 procedimientos\n\"sin interoperabilidad, se solicita a usuarios\"",
                           GREEN, BOX_GREEN, fontsize=8.6)
_, yb1_bot, cxb1, yb1_top = box(xb, yb[1], wb, 8, "651 dependencias interinstitucionales\ndocumentadas oficialmente (Ley N° 21.180)",
                                  GREEN, BOX_GREEN, fontsize=8.6)
_, yb2_bot, cxb2, yb2_top = box(xb, yb[2], wb, 8, "Cruce contra la red de co-uso institucional\n(Sección 4.3) → 469 con correlato conductual",
                                  GREEN, BOX_GREEN, fontsize=8.3)
_, yb3_bot, cxb3, yb3_top = box(xb, yb[3], wb, 8, "3 dependencias satisfacen también\nsoporte, significancia, estabilidad y dirección",
                                  GREEN, BOX_GREEN, fontsize=8.6, weight="bold")
arrow(cxb, yb[0], cxb, yb1_top)
arrow(cxb, yb1_bot, cxb, yb2_top)
arrow(cxb, yb2_bot, cxb, yb3_top)

ax.text(xb + wb / 2, 60, "Vía 2 · CPAT, dirección inversa (Sección 3.4)", ha="center", fontsize=10.5,
         color=GREEN, weight="bold")

# ---- Convergencia ----
xf, yf, wf, hf = 22, 2, 56, 9
final_cx = xf + wf / 2
box(xf, yf, wf, hf, "12 pathways confirmados (Tabla 1)\n7 sin reservas + 5 con salvedad explícita",
    BOX_FINAL, BOX_FINAL, textcolor="#ffffff", fontsize=10, weight="bold")

arrow(cxa3, ya3_bot, final_cx - 8, yf + hf, color=BLUE)
arrow(cxb3, yb3_bot, final_cx + 8, yf + hf, color=GREEN)

ax.text(final_cx, yf - 2.2,
         "Convergencia real, no buscada: DIRECTEMAR→SERNAPESCA se identificó de forma\n"
         "independiente por ambas vías (Sección 4.4) — la evidencia más fuerte del estudio.",
         ha="center", va="top", fontsize=8, color=TEXT_SECONDARY, style="italic")

ax.set_title("Pipeline metodológico: dos vías independientes, un mismo estándar de verificación",
              fontsize=13, color=TEXT_PRIMARY, loc="left", pad=6, fontweight="bold", x=0.0)

plt.tight_layout()
plt.savefig("/home/crist/eclat/textos/figuras/fig1_pipeline.png", facecolor=SURFACE, bbox_inches="tight")
print("OK: fig1_pipeline.png")
