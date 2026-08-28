"""
Camino A · fase multi-mes -- Paso 15 (LOCAL): validacion estadistica,
Problema 2, Parte 3 (comparabilidad entre G_bruto y G_lift), sin los modelos
nulos de la Parte 2 -- se hace primero por ser lo mas barato y mas decisivo,
segun la propia nota "Que hacer con el resultado" de validacion_estadistica.md.

Cubre de la Parte 3 del encargo:
  - CONTROL DECISIVO: red con el MISMO numero de aristas que G_lift (16.052),
    pero seleccionadas por soporte mas alto en vez de por lift. Si da Q bajo,
    la estructura de G_lift viene del enriquecimiento, no de haber filtrado
    a ese numero de aristas.
  - Sensibilidad al umbral de lift: recalcula nodos/aristas/Q para
    lift>=1.1, 1.2, 1.5, 2.0, 3.0.

NO cubre todavia: el "Q normalizado" (Q_obs - media_Q_nulo)/(Q_max - media_Q_nulo)
pedido en el encargo -- ese requiere la media_Q_nulo de los modelos nulos de
la Parte 2, que no se ha corrido aun. Se deja explicitamente pendiente, no se
inventa un sustituto.

DECISION DISCUTIBLE, declarada: la red de control (seleccionada por soporte)
se pesa con weight=soporte, no weight=lift -- mismo criterio de peso natural
que ya usa G_bruto (peso=soporte) para todo el resto del estudio, y paralelo
a como G_lift usa weight=lift. Asi la comparacion es "seleccionar por X y
pesar por X", simetrica entre control y G_lift.

Requiere: networkx, python-louvain (ya en .venv_redes)
"""

import csv
import json
from pathlib import Path

import networkx as nx
import community as community_louvain

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT_BASE = 1.2
SEED = 42
UMBRALES_LIFT_SENSIBILIDAD = [1.1, 1.2, 1.5, 2.0, 3.0]


def cargar():
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
    return pares, marginales, n_total


def construir_grafo(pares, peso_attr):
    G = nx.Graph()
    for p in pares:
        G.add_edge(p["a"], p["b"], weight=p[peso_attr], soporte=p["soporte"], lift=p["lift"])
    return G


def louvain_Q(G):
    part = community_louvain.best_partition(G, weight="weight", random_state=SEED)
    Q = community_louvain.modularity(part, G, weight="weight")
    return Q, len(set(part.values())), part


def main():
    OUT_DIR.mkdir(exist_ok=True)
    pares, marginales, n_total = cargar()
    print(f"Pares con soporte>={MIN_SOPORTE_PAR}: {len(pares):,} (candidatos totales, universo de G_bruto)")

    # G_lift de referencia (umbral base 1.2) -- para fijar el numero de aristas objetivo
    pares_lift_base = [p for p in pares if p["lift"] >= UMBRAL_LIFT_BASE]
    G_lift = construir_grafo(pares_lift_base, "lift")
    n_aristas_objetivo = G_lift.number_of_edges()
    Q_lift, k_lift, _ = louvain_Q(G_lift)
    print(f"\nG_lift (lift>={UMBRAL_LIFT_BASE}, referencia): {G_lift.number_of_nodes()} nodos, "
          f"{n_aristas_objetivo:,} aristas, Q={Q_lift:.4f}, {k_lift} comunidades")

    # === CONTROL DECISIVO ===
    print(f"\n{'='*70}\nCONTROL DECISIVO: mismas {n_aristas_objetivo:,} aristas, "
          f"seleccionadas por SOPORTE en vez de lift\n{'='*70}")
    pares_por_soporte = sorted(pares, key=lambda p: -p["soporte"])[:n_aristas_objetivo]
    G_control = construir_grafo(pares_por_soporte, "soporte")
    Q_control, k_control, part_control = louvain_Q(G_control)
    print(f"G_control: {G_control.number_of_nodes()} nodos, {G_control.number_of_edges():,} aristas "
          f"(objetivo {n_aristas_objetivo:,}), Q={Q_control:.4f}, {k_control} comunidades")

    soporte_min_control = min(p["soporte"] for p in pares_por_soporte)
    lift_del_control = [p["lift"] for p in pares_por_soporte]
    print(f"Umbral de soporte implicito del control: >= {soporte_min_control:,}")
    print(f"Lift dentro del control -- min={min(lift_del_control):.3f}, "
          f"max={max(lift_del_control):.3f}, mediana={sorted(lift_del_control)[len(lift_del_control)//2]:.3f} "
          f"(si la mayoria ya tiene lift alto, el control no es un contraste limpio -- reportar igual, no ocultar)")

    veredicto = "estructura viene del ENRIQUECIMIENTO (lift), no del recorte a N aristas" if Q_control < 0.5 * Q_lift else \
                "AMBIGUO -- el control también da modularidad alta, la comparación necesita más matices"
    print(f"\nVEREDICTO preliminar (antes de los modelos nulos de la Parte 2): {veredicto}")
    print(f"  Q_lift={Q_lift:.4f} vs. Q_control={Q_control:.4f} "
          f"(razón Q_control/Q_lift = {Q_control/Q_lift:.3f})")

    # === SENSIBILIDAD AL UMBRAL DE LIFT ===
    print(f"\n{'='*70}\nSENSIBILIDAD AL UMBRAL DE LIFT\n{'='*70}")
    filas_sens = []
    for umb in UMBRALES_LIFT_SENSIBILIDAD:
        sub = [p for p in pares if p["lift"] >= umb]
        G_u = construir_grafo(sub, "lift")
        if G_u.number_of_edges() == 0:
            filas_sens.append({"umbral_lift": umb, "nodos": 0, "aristas": 0, "Q": None, "comunidades": None})
            print(f"  lift>={umb}: sin aristas")
            continue
        Q_u, k_u, _ = louvain_Q(G_u)
        filas_sens.append({
            "umbral_lift": umb, "nodos": G_u.number_of_nodes(), "aristas": G_u.number_of_edges(),
            "Q": round(Q_u, 4), "comunidades": k_u,
        })
        print(f"  lift>={umb}: {G_u.number_of_nodes()} nodos, {G_u.number_of_edges():,} aristas, "
              f"Q={Q_u:.4f}, {k_u} comunidades")

    # --- Salidas ---
    with open(OUT_DIR / "parte3_control_decisivo.json", "w", encoding="utf-8") as f:
        json.dump({
            "G_lift_referencia": {"nodos": G_lift.number_of_nodes(), "aristas": n_aristas_objetivo,
                                   "Q": Q_lift, "comunidades": k_lift},
            "G_control_por_soporte": {"nodos": G_control.number_of_nodes(),
                                       "aristas": G_control.number_of_edges(),
                                       "Q": Q_control, "comunidades": k_control,
                                       "soporte_min_incluido": soporte_min_control,
                                       "lift_min_en_control": min(lift_del_control),
                                       "lift_max_en_control": max(lift_del_control)},
            "razon_Q_control_sobre_Q_lift": Q_control / Q_lift,
            "veredicto_preliminar": veredicto,
            "nota": "Q normalizado contra modelos nulos PENDIENTE (Parte 2 no corrida aun)",
        }, f, indent=2, ensure_ascii=False)

    with open(OUT_DIR / "parte3_sensibilidad_lift.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["umbral_lift", "nodos", "aristas", "Q", "comunidades"])
        w.writeheader()
        w.writerows(filas_sens)

    print(f"\nSalidas en {OUT_DIR}/")


if __name__ == "__main__":
    main()
