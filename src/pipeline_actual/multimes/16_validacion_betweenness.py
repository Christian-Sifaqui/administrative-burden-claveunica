"""
Camino A · fase multi-mes -- Paso 16 (LOCAL): validacion estadistica,
Problema 2b -- doble (en realidad triple) verificacion de la betweenness
publicada en la Seccion 4.3 del paper, sobre G_lift ("RED B",
enriquecimiento, lift>=1.2, 330 nodos, 16.052 aristas).

ADVERTENCIA TECNICA DEL ENCARGO: en NetworkX, betweenness_centrality trata
'weight' como DISTANCIA (mas chico = camino mas corto/preferido), mientras
que constraint/effective_size lo tratan como FUERZA del vinculo (mas grande
= mas fuerte). Son interpretaciones opuestas. Si se hubiera pasado el lift
crudo como 'weight' a betweenness_centrality, los caminos mas cortos habrian
preferido aristas de MENOR lift -- lo contrario de lo buscado.

Se calculan TRES variantes sobre la misma red, para poder verificar cual
corresponde a los porcentajes ya publicados (SEGPRES 11,8%, Comision para la
Integridad Publica 6,8%, Contraloria 6,6%, Servicio Civil 3,5%, CMF 3,2%):
  A) weight="distancia" = 1/log1p(lift) -- la que USO REALMENTE
     4_analisis_red_local.py (el script original que genero los numeros
     publicados). Interpretacion correcta: mayor lift -> distancia menor.
  B) weight="lift" crudo -- LA VERSION CON EL BUG que advierte el encargo.
     Si esta coincidiera con los porcentajes publicados en vez de la A,
     significaria que el paper tiene el error que se estaba buscando.
  C) weight = 1/lift -- la transformacion que el propio prompt del encargo
     sugiere como default ("transforma el peso a distancia (1/peso)"),
     distinta de la A (que usa 1/log1p(peso) para comprimir el rango
     extremo de lift, 1.2 a ~5.275). Sirve para ver si la eleccion
     especifica de compresion logaritmica cambia el ranking o no.

Requiere: networkx (ya en .venv_redes)
"""

import csv
import json
import math
from pathlib import Path

import networkx as nx

DATA_DIR = Path("output_multimes")
OUT_DIR = DATA_DIR / "validacion_red"
MIN_SOPORTE_PAR = 50
UMBRAL_LIFT = 1.2
TOP_N = 15

# Los 5 nombres de instituciones que el paper reporta como "puentes"
# (mayor intermediacion en la red de enriquecimiento) y su % publicado,
# transcritos de validacion_estadistica.md, para verificar coincidencia.
PUENTES_PUBLICADOS = {
    "SUBSECRETARIA GENERAL DE LA PRESIDENCIA": 11.8,
    "COMISION PARA LA INTEGRIDAD PUBLICA": 6.8,
    "CONTRALORIA GENERAL DE LA REPUBLICA": 6.6,
    "SERVICIO CIVIL": 3.5,  # nombre aproximado, se busca por substring
    "COMISION PARA EL MERCADO FINANCIERO": 3.2,
}


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
            lft = soporte / esperado if esperado > 0 else float("nan")
            if lft >= UMBRAL_LIFT:
                G.add_edge(a, b, soporte=soporte, lift=lft)
    return G, marginales


def buscar_publicado(nombre_buscado, betweenness_dict):
    """Coincidencia tolerante por substring (mayusculas/espacios ya
    normalizados aguas arriba en el catalogo de instituciones)."""
    candidatos = [k for k in betweenness_dict if nombre_buscado.split()[0] in k.upper()
                  or all(w in k.upper() for w in nombre_buscado.split()[:2])]
    return candidatos


def main():
    OUT_DIR.mkdir(exist_ok=True)
    G, marginales = cargar_G_lift()
    print(f"G_lift: {G.number_of_nodes()} nodos, {G.number_of_edges():,} aristas")

    # --- Variante A: distancia = 1/log1p(lift) -- LA REALMENTE USADA ---
    print("\n[Variante A] weight='distancia' = 1/log1p(lift) -- la que usó 4_analisis_red_local.py")
    for _, _, d in G.edges(data=True):
        d["dist_log"] = 1.0 / math.log1p(d["lift"]) if d["lift"] > 0 else float("inf")
    btw_A = nx.betweenness_centrality(G, weight="dist_log", normalized=True)

    # --- Variante B: weight=lift CRUDO -- la version con el bug ---
    print("[Variante B] weight='lift' crudo -- ADVERTENCIA: esto es la interpretación "
          "INCORRECTA (distancia debería ser inversa al lift, no el lift mismo). Se calcula "
          "solo para poder DESCARTARLA por comparación, no porque sea válida.")
    btw_B = nx.betweenness_centrality(G, weight="lift", normalized=True)

    # --- Variante C: distancia = 1/lift (la sugerida por el propio prompt) ---
    print("[Variante C] weight='distancia' = 1/lift (reciproco simple, sin comprimir log)")
    for _, _, d in G.edges(data=True):
        d["dist_recip"] = 1.0 / d["lift"] if d["lift"] > 0 else float("inf")
    btw_C = nx.betweenness_centrality(G, weight="dist_recip", normalized=True)

    def top(d, n=TOP_N):
        return sorted(d.items(), key=lambda x: -x[1])[:n]

    print(f"\n{'='*70}\nTOP {TOP_N} por variante\n{'='*70}")
    for etiqueta, btw in (("A (distancia=1/log1p(lift), la publicada)", btw_A),
                          ("B (lift crudo -- version con el bug)", btw_B),
                          ("C (distancia=1/lift, reciproco simple)", btw_C)):
        print(f"\n--- Variante {etiqueta} ---")
        for inst, b in top(btw):
            print(f"  {inst:<55} betweenness={b:.5f}")

    # --- Verificacion contra los % publicados ---
    print(f"\n{'='*70}\nVerificación contra los porcentajes publicados\n{'='*70}")
    print("(betweenness normalizada de NetworkX está en escala 0-1, no en %; se reporta "
          "×100 para comparar directamente contra las cifras del paper)")
    resultados_verificacion = []
    for nombre_pub, pct_pub in PUENTES_PUBLICADOS.items():
        fila = {"institucion_buscada": nombre_pub, "pct_publicado": pct_pub}
        for etiqueta_corta, btw in (("A_log1p", btw_A), ("B_lift_crudo", btw_B), ("C_reciproco", btw_C)):
            candidatos = buscar_publicado(nombre_pub, btw)
            if not candidatos:
                fila[f"pct_{etiqueta_corta}"] = None
                continue
            # si hay varios candidatos por coincidencia parcial, toma el de mayor betweenness
            mejor = max(candidatos, key=lambda k: btw[k])
            fila[f"pct_{etiqueta_corta}"] = round(btw[mejor] * 100, 2)
            fila[f"nombre_encontrado_{etiqueta_corta}"] = mejor
        resultados_verificacion.append(fila)
        print(f"\n{nombre_pub} (publicado: {pct_pub}%)")
        for etiqueta_corta in ("A_log1p", "B_lift_crudo", "C_reciproco"):
            v = fila.get(f"pct_{etiqueta_corta}")
            nombre_enc = fila.get(f"nombre_encontrado_{etiqueta_corta}", "NO ENCONTRADO")
            print(f"    {etiqueta_corta}: {v}%  ({nombre_enc})")

    # --- Comparacion hubs (por fuerza/soporte) vs puentes (por betweenness), por variante ---
    fuerza = {}
    for n in G.nodes():
        fuerza[n] = sum(d["soporte"] for _, _, d in G.edges(n, data=True))
    print(f"\n{'='*70}\nTop {TOP_N} por FUERZA (hubs de tráfico, referencia)\n{'='*70}")
    hubs = top(fuerza)
    for inst, f in hubs:
        print(f"  {inst:<55} fuerza(soporte)={f:>14,.0f}")

    hubs_nombres = {h for h, _ in hubs}
    for etiqueta_corta, btw in (("A_log1p", btw_A), ("B_lift_crudo", btw_B), ("C_reciproco", btw_C)):
        puentes_nombres = {p for p, _ in top(btw)}
        overlap = hubs_nombres & puentes_nombres
        print(f"\nVariante {etiqueta_corta}: overlap hubs∩puentes (top {TOP_N} cada uno) = "
              f"{len(overlap)}/{TOP_N} -- {'CONFIRMA puentes≠hubs' if len(overlap) <= 2 else 'hubs y puentes se parecen más de lo esperado'}")
        if overlap:
            print(f"    En común: {sorted(overlap)}")

    # --- Salidas ---
    with open(OUT_DIR / "parte2b_betweenness_variantes.json", "w", encoding="utf-8") as f:
        json.dump({
            "top_A_log1p": [{"institucion": i, "betweenness_pct": round(b*100,3)} for i,b in top(btw_A)],
            "top_B_lift_crudo": [{"institucion": i, "betweenness_pct": round(b*100,3)} for i,b in top(btw_B)],
            "top_C_reciproco": [{"institucion": i, "betweenness_pct": round(b*100,3)} for i,b in top(btw_C)],
            "verificacion_vs_publicado": resultados_verificacion,
        }, f, indent=2, ensure_ascii=False)

    print(f"\nSalidas en {OUT_DIR}/parte2b_betweenness_variantes.json")


if __name__ == "__main__":
    main()
