import json
import argparse
from collections import defaultdict, Counter
from pathlib import Path

# ==================== CONFIG ====================
JSONL_FILE = "output/itemsets.jsonl"   # ← cambia si tu ruta es distinta
TOP_N      = 20                        # cuántos resultados mostrar en rankings
# ================================================


def cargar_itemsets(path: str) -> list[tuple[list[str], int]]:
    """Carga el JSONL y devuelve lista de (items, count)."""
    data = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                items, count = json.loads(line)
                data.append((items, int(count)))
            except (json.JSONDecodeError, ValueError) as e:
                print(f"⚠ Línea {lineno} inválida: {e}")
    return data


def separar_por_tamano(data):
    """Agrupa itemsets por cantidad de elementos."""
    grupos = defaultdict(list)
    for items, count in data:
        grupos[len(items)].append((items, count))
    return grupos


# ==================== ANÁLISIS ====================

def resumen_general(data, grupos):
    total      = len(data)
    total_occ  = sum(c for _, c in data)
    tamanos    = sorted(grupos.keys())

    print("=" * 60)
    print("RESUMEN GENERAL")
    print("=" * 60)
    print(f"  Total itemsets:          {total:>12,}")
    print(f"  Suma total ocurrencias:  {total_occ:>12,}")
    print(f"  Tamaños presentes:       {tamanos}")
    print()

    for k in tamanos:
        sub   = grupos[k]
        cnt   = len(sub)
        occ   = sum(c for _, c in sub)
        top1  = sub[0][1] if sub else 0
        bot1  = sub[-1][1] if sub else 0
        print(f"  Tamaño {k}: {cnt:>8,} itemsets | "
              f"ocurr. total {occ:>12,} | "
              f"max {top1:>10,} | min {bot1:>10,}")
    print()


def top_singletons(grupos, n):
    """Apps más frecuentes individualmente."""
    singletons = grupos.get(1, [])
    print("=" * 60)
    print(f"TOP {n} APPS MÁS FRECUENTES (itemsets de tamaño 1)")
    print("=" * 60)
    for i, (items, count) in enumerate(singletons[:n], 1):
        print(f"  {i:>3}. {count:>10,}  {items[0]}")
    print()


def top_pares(grupos, n):
    """Pares de apps más frecuentes."""
    pares = grupos.get(2, [])
    print("=" * 60)
    print(f"TOP {n} PARES MÁS FRECUENTES (itemsets de tamaño 2)")
    print("=" * 60)
    for i, (items, count) in enumerate(pares[:n], 1):
        print(f"  {i:>3}. {count:>10,}  {items[0]}  ↔  {items[1]}")
    print()


def top_trios(grupos, n):
    trios = grupos.get(3, [])
    if not trios:
        return
    print("=" * 60)
    print(f"TOP {n} TRÍOS MÁS FRECUENTES (itemsets de tamaño 3)")
    print("=" * 60)
    for i, (items, count) in enumerate(trios[:n], 1):
        print(f"  {i:>3}. {count:>10,}  {' | '.join(items)}")
    print()


def apps_mas_coocurrentes(data, n):
    """
    Qué apps aparecen más veces dentro de itemsets de tamaño >= 2.
    Mide la 'centralidad' de cada app en el grafo de co-ocurrencias.
    """
    conteo = Counter()
    for items, count in data:
        if len(items) >= 2:
            for app in items:
                conteo[app] += count

    print("=" * 60)
    print(f"TOP {n} APPS POR PESO EN CO-OCURRENCIAS (tamaño >= 2)")
    print("=" * 60)
    for i, (app, total) in enumerate(conteo.most_common(n), 1):
        print(f"  {i:>3}. {total:>12,}  {app}")
    print()


def pares_mas_frecuentes_dentro_de_grandes(data, n):
    """
    Extrae todos los pares posibles de itemsets de tamaño >= 2
    y suma sus conteos. Descubre asociaciones indirectas.
    """
    from itertools import combinations
    par_count = Counter()
    for items, count in data:
        if len(items) >= 2:
            for a, b in combinations(sorted(items), 2):
                par_count[(a, b)] += count

    print("=" * 60)
    print(f"TOP {n} PARES POR PESO ACUMULADO EN TODOS LOS ITEMSETS")
    print("=" * 60)
    for i, ((a, b), total) in enumerate(par_count.most_common(n), 1):
        print(f"  {i:>3}. {total:>12,}  {a}  ↔  {b}")
    print()


def distribucion_por_tamano(grupos):
    """Muestra cuántos itemsets hay de cada tamaño como porcentaje."""
    total = sum(len(v) for v in grupos.values())
    print("=" * 60)
    print("DISTRIBUCIÓN DE ITEMSETS POR TAMAÑO")
    print("=" * 60)
    for k in sorted(grupos.keys()):
        cnt  = len(grupos[k])
        pct  = 100 * cnt / total
        barra = "█" * int(pct / 2)
        print(f"  Tamaño {k}: {cnt:>8,}  ({pct:5.1f}%)  {barra}")
    print()


def buscar_app(data, termino: str, n: int):
    """Filtra itemsets que contienen un término (case-insensitive)."""
    termino_lower = termino.lower()
    encontrados   = [
        (items, count) for items, count in data
        if any(termino_lower in app.lower() for app in items)
    ]
    encontrados.sort(key=lambda x: x[1], reverse=True)

    print("=" * 60)
    print(f"ITEMSETS QUE CONTIENEN '{termino}' (top {n})")
    print("=" * 60)
    if not encontrados:
        print("  — Sin resultados —")
    for i, (items, count) in enumerate(encontrados[:n], 1):
        print(f"  {i:>3}. {count:>10,}  {' | '.join(items)}")
    print(f"  Total encontrados: {len(encontrados):,}")
    print()


def umbral_largo_de_cola(data):
    """
    Muestra cuántos itemsets acumulan el 80% de las ocurrencias totales.
    Útil para entender concentración (ley de Pareto).
    """
    total_occ  = sum(c for _, c in data)
    acumulado  = 0
    for i, (_, count) in enumerate(data, 1):
        acumulado += count
        if acumulado >= 0.8 * total_occ:
            pct_items = 100 * i / len(data)
            print("=" * 60)
            print("CONCENTRACIÓN (regla 80/20)")
            print("=" * 60)
            print(f"  El 80% de las ocurrencias están en los primeros")
            print(f"  {i:,} itemsets ({pct_items:.1f}% del total).")
            print()
            return


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="Analiza el JSONL de itemsets generado por FP-Growth."
    )
    parser.add_argument("--file",   default=JSONL_FILE, help="Ruta al .jsonl")
    parser.add_argument("--top",    type=int, default=TOP_N, help="Top N resultados")
    parser.add_argument("--buscar", type=str, default=None,
                        help="Buscar itemsets que contengan este texto")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"❌ Archivo no encontrado: {path}")
        return

    print(f"\n📂 Cargando {path} ...")
    data   = cargar_itemsets(str(path))
    grupos = separar_por_tamano(data)
    print(f"✅ {len(data):,} itemsets cargados.\n")

    resumen_general(data, grupos)
    distribucion_por_tamano(grupos)
    umbral_largo_de_cola(data)
    top_singletons(grupos, args.top)
    top_pares(grupos, args.top)
    top_trios(grupos, args.top)
    apps_mas_coocurrentes(data, args.top)
    pares_mas_frecuentes_dentro_de_grandes(data, args.top)

    if args.buscar:
        buscar_app(data, args.buscar, args.top)


if __name__ == "__main__":
    main()