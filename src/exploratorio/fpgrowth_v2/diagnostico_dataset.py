import os
import csv
import io
import zipfile
import time
import math
import psutil
from collections import defaultdict, Counter
from tqdm import tqdm

# ================= CONFIG =================

ZIP_FILES = [
    "cu_user_client-2024.zip",
    "cu_user_client-2025.zip",
]

MAPPING_FILE = "dim_integracion_cu.csv"   # opcional; si no existe, usa client_id crudo
DELIMITER = ";"

# Soportes candidatos para traducir a conteo absoluto
SUPPORT_CANDIDATES = [0.10, 0.05, 0.03, 0.02, 0.01, 0.005, 0.001]

# ==========================================

_process = psutil.Process()


def log(msg: str) -> None:
    ram = _process.memory_info().rss / (1024 ** 3)
    print(f"[{time.strftime('%H:%M:%S')}] {msg} | RAM: {ram:.2f} GB", flush=True)


def load_mapping(mapping_file: str):
    """
    Devuelve:
      - known_items: set de client_id válidos si existe mapping
      - item_meta: client_id -> (nombre_app, institucion)
    """
    if not os.path.exists(mapping_file):
        log("No se encontró mapping; se trabajará con client_id observados en los ZIP.")
        return None, {}

    known_items = set()
    item_meta = {}

    with open(mapping_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"client_id", "nombre_app", "institucion"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"El mapping debe contener {sorted(required)}. "
                f"Encontradas: {reader.fieldnames}"
            )

        for row in reader:
            cid = (row.get("client_id") or "").strip()
            if not cid:
                continue
            known_items.add(cid)
            item_meta[cid] = (
                (row.get("nombre_app") or "").strip(),
                (row.get("institucion") or "").strip(),
            )

    log(f"Mapping cargado. Ítems declarados en catálogo: {len(known_items):,}")
    return known_items, item_meta


def iter_rows_from_outer_zip(zip_path: str):
    """
    Itera filas de los CSV contenidos dentro de ZIP internos.
    """
    with zipfile.ZipFile(zip_path) as outer_zip:
        inner_names = outer_zip.namelist()

        for inner_name in tqdm(inner_names, desc=os.path.basename(zip_path)):
            if not inner_name.endswith(".zip"):
                continue

            try:
                inner_bytes = outer_zip.read(inner_name)
            except Exception as e:
                log(f"Error leyendo inner zip {inner_name}: {e}")
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_name = next(
                        (n for n in inner_zip.namelist() if n.endswith(".csv")),
                        None
                    )
                    if csv_name is None:
                        continue

                    with inner_zip.open(csv_name) as csv_bin:
                        reader = csv.DictReader(
                            io.TextIOWrapper(csv_bin, encoding="utf-8"),
                            delimiter=DELIMITER
                        )

                        required = {"run_anonimizado", "client_id_traza"}
                        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                            log(
                                f"CSV {csv_name} sin columnas esperadas. "
                                f"Encontradas: {reader.fieldnames}"
                            )
                            continue

                        for row in reader:
                            yield row

            except Exception as e:
                log(f"Error procesando inner zip {inner_name}: {e}")
                continue


def percentile_from_counter(counter: Counter, p: float) -> int:
    """
    Percentil aproximado a partir de Counter{valor: frecuencia}.
    p en [0,1]
    """
    if not counter:
        return 0

    total = sum(counter.values())
    target = math.ceil(total * p)
    acc = 0
    for value in sorted(counter):
        acc += counter[value]
        if acc >= target:
            return value
    return max(counter)


def analyze_dataset(zip_files, known_items=None):
    """
    Construye transacciones por run_anonimizado en memoria.
    Esto sirve como diagnóstico estadístico, no para minería final.
    """
    phase = "Lectura de ZIPs y consolidación de transacciones"
    print("\n" + "=" * 90)
    log(phase)
    print("=" * 90)

    transactions = defaultdict(set)

    raw_rows = 0
    rows_with_run = 0
    skipped_empty_run = 0
    skipped_empty_trace = 0
    skipped_unknown_item = 0
    exploded_pairs = 0

    for zip_path in zip_files:
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"No existe el archivo: {zip_path}")

        log(f"Procesando {zip_path}")

        for row in iter_rows_from_outer_zip(zip_path):
            raw_rows += 1

            run = (row.get("run_anonimizado") or "").strip()
            if not run:
                skipped_empty_run += 1
                continue

            rows_with_run += 1

            client_ids_raw = (row.get("client_id_traza") or "").strip().strip('"')
            if not client_ids_raw:
                skipped_empty_trace += 1
                continue

            for cid in client_ids_raw.split(","):
                cid = cid.strip()
                if not cid:
                    continue

                if known_items is not None and cid not in known_items:
                    skipped_unknown_item += 1
                    continue

                transactions[run].add(cid)
                exploded_pairs += 1

            if raw_rows % 2_000_000 == 0:
                log(
                    f"Avance: filas={raw_rows:,}, usuarios={len(transactions):,}, "
                    f"pares observados={exploded_pairs:,}"
                )

    log("Consolidación terminada")

    print("\n" + "=" * 90)
    log("Cálculo de métricas")
    print("=" * 90)

    n_transactions = len(transactions)
    if n_transactions == 0:
        raise RuntimeError("No se generaron transacciones válidas.")

    item_support_counts = Counter()
    tx_size_hist = Counter()
    distinct_observed_items = set()

    total_items_in_transactions = 0

    for items in tqdm(transactions.values(), desc="Metricando transacciones"):
        tx_size = len(items)
        tx_size_hist[tx_size] += 1
        total_items_in_transactions += tx_size

        for cid in items:
            item_support_counts[cid] += 1
            distinct_observed_items.add(cid)

    distinct_items = len(distinct_observed_items)
    avg_tx_size = total_items_in_transactions / n_transactions
    density = (avg_tx_size / distinct_items) if distinct_items else 0.0

    p50 = percentile_from_counter(tx_size_hist, 0.50)
    p90 = percentile_from_counter(tx_size_hist, 0.90)
    p95 = percentile_from_counter(tx_size_hist, 0.95)
    p99 = percentile_from_counter(tx_size_hist, 0.99)
    max_tx = max(tx_size_hist) if tx_size_hist else 0

    top_items = item_support_counts.most_common(20)

    return {
        "raw_rows": raw_rows,
        "rows_with_run": rows_with_run,
        "skipped_empty_run": skipped_empty_run,
        "skipped_empty_trace": skipped_empty_trace,
        "skipped_unknown_item": skipped_unknown_item,
        "exploded_pairs": exploded_pairs,
        "n_transactions": n_transactions,
        "distinct_items": distinct_items,
        "avg_tx_size": avg_tx_size,
        "density": density,
        "tx_size_hist": tx_size_hist,
        "p50": p50,
        "p90": p90,
        "p95": p95,
        "p99": p99,
        "max_tx": max_tx,
        "top_items": top_items,
    }


def support_table(n_transactions: int):
    rows = []
    for s in SUPPORT_CANDIDATES:
        cnt = max(1, math.ceil(s * n_transactions))
        rows.append((s, cnt))
    return rows


def suggest_min_support(n_transactions: int, distinct_items: int, avg_tx_size: float):
    """
    Heurística orientativa para una primera corrida.
    No es una regla matemática; sirve como punto de partida.
    """
    # Densidad nominal simple
    density = (avg_tx_size / distinct_items) if distinct_items else 0.0

    # Reglas heurísticas
    if n_transactions >= 10_000_000:
        if avg_tx_size <= 10:
            return 0.02
        elif avg_tx_size <= 20:
            return 0.03
        else:
            return 0.05

    if n_transactions >= 1_000_000:
        if density < 0.01:
            return 0.01
        return 0.02

    if n_transactions >= 100_000:
        if density < 0.02:
            return 0.005
        return 0.01

    return 0.001


def suggest_algorithm(n_transactions: int, distinct_items: int, avg_tx_size: float):
    """
    Recomendación heurística de algoritmo.
    """
    density = (avg_tx_size / distinct_items) if distinct_items else 0.0

    # Interpretación simple:
    # - FP-Growth: suele rendir bien en datasets medianamente densos o tamaño medio
    # - Eclat: más cómodo con datos relativamente sparse y cuando interesa soporte alto
    # - LCM: muy fuerte para enumeración eficiente, especialmente en grandes volúmenes/sparse
    if n_transactions >= 5_000_000:
        if avg_tx_size <= 10 and density < 0.01:
            return "LCM", "Muy grande y disperso; LCM suele ser mejor candidato inicial."
        if avg_tx_size <= 15:
            return "Eclat o LCM", "Muy grande; probar primero LCM y, si no está disponible, Eclat."
        return "FP-Growth o LCM", "Grande y algo más denso; FP-Growth puede servir, pero LCM merece prueba."

    if n_transactions >= 500_000:
        if density < 0.01:
            return "Eclat", "Volumen alto y sparse; Eclat puede ser competitivo."
        return "FP-Growth", "Volumen medio-alto y densidad moderada; FP-Growth es una buena primera opción."

    return "FP-Growth", "Tamaño moderado; FP-Growth suele ser una opción razonable."


def print_report(stats, item_meta):
    print("\n" + "=" * 90)
    print("REPORTE DIAGNÓSTICO")
    print("=" * 90)

    print(f"Filas leídas totales                : {stats['raw_rows']:,}")
    print(f"Filas con run válido                : {stats['rows_with_run']:,}")
    print(f"Filas sin run                       : {stats['skipped_empty_run']:,}")
    print(f"Filas sin client_id_traza           : {stats['skipped_empty_trace']:,}")
    print(f"Ítems desconocidos omitidos         : {stats['skipped_unknown_item']:,}")
    print(f"Pares run-item observados           : {stats['exploded_pairs']:,}")
    print("-" * 90)
    print(f"Número de transacciones             : {stats['n_transactions']:,}")
    print(f"Ítems distintos observados          : {stats['distinct_items']:,}")
    print(f"Tamaño promedio de transacción      : {stats['avg_tx_size']:.4f}")
    print(f"Densidad aproximada                 : {stats['density']:.8f}")
    print("-" * 90)
    print(f"P50 tamaño transacción              : {stats['p50']}")
    print(f"P90 tamaño transacción              : {stats['p90']}")
    print(f"P95 tamaño transacción              : {stats['p95']}")
    print(f"P99 tamaño transacción              : {stats['p99']}")
    print(f"Tamaño máximo de transacción        : {stats['max_tx']}")

    print("\n" + "=" * 90)
    print("SOPORTES CANDIDATOS")
    print("=" * 90)
    for s, cnt in support_table(stats["n_transactions"]):
        print(f"{s:>7.3%}  ->  {cnt:,} transacciones")

    suggested = suggest_min_support(
        stats["n_transactions"],
        stats["distinct_items"],
        stats["avg_tx_size"]
    )
    suggested_cnt = math.ceil(suggested * stats["n_transactions"])
    print("-" * 90)
    print(f"Soporte mínimo sugerido inicial     : {suggested:.3%}")
    print(f"Equivale a                          : {suggested_cnt:,} transacciones")

    algo, rationale = suggest_algorithm(
        stats["n_transactions"],
        stats["distinct_items"],
        stats["avg_tx_size"]
    )

    print("\n" + "=" * 90)
    print("RECOMENDACIÓN DE ALGORITMO")
    print("=" * 90)
    print(f"Algoritmo sugerido                  : {algo}")
    print(f"Justificación                       : {rationale}")

    print("\n" + "=" * 90)
    print("TOP 20 ÍTEMS POR SOPORTE")
    print("=" * 90)
    for cid, cnt in stats["top_items"]:
        supp = cnt / stats["n_transactions"]
        if cid in item_meta:
            nombre_app, institucion = item_meta[cid]
            label = f"{cid} | {nombre_app} | {institucion}"
        else:
            label = cid
        print(f"{cnt:>12,}  ({supp:>7.3%})  {label}")


def main():
    start = time.time()
    log("Inicio del diagnóstico")

    known_items, item_meta = load_mapping(MAPPING_FILE)

    stats = analyze_dataset(
        zip_files=ZIP_FILES,
        known_items=known_items
    )

    print_report(stats, item_meta)

    log(f"Diagnóstico terminado en {(time.time() - start)/60:.2f} minutos")


if __name__ == "__main__":
    main()