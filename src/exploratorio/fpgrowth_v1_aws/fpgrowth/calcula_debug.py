import os
import csv
import io
import zipfile
import hashlib
import json
import time
import gc
import math
import psutil
import shutil
import traceback
from collections import defaultdict
from tqdm import tqdm
from fim import fpgrowth, arules

# ================= CONFIG =================

K_SHARDS = 1024
SHARDS_DIR = "shards"
OUT_DIR = "output"

# Más conservador para la primera prueba
MIN_SUPPORT = 0.02
MIN_CONFIDENCE = 0.30
ZMIN = 1
ZMAX = 3
GUARD_MAX_AVG_APPS = 25

# En datasets muy grandes conviene probar primero solo itemsets
GENERATE_RULES = False

# Si True, reutiliza shards solo si el manifest coincide
SKIP_SHARDING_IF_EXISTS = True

ZIP_FILES = [
    "cu_user_client-2024.zip",
    "cu_user_client-2025.zip"
]

MAPPING_FILE = "dim_integracion_cu.csv"

MANIFEST_FILE = os.path.join(SHARDS_DIR, "_manifest.json")

# Cada cuántos shards mostrar progreso detallado adicional
LOG_EVERY_N_SHARDS = 32

# ==========================================

_process = psutil.Process()


def log(msg):
    ram = _process.memory_info().rss / (1024 ** 3)
    print(f"[{time.strftime('%H:%M:%S')}] {msg} | RAM: {ram:.2f} GB", flush=True)


def phase(msg):
    print("\n" + "=" * 90, flush=True)
    log(msg)
    print("=" * 90, flush=True)


def safe_remove(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def shard_index(run):
    return int(hashlib.md5(run.encode("utf-8")).hexdigest(), 16) % K_SHARDS


def compute_manifest():
    payload = {
        "K_SHARDS": K_SHARDS,
        "ZIP_FILES": ZIP_FILES,
        "MAPPING_FILE": MAPPING_FILE,
    }
    return payload


def write_manifest():
    os.makedirs(SHARDS_DIR, exist_ok=True)
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(compute_manifest(), f, ensure_ascii=False, indent=2)


def manifest_matches():
    if not os.path.exists(MANIFEST_FILE):
        log("Manifest de shards no existe.")
        return False

    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            on_disk = json.load(f)
    except Exception as e:
        log(f"No se pudo leer manifest: {e}")
        return False

    expected = compute_manifest()
    if on_disk != expected:
        log("Manifest no coincide con la configuración actual.")
        return False

    return True


def shards_exist_and_valid():
    if not os.path.isdir(SHARDS_DIR):
        log("Directorio de shards no existe.")
        return False

    shard_files = sorted(
        f for f in os.listdir(SHARDS_DIR)
        if f.startswith("shard_") and f.endswith(".csv")
    )

    if len(shard_files) != K_SHARDS:
        log(
            f"Cantidad de shards inconsistente: esperados={K_SHARDS}, "
            f"encontrados={len(shard_files)}"
        )
        return False

    for i in range(K_SHARDS):
        expected_name = f"shard_{i}.csv"
        if expected_name not in shard_files:
            log(f"Falta shard esperado: {expected_name}")
            return False

    if not manifest_matches():
        return False

    return True


# =========================================================
# LOAD MAPPING
# =========================================================

def load_mapping():
    phase("FASE 1: Cargando mapping")

    header_index = {}
    header_reverse = {}

    if not os.path.exists(MAPPING_FILE):
        raise FileNotFoundError(f"No existe el archivo de mapping: {MAPPING_FILE}")

    with open(MAPPING_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {"client_id", "nombre_app", "institucion"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"El mapping debe contener columnas {sorted(required)}. "
                f"Encontradas: {reader.fieldnames}"
            )

        for idx, row in enumerate(reader):
            cid = (row.get("client_id") or "").strip()
            if not cid:
                continue

            header_index[cid] = idx
            header_reverse[idx] = {
                "nombre_app": (row.get("nombre_app") or "").strip(),
                "institucion": (row.get("institucion") or "").strip()
            }

    log(f"Mapping cargado. client_id únicos={len(header_index)}")

    if not header_index:
        raise RuntimeError("El mapping quedó vacío.")

    return header_index, header_reverse


# =========================================================
# BUILD SHARDS
# =========================================================

def build_shards():
    phase("FASE 2: Preparando shards")

    if SKIP_SHARDING_IF_EXISTS and shards_exist_and_valid():
        log("Shards válidos ya existen. Se reutilizarán.")
        return

    log("Se reconstruirán los shards desde cero.")

    if os.path.exists(SHARDS_DIR):
        safe_remove(SHARDS_DIR)

    os.makedirs(SHARDS_DIR, exist_ok=True)

    writers = {}
    max_open = 64  # FIFO simple; suficiente para limitar descriptores abiertos

    def get_writer(idx):
        if idx not in writers:
            if len(writers) >= max_open:
                oldest_key = next(iter(writers))
                writers.pop(oldest_key).close()

            writers[idx] = open(
                os.path.join(SHARDS_DIR, f"shard_{idx}.csv"),
                "a",
                buffering=1 << 20,
                encoding="utf-8"
            )
        return writers[idx]

    total_rows = 0
    total_pairs = 0

    for zip_path in ZIP_FILES:
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"No existe el archivo ZIP: {zip_path}")

        log(f"Procesando ZIP externo: {zip_path}")

        with zipfile.ZipFile(zip_path) as outer_zip:
            inner_names = outer_zip.namelist()
            log(f"Entradas internas detectadas: {len(inner_names)}")

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
                            (m for m in inner_zip.namelist() if m.endswith(".csv")),
                            None
                        )
                        if csv_name is None:
                            log(f"Inner zip sin CSV: {inner_name}")
                            continue

                        with inner_zip.open(csv_name) as csv_bin:
                            reader = csv.DictReader(
                                io.TextIOWrapper(csv_bin, encoding="utf-8"),
                                delimiter=";"
                            )

                            required = {"run_anonimizado", "client_id_traza"}
                            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                                log(
                                    f"CSV {csv_name} sin columnas esperadas. "
                                    f"Encontradas: {reader.fieldnames}"
                                )
                                continue

                            for row in reader:
                                total_rows += 1

                                run = (row.get("run_anonimizado") or "").strip()
                                if not run:
                                    continue

                                client_ids_raw = (row.get("client_id_traza") or "").strip().strip('"')
                                if not client_ids_raw:
                                    continue

                                sidx = shard_index(run)

                                for cid in client_ids_raw.split(","):
                                    cid = cid.strip()
                                    if not cid:
                                        continue
                                    get_writer(sidx).write(f"{run},{cid}\n")
                                    total_pairs += 1

                                if total_rows % 2_000_000 == 0:
                                    log(
                                        f"Sharding en progreso: filas={total_rows:,}, "
                                        f"pares run-cid={total_pairs:,}"
                                    )

                except Exception as e:
                    log(f"Error procesando inner zip {inner_name}: {e}")
                    continue

    for f in writers.values():
        f.close()

    write_manifest()
    log(f"Sharding finalizado. filas={total_rows:,}, pares run-cid={total_pairs:,}")


# =========================================================
# PASS 1 – CONTEO GLOBAL
# =========================================================

def pass1_count(header_index):
    phase("FASE 3: Conteo global de soportes")

    global_counts = defaultdict(int)
    total_users = 0
    skipped_unknown_cid = 0

    shard_files = sorted(
        f for f in os.listdir(SHARDS_DIR)
        if f.startswith("shard_") and f.endswith(".csv")
    )

    log(f"Shards a procesar en Pass1: {len(shard_files)}")

    for n, shard_file in enumerate(tqdm(shard_files, desc="Pass1"), start=1):
        user_bits = defaultdict(int)
        shard_path = os.path.join(SHARDS_DIR, shard_file)

        with open(shard_path, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",", 1)
                if len(parts) != 2:
                    continue

                run, cid = parts
                bit = header_index.get(cid)

                if bit is None:
                    skipped_unknown_cid += 1
                    continue

                user_bits[run] |= (1 << bit)

        for bitmask in user_bits.values():
            total_users += 1

            b = bitmask
            while b:
                lsb = b & -b
                idx = lsb.bit_length() - 1
                global_counts[idx] += 1
                b ^= lsb

        if n % LOG_EVERY_N_SHARDS == 0:
            log(
                f"Pass1 progreso: shard={n}/{len(shard_files)}, "
                f"usuarios acumulados={total_users:,}, "
                f"ítems con soporte>0={len(global_counts):,}, "
                f"cid desconocidos={skipped_unknown_cid:,}"
            )

    log(
        f"Pass1 finalizado. usuarios válidos={total_users:,}, "
        f"ítems con soporte>0={len(global_counts):,}, "
        f"cid desconocidos omitidos={skipped_unknown_cid:,}"
    )

    return global_counts, total_users


# =========================================================
# COMPACTAR ÍTEMS FRECUENTES
# =========================================================

def compact_frequent_items(header_index, header_reverse, global_counts, min_count):
    phase("FASE 4: Compactando ítems frecuentes")

    freq_index = {}
    compact_reverse = {}
    nfreq = 0

    for cid, old_idx in header_index.items():
        if global_counts.get(old_idx, 0) >= min_count:
            freq_index[cid] = nfreq
            compact_reverse[nfreq] = header_reverse[old_idx]
            nfreq += 1

    log(
        f"Ítems frecuentes compactados: {nfreq:,} "
        f"(min_count={min_count:,})"
    )

    if nfreq == 0:
        raise RuntimeError(
            "No quedaron ítems frecuentes tras aplicar el soporte mínimo."
        )

    return freq_index, compact_reverse


# =========================================================
# BUILD TRANSACTIONS
# =========================================================

def build_transactions(freq_index):
    phase("FASE 5: Construcción de transacciones")

    transactions = []
    dropped_empty = 0
    total_items = 0

    shard_files = sorted(
        f for f in os.listdir(SHARDS_DIR)
        if f.startswith("shard_") and f.endswith(".csv")
    )

    log(f"Shards a procesar en Build transactions: {len(shard_files)}")

    for n, shard_file in enumerate(tqdm(shard_files, desc="Build transactions"), start=1):
        user_bits = defaultdict(int)
        shard_path = os.path.join(SHARDS_DIR, shard_file)

        with open(shard_path, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",", 1)
                if len(parts) != 2:
                    continue

                run, cid = parts
                bit = freq_index.get(cid)

                if bit is not None:
                    user_bits[run] |= (1 << bit)

        for bitmask in user_bits.values():
            items = []
            b = bitmask
            while b:
                lsb = b & -b
                idx = lsb.bit_length() - 1
                items.append(idx)
                b ^= lsb

            if items:
                transactions.append(items)
                total_items += len(items)
            else:
                dropped_empty += 1

        if n % LOG_EVERY_N_SHARDS == 0:
            ram = _process.memory_info().rss / (1024 ** 3)
            tx_count = len(transactions)
            avg = (total_items / tx_count) if tx_count else 0.0
            log(
                f"Build progreso: shard={n}/{len(shard_files)}, "
                f"transactions={tx_count:,}, avg_apps={avg:.2f}, "
                f"vacías={dropped_empty:,}, RAM={ram:.2f} GB"
            )

    if not transactions:
        raise RuntimeError(
            "No se construyeron transacciones. "
            "Revisa MIN_SUPPORT o los datos de entrada."
        )

    avg_apps = total_items / len(transactions)
    log(
        f"Build finalizado. transacciones={len(transactions):,}, "
        f"avg_apps={avg_apps:.2f}, vacías={dropped_empty:,}"
    )

    return transactions, avg_apps


# =========================================================
# SAVE OUTPUT
# =========================================================

def item_name(idx, header_reverse):
    meta = header_reverse[idx]
    return f"{meta['nombre_app']}: {meta['institucion']}"


def save_itemsets(itemsets, total_users, header_reverse):
    phase("FASE 7: Guardando itemsets")

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "itemsets.jsonl")

    with open(path, "w", encoding="utf-8") as f:
        for items, supp in itemsets:
            names = [item_name(i, header_reverse) for i in items]
            f.write(json.dumps({
                "items": names,
                "support": round(supp / total_users, 8),
                "count": int(supp)
            }, ensure_ascii=False) + "\n")

    log(f"Itemsets guardados en {path}")


def save_rules(rules, total_users, header_reverse):
    phase("FASE 8: Guardando reglas")

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "rules.jsonl")

    sample_logged = False

    with open(path, "w", encoding="utf-8") as f:
        for rule in rules:
            if not isinstance(rule, (list, tuple)) or len(rule) != 5:
                continue

            # OJO: en algunas versiones puede venir (antecedent, consequent, supp, conf, lift)
            # y en otras al revés. Dejamos una traza explícita con la primera regla.
            left, right, supp, conf, lift = rule

            if not sample_logged:
                log(f"Muestra de regla cruda detectada: {rule}")
                sample_logged = True

            left_names = [item_name(i, header_reverse) for i in left]
            right_names = [item_name(i, header_reverse) for i in right]

            f.write(json.dumps({
                "left": left_names,
                "right": right_names,
                "support": round(supp / total_users, 8),
                "count": int(supp),
                "confidence": round(conf, 6),
                "lift": round(lift, 6)
            }, ensure_ascii=False) + "\n")

    log(f"Reglas guardadas en {path}")


# =========================================================
# MINERÍA
# =========================================================

def run_fpgrowth(transactions, min_count, zmin, zmax):
    phase("FASE 6A: Ejecutando FP-Growth")

    log(
        f"Parámetros FP-Growth: supp={min_count:,}, "
        f"zmin={zmin}, zmax={zmax}, n_transactions={len(transactions):,}"
    )
    log("Llamando a fim.fpgrowth() ... aquí suele caer si falta memoria")

    t0 = time.time()
    itemsets = fpgrowth(
        transactions,
        supp=min_count,
        zmin=zmin,
        zmax=zmax,
        report="s"
    )
    dt = time.time() - t0

    log(f"FP-Growth finalizó correctamente en {dt/60:.2f} min")
    log(f"Cantidad de itemsets obtenidos: {len(itemsets):,}")

    return itemsets


def run_arules(transactions, min_count, min_confidence, zmin, zmax):
    phase("FASE 6B: Ejecutando reglas de asociación")

    log(
        f"Parámetros arules: supp={min_count:,}, conf={min_confidence}, "
        f"zmin={max(zmin, 2)}, zmax={zmax}, "
        f"n_transactions={len(transactions):,}"
    )
    log("Llamando a fim.arules() ...")

    t0 = time.time()
    rules = arules(
        transactions,
        supp=min_count,
        conf=min_confidence,
        zmin=max(zmin, 2),
        zmax=zmax,
        report="scl"
    )
    dt = time.time() - t0

    log(f"arules finalizó correctamente en {dt/60:.2f} min")
    log(f"Cantidad de reglas obtenidas: {len(rules):,}")

    return rules


# =========================================================
# MAIN
# =========================================================

def main():
    start = time.time()

    try:
        phase("INICIO DEL PROCESO")

        header_index, header_reverse = load_mapping()

        build_shards()

        global_counts, total_users = pass1_count(header_index)

        if total_users == 0:
            raise RuntimeError("No se detectaron usuarios válidos en Pass1.")

        min_count = max(1, math.ceil(MIN_SUPPORT * total_users))
        log(
            f"Soporte mínimo calculado: MIN_SUPPORT={MIN_SUPPORT} -> "
            f"min_count={min_count:,} sobre total_users={total_users:,}"
        )

        freq_index, compact_reverse = compact_frequent_items(
            header_index=header_index,
            header_reverse=header_reverse,
            global_counts=global_counts,
            min_count=min_count
        )

        # Liberación temprana de estructuras pesadas que ya no se necesitan
        phase("FASE 4B: Liberando memoria antes de construir transacciones")
        del global_counts
        del header_index
        del header_reverse
        gc.collect()
        log("Memoria liberada tras compactación")

        transactions, avg_apps = build_transactions(freq_index)

        # freq_index ya no se necesita una vez construidas las transacciones
        del freq_index
        gc.collect()
        log("freq_index liberado antes de minería")

        zmax_eff = ZMAX
        if avg_apps > GUARD_MAX_AVG_APPS:
            log(
                f"Dataset denso detectado (avg_apps={avg_apps:.2f}) -> "
                f"reduciendo zmax efectivo a 2"
            )
            zmax_eff = min(zmax_eff, 2)

        log("Último mensaje antes de FP-Growth. Si muere después de esto, cayó dentro de fim.fpgrowth().")

        itemsets = run_fpgrowth(
            transactions=transactions,
            min_count=min_count,
            zmin=ZMIN,
            zmax=zmax_eff
        )

        save_itemsets(itemsets, total_users, compact_reverse)

        if GENERATE_RULES:
            log("Se generarán reglas de asociación.")
            rules = run_arules(
                transactions=transactions,
                min_count=min_count,
                min_confidence=MIN_CONFIDENCE,
                zmin=ZMIN,
                zmax=zmax_eff
            )
            save_rules(rules, total_users, compact_reverse)
        else:
            log("GENERATE_RULES=False -> se omite generación de reglas en esta corrida.")

        phase("LIMPIEZA FINAL")
        del transactions
        del itemsets
        gc.collect()
        log(f"Finalizado en {(time.time() - start)/60:.2f} minutos")

    except MemoryError:
        log("MemoryError capturado en Python.")
        traceback.print_exc()
        raise
    except Exception as e:
        log(f"ERROR FATAL: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()