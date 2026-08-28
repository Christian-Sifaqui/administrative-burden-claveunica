import os
import csv
import io
import zipfile
import hashlib
import json
import time
import gc
import psutil
import shutil
from collections import defaultdict
from tqdm import tqdm
from fim import fpgrowth, arules

# ================= CONFIG =================

K_SHARDS = 1024
SHARDS_DIR = "shards"
OUT_DIR = "output"

MIN_SUPPORT = 0.01
MIN_CONFIDENCE = 0.3
ZMIN = 1
ZMAX = 5
GUARD_MAX_AVG_APPS = 40

SKIP_SHARDING_IF_EXISTS = True

ZIP_FILES = [
    "cu_user_client-2024.zip",
    "cu_user_client-2025.zip"
]

MAPPING_FILE = "dim_integracion_cu.csv"

# ==========================================

_process = psutil.Process()

def log(msg):
    ram = _process.memory_info().rss / (1024 ** 3)
    print(f"[{time.strftime('%H:%M:%S')}] {msg} | RAM: {ram:.2f} GB")


def shard_index(run):
    return int(hashlib.md5(run.encode()).hexdigest(), 16) % K_SHARDS


def shards_exist():
    if not os.path.isdir(SHARDS_DIR):
        return False

    shard_files = [f for f in os.listdir(SHARDS_DIR) if f.startswith("shard_")]

    if len(shard_files) != K_SHARDS:
        print(
            f"⚠ ADVERTENCIA: K_SHARDS={K_SHARDS} pero existen "
            f"{len(shard_files)} shards en disco. Se reconstruirán."
        )
        return False

    return True


# =========================================================
# LOAD MAPPING
# =========================================================

def load_mapping():
    header_index = {}
    header_reverse = {}

    with open(MAPPING_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            cid = row["client_id"].strip()
            header_index[cid] = idx
            header_reverse[idx] = {
                "nombre_app": row["nombre_app"].strip(),
                "institucion": row["institucion"].strip()
            }

    return header_index, header_reverse


# =========================================================
# BUILD SHARDS
# =========================================================

def build_shards():
    if SKIP_SHARDING_IF_EXISTS and shards_exist():
        log("Shards válidos ya existen. Saltando sharding.")
        return

    # Elimina shards parciales o inconsistentes
    if os.path.exists(SHARDS_DIR):
        shutil.rmtree(SHARDS_DIR)

    os.makedirs(SHARDS_DIR)

    writers = {}
    max_open = 64  # LRU

    def get_writer(idx):
        if idx not in writers:
            if len(writers) >= max_open:
                oldest_key = next(iter(writers))
                writers.pop(oldest_key).close()

            writers[idx] = open(
                os.path.join(SHARDS_DIR, f"shard_{idx}.csv"),
                "a",                     # ← obligatorio con LRU
                buffering=1 << 20
            )
        return writers[idx]

    for zip_path in ZIP_FILES:
        log(f"Procesando {zip_path}")

        with zipfile.ZipFile(zip_path) as outer_zip:
            for inner_name in tqdm(
                outer_zip.namelist(),
                desc=os.path.basename(zip_path)
            ):
                if not inner_name.endswith(".zip"):
                    continue

                inner_bytes = outer_zip.read(inner_name)

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_name = next(
                        (m for m in inner_zip.namelist() if m.endswith(".csv")),
                        None
                    )
                    if csv_name is None:
                        continue

                    with inner_zip.open(csv_name) as csv_bin:
                        reader = csv.DictReader(
                            io.TextIOWrapper(csv_bin, encoding="utf-8"),
                            delimiter=";"
                        )

                        for row in reader:
                            run = row["run_anonimizado"].strip()
                            if not run:
                                continue

                            client_ids_raw = row["client_id_traza"].strip().strip('"')
                            if not client_ids_raw:
                                continue

                            sidx = shard_index(run)

                            for cid in client_ids_raw.split(","):
                                cid = cid.strip()
                                if not cid:
                                    continue

                                get_writer(sidx).write(f"{run},{cid}\n")

    for f in writers.values():
        f.close()


# =========================================================
# PASS 1 – CONTEO GLOBAL
# =========================================================

def pass1_count(header_index):
    global_counts = defaultdict(int)
    total_users = 0

    shard_files = sorted(
        f for f in os.listdir(SHARDS_DIR)
        if f.startswith("shard_")
    )

    for shard_file in tqdm(shard_files, desc="Pass1"):
        user_bits = defaultdict(int)

        with open(os.path.join(SHARDS_DIR, shard_file)) as f:
            for line in f:
                parts = line.strip().split(",", 1)
                if len(parts) != 2:
                    continue

                run, cid = parts

                if cid not in header_index:
                    continue

                bit = header_index[cid]
                user_bits[run] |= (1 << bit)

        for bitmask in user_bits.values():
            total_users += 1

            b = bitmask
            while b:
                lsb = b & -b
                idx = lsb.bit_length() - 1
                global_counts[idx] += 1
                b ^= lsb

    return global_counts, total_users


# =========================================================
# BUILD TRANSACTIONS
# =========================================================

def build_transactions(is_freq_mask):
    transactions = []

    shard_files = sorted(
        f for f in os.listdir(SHARDS_DIR)
        if f.startswith("shard_")
    )

    for shard_file in tqdm(shard_files, desc="Build transactions"):
        user_bits = defaultdict(int)

        with open(os.path.join(SHARDS_DIR, shard_file)) as f:
            for line in f:
                parts = line.strip().split(",", 1)
                if len(parts) != 2:
                    continue

                run, cid = parts

                bit = is_freq_mask.get(cid)
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

            if items:  # defensivo explícito
                transactions.append(items)

    return transactions


# =========================================================
# SAVE OUTPUT
# =========================================================

def save_itemsets(itemsets, total_users, header_reverse):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "itemsets.jsonl")

    with open(path, "w", encoding="utf-8") as f:
        for items, supp in itemsets:
            names = [
                f"{header_reverse[i]['nombre_app']}: "
                f"{header_reverse[i]['institucion']}"
                for i in items
            ]

            f.write(json.dumps({
                "items": names,
                "support": round(supp / total_users, 8),
                "count": int(supp)
            }) + "\n")


def save_rules(rules, total_users, header_reverse):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "rules.jsonl")

    with open(path, "w", encoding="utf-8") as f:
        for rule in rules:
            if len(rule) != 5:
                continue  # Filtrar reglas que no tienen los 5 componentes esperados

            consequent, antecedent, supp, conf, lift = rule  # Cada regla tiene 5 componentes

            # Mapeo de los elementos del antecedente y consecuente a sus nombres
            ant_names = [
                f"{header_reverse[i]['nombre_app']}: {header_reverse[i]['institucion']}"
                for i in antecedent
            ]

            cons_names = [
                f"{header_reverse[i]['nombre_app']}: {header_reverse[i]['institucion']}"
                for i in consequent
            ]

            # Guardar la regla con sus métricas: antecedente, consecuente, soporte, confianza y lift
            f.write(json.dumps({
                "antecedent": ant_names,
                "consequent": cons_names,
                "support": round(supp / total_users, 8),
                "count": int(supp),
                "confidence": round(conf, 6),
                "lift": round(lift, 6)
            }) + "\n")


# =========================================================
# MAIN
# =========================================================

def main():
    start = time.time()

    header_index, header_reverse = load_mapping()
    log("Mapping cargado")

    build_shards()

    global_counts, total_users = pass1_count(header_index)
    log(f"Usuarios válidos: {total_users}")

    min_count = max(1, int(MIN_SUPPORT * total_users))

    is_freq_mask = {
        cid: idx
        for cid, idx in header_index.items()
        if global_counts.get(idx, 0) >= min_count
    }

    transactions = build_transactions(is_freq_mask)

    if not transactions:
        raise RuntimeError(
            "No hay transacciones frecuentes. "
            "Revisa MIN_SUPPORT o el dataset."
        )

    avg_apps = sum(len(t) for t in transactions) / len(transactions)
    log(f"Apps promedio por usuario: {avg_apps:.2f}")

    zmax_eff = ZMAX
    if avg_apps > GUARD_MAX_AVG_APPS:
        log("Dataset denso → reduciendo ZMAX a 3")
        zmax_eff = 3

    log("Ejecutando FP-Growth")
    itemsets = fpgrowth(
        transactions,
        supp=min_count,
        zmin=ZMIN,
        zmax=zmax_eff,
        report="s"  # Solo soporte, ya que las métricas adicionales las generamos con arules
    )

    log("Generando reglas de asociación")
    # Generación de reglas de asociación con soporte, confianza y lift
    rules = arules(
        transactions,
        supp=min_count,
        conf=MIN_CONFIDENCE,
        zmin=max(ZMIN, 2),
        zmax=zmax_eff,
        report="scl"  # Soporte, confianza y lift
    )

    # Guardar los resultados de itemsets y reglas
    save_itemsets(itemsets, total_users, header_reverse)
    save_rules(rules, total_users, header_reverse)

    # Liberar memoria
    del transactions
    gc.collect()

    log(f"Finalizado en {(time.time()-start)/60:.2f} minutos")


if __name__ == "__main__":
    main()
