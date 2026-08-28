import os
import io
import csv
import gc
import json
import time
import psutil
import zipfile
import hashlib
from collections import defaultdict, OrderedDict
from tqdm import tqdm

# ===================== CONFIG =====================
# ✅ Ahora soporta múltiples años (se unifican en un solo cálculo)
MAIN_FILES = [
    "cu_user_client-2024.zip",
    "cu_user_client-2025.zip",
]

MAPPING_FILE = "dim_integracion_cu.csv"

# Usa un directorio NUEVO para evitar mezclar con shards antiguos de un solo año
SHARD_DIR = "shards_2024_2025"
K_SHARDS = 1024
MAX_OPEN_FILES = 64

TRANSACTIONS_FILE = "transactions_fp_2024_2025.txt"
STATS_FILE = "transactions_stats_2024_2025.json"

FINAL_OUTPUT = "final_output_2024_2025.jsonl"
TRANSLATED_OUTPUT = "final_output_traducido_2024_2025.jsonl"

SUPPORT = 0.0005
ZMIN = 1
ZMAX = 5

# Guardrails
GUARD_MAX_L1 = 1200
GUARD_MAX_AVG_APPS = 25.0
GUARD_MAX_APPS = 200
GUARD_SUPPORT_MULT = 2.0

# Resume flags
SKIP_SHARDING_IF_EXISTS = True
SKIP_TRANSACTIONS_IF_EXISTS = True
SKIP_MINING_IF_OUTPUT_EXISTS = False  # True para no minar si FINAL_OUTPUT ya existe

USE_BOOL_MASK_FOR_L1 = True
# ==================================================


# ===================== UTILIDADES =====================
def _fmt_hms(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _get_logger():
    try:
        return tqdm.write
    except Exception:
        return print


def log_system_usage(prefix=""):
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.5)
    _get_logger()(f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024**3):.2f} GB), CPU: {cpu}%")


def load_client_ids(mapping_file):
    ids = []
    with open(mapping_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("client_id", "").strip()
            if cid:
                ids.append(cid)

    seen = OrderedDict()
    for cid in ids:
        seen[cid] = None
    client_ids = list(seen.keys())
    cid_to_idx = {cid: i for i, cid in enumerate(client_ids)}
    return client_ids, cid_to_idx


def load_client_names_mapping(filename):
    mapping = {}
    with open(filename, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row.get("nombre_app", "").strip()
            inst = row.get("institucion", "").strip()
            client_id = row.get("client_id", "").strip()
            if client_id:
                mapping[client_id] = f"{name}: {inst}"
    return mapping


def traducir_final_output(input_file, output_file, mapping_file):
    mapping = load_client_names_mapping(mapping_file)
    with open(input_file, "r", encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
        for line in fin:
            items, support = json.loads(line)
            traducidos = [mapping.get(i, f"{i} --> Desconocido") for i in items]
            fout.write(json.dumps([traducidos, support], ensure_ascii=False) + "\n")
    _get_logger()(f"✅ Archivo traducido guardado en: {output_file}")


def count_lines_fast(path, chunk_size=1024 * 1024) -> int:
    n = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            n += b.count(b"\n")
    return n


def load_transactions_stats(stats_file: str):
    try:
        with open(stats_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_transactions_stats(stats_file: str, stats: dict):
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(stats, f)


def compute_stats_from_transactions(transactions_file: str):
    total_apps = 0
    max_apps = 0
    n_tx = 0
    bad_lines = 0

    with open(transactions_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Stats transactions_fp", dynamic_ncols=True):
            s = line.strip()
            if not s:
                continue
            parts = s.split()
            n = len(parts)
            total_apps += n
            if n > max_apps:
                max_apps = n
            n_tx += 1

    avg_apps = (total_apps / n_tx) if n_tx else 0.0
    return avg_apps, max_apps, n_tx, bad_lines


def _stable_shard(run: str, k: int) -> int:
    d = hashlib.md5(run.encode("utf-8")).digest()
    return int.from_bytes(d[:8], "big") % k


# ===================== SHARD WRITER (LRU) =====================
class ShardWriter:
    def __init__(self, shard_dir: str, k: int, max_open: int = 64):
        self.shard_dir = shard_dir
        self.k = k
        self.max_open = max_open
        self.handles = OrderedDict()  # shard_id -> file handle

    def _path(self, shard_id: int) -> str:
        return os.path.join(self.shard_dir, f"pairs_{shard_id:04d}.tsv")

    def write(self, shard_id: int, run: str, item_idx: int):
        if shard_id in self.handles:
            fp = self.handles.pop(shard_id)
            self.handles[shard_id] = fp
        else:
            if len(self.handles) >= self.max_open:
                _, old_fp = self.handles.popitem(last=False)
                old_fp.close()
            fp = open(self._path(shard_id), "a", encoding="utf-8", buffering=1024 * 1024)
            self.handles[shard_id] = fp

        fp.write(run)
        fp.write("\t")
        fp.write(str(item_idx))
        fp.write("\n")

    def close(self):
        for fp in self.handles.values():
            fp.close()
        self.handles.clear()


def shards_exist(shard_dir: str, k: int) -> bool:
    if not os.path.isdir(shard_dir):
        return False
    files = [f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")]
    return len(files) >= k


# ===================== FASE 1: MULTI-ZIP -> shards =====================
def preprocess_multiple_zips_to_shards(main_zip_paths, shard_dir: str, cid_to_idx: dict, k: int):
    """
    Lee TODOS los ZIP principales (2024 + 2025) y escribe en los mismos shards.
    Beneficio: mismo run cae siempre en el mismo shard, por lo que al reconstruir
    se unifican transacciones del usuario entre años sin fragmentación.
    """
    os.makedirs(shard_dir, exist_ok=True)
    log = _get_logger()
    writer = ShardWriter(shard_dir, k=k, max_open=MAX_OPEN_FILES)

    for main_zip_path in main_zip_paths:
        log(f"📦 Procesando ZIP principal: {main_zip_path}")
        with zipfile.ZipFile(main_zip_path, "r") as top_zip:
            inner_zip_names = [n for n in top_zip.namelist() if n.lower().endswith(".zip")]
            inner_zip_names.sort()
            log(f"   └─ ZIPs internos: {len(inner_zip_names)}")

            for inner_name in tqdm(inner_zip_names, desc=f"Leyendo internos ({os.path.basename(main_zip_path)})", dynamic_ncols=True):
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    continue

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith(".csv")]
                    if not csv_members:
                        continue
                    csv_name = csv_members[0]

                    with inner_zip.open(csv_name, "r") as csv_file_bin:
                        with io.TextIOWrapper(csv_file_bin, encoding="utf-8", newline="") as csv_text:
                            reader = csv.reader(csv_text, delimiter=";", quotechar='"')
                            header = next(reader, None)
                            if header is None:
                                continue

                            header_norm = [h.strip('"') for h in header]
                            try:
                                idx_run = header_norm.index("run_anonimizado")
                                idx_client = header_norm.index("client_id_traza")
                            except ValueError:
                                continue

                            for row in reader:
                                if not row or len(row) <= max(idx_run, idx_client):
                                    continue
                                run = str(row[idx_run]).strip()
                                if not run:
                                    continue
                                client_field = str(row[idx_client]).strip().strip('"')
                                if not client_field:
                                    continue

                                shard = _stable_shard(run, k)

                                for cid in client_field.split(","):
                                    cid = cid.strip()
                                    if not cid:
                                        continue
                                    item_idx = cid_to_idx.get(cid)
                                    if item_idx is None:
                                        continue
                                    writer.write(shard, run, item_idx)

    writer.close()
    log(f"✅ Shards escritos en: {shard_dir} (K={k}, max_open={MAX_OPEN_FILES})")


# ===================== FASE 2: shards -> conteos + transacciones =====================
def shards_pass1_counts(shard_dir: str, num_items: int):
    shard_files = sorted([f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")])

    item_counts = [0] * num_items
    total_users = 0
    malformed_lines = 0
    out_of_range = 0

    for sf in tqdm(shard_files, desc="Pass 1 (counts)", dynamic_ncols=True):
        path = os.path.join(shard_dir, sf)
        user_bits = {}

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                tab_pos = line.find("\t")
                if tab_pos <= 0:
                    malformed_lines += 1
                    continue
                run = line[:tab_pos]
                b_str = line[tab_pos + 1 :].rstrip("\r\n")
                if not b_str:
                    malformed_lines += 1
                    continue
                try:
                    b = int(b_str)
                except ValueError:
                    malformed_lines += 1
                    continue
                if b < 0 or b >= num_items:
                    out_of_range += 1
                    continue

                user_bits[run] = user_bits.get(run, 0) | (1 << b)

        total_users += len(user_bits)

        for bits in user_bits.values():
            x = bits
            while x:
                lsb = x & -x
                i = lsb.bit_length() - 1
                item_counts[i] += 1
                x ^= lsb

        user_bits.clear()
        gc.collect()

    return item_counts, total_users, malformed_lines, out_of_range


def write_transactions_file_from_shards(
    shard_dir: str,
    transactions_file: str,
    header_counts: dict,
    is_freq_mask=None,
):
    shard_files = sorted([f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")])

    total_users = 0
    total_apps = 0
    max_apps = 0

    def iter_set_bits(bits: int):
        items = []
        x = bits
        while x:
            lsb = x & -x
            i = lsb.bit_length() - 1
            items.append(i)
            x ^= lsb
        return items

    def is_freq(b: int) -> bool:
        if is_freq_mask is None:
            return b in header_counts
        return is_freq_mask[b]

    with open(transactions_file, "w", encoding="utf-8") as out:
        for sf in tqdm(shard_files, desc="Pass 2 (write tx)", dynamic_ncols=True):
            path = os.path.join(shard_dir, sf)
            user_bits = {}

            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    tab_pos = line.find("\t")
                    if tab_pos <= 0:
                        continue
                    run = line[:tab_pos]
                    b_str = line[tab_pos + 1 :].rstrip("\r\n")
                    if not b_str:
                        continue
                    try:
                        b = int(b_str)
                    except ValueError:
                        continue
                    if is_freq(b):
                        user_bits[run] = user_bits.get(run, 0) | (1 << b)

            for bits in user_bits.values():
                items = iter_set_bits(bits)
                if not items:
                    continue

                items.sort(key=lambda i: (header_counts.get(i, 0), i), reverse=True)

                n = len(items)
                total_users += 1
                total_apps += n
                if n > max_apps:
                    max_apps = n

                out.write(" ".join(map(str, items)))
                out.write("\n")

            user_bits.clear()
            gc.collect()

    avg_apps = (total_apps / total_users) if total_users else 0.0
    return avg_apps, max_apps, total_users


# ===================== FP-TREE (sin matriz) =====================
class FPNode:
    __slots__ = ("item", "count", "parent", "children", "node_link")

    def __init__(self, item, parent):
        self.item = item
        self.count = 0
        self.parent = parent
        self.children = {}
        self.node_link = None


def build_fptree_from_file_onepass_fast(transactions_file: str, header_counts: dict):
    """
    Robusto (Issue 2): parsea a lista de int dentro del try.
    Nota: transactions_file ya viene filtrado a L1, así que no se chequea 'item in header'.
    """
    log = _get_logger()
    header_table = {item: [cnt, None] for item, cnt in header_counts.items()}
    root = FPNode(None, None)
    bad_lines = 0

    def update_header(item, node):
        head = header_table[item][1]
        if head is None:
            header_table[item][1] = node
        else:
            while head.node_link is not None:
                head = head.node_link
            head.node_link = node

    with open(transactions_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Construyendo FP-tree", dynamic_ncols=True):
            s = line.strip()
            if not s:
                continue

            try:
                tx = [int(x) for x in s.split()]
            except ValueError:
                bad_lines += 1
                continue

            curr = root
            for item in tx:
                if item in curr.children:
                    child = curr.children[item]
                else:
                    child = FPNode(item, curr)
                    curr.children[item] = child
                    update_header(item, child)
                child.count += 1
                curr = child

    if bad_lines:
        log(f"⚠️ Líneas inválidas ignoradas en {transactions_file}: {bad_lines:,}")

    return root, header_table


def ascend_path(node):
    path = []
    curr = node.parent
    while curr is not None and curr.item is not None:
        path.append(curr.item)
        curr = curr.parent
    return path


def conditional_pattern_base(item, header_table):
    base = []
    node = header_table[item][1]
    while node is not None:
        path = ascend_path(node)
        if path:
            base.append((path, node.count))
        node = node.node_link
    return base


def build_fptree_weighted(transactions_with_counts, min_count):
    header = defaultdict(int)
    for tx, cnt in transactions_with_counts:
        for it in tx:
            header[it] += cnt

    header = {it: total for it, total in header.items() if total >= min_count}
    if not header:
        return None, None

    header_table = {it: [total, None] for it, total in header.items()}
    root = FPNode(None, None)

    def update_header(it, node):
        head = header_table[it][1]
        if head is None:
            header_table[it][1] = node
        else:
            while head.node_link is not None:
                head = head.node_link
            head.node_link = node

    for tx, cnt in transactions_with_counts:
        filtered = [i for i in tx if i in header_table]
        if not filtered:
            continue
        filtered.sort(key=lambda i: (header_table[i][0], i), reverse=True)

        curr = root
        for it in filtered:
            if it in curr.children:
                child = curr.children[it]
            else:
                child = FPNode(it, curr)
                curr.children[it] = child
                update_header(it, child)
            child.count += cnt
            curr = child

    return root, header_table


def mine_fptree(header_table, min_count, prefix, results, zmin, zmax):
    items = sorted(header_table.items(), key=lambda kv: kv[1][0])

    for item, (item_count, _) in items:
        new_prefix = prefix + (item,)
        if zmin <= len(new_prefix) <= zmax:
            results[new_prefix] = item_count
        if len(new_prefix) == zmax:
            continue

        base = conditional_pattern_base(item, header_table)
        if not base:
            continue

        _, cond_header_table = build_fptree_weighted(base, min_count)
        if cond_header_table:
            mine_fptree(cond_header_table, min_count, new_prefix, results, zmin, zmax)


def fpgrowth_global_no_matrix(transactions_file: str, header_counts: dict, min_count: int, zmin: int, zmax: int):
    log = _get_logger()

    log("🌲 Construyendo FP-tree global (1 pasada, robusto)...")
    _, header_table = build_fptree_from_file_onepass_fast(transactions_file, header_counts)
    if not header_table:
        log("⚠️ header_table vacío: no hay items frecuentes.")
        return {}

    log(f"✅ FP-tree construido. Items en header: {len(header_table):,}")
    log_system_usage("Después de construir FP-tree:")

    log("⛏️ Minando itemsets...")
    results = {}
    mine_fptree(header_table, min_count, prefix=tuple(), results=results, zmin=zmin, zmax=zmax)
    return results


# ===================== OUTPUT =====================
def write_final_output(results_idx_counts: dict, client_ids: list, output_file: str):
    items_sorted = sorted(
        results_idx_counts.items(),
        key=lambda kv: (kv[1], len(kv[0]), kv[0]),
        reverse=True,
    )
    with open(output_file, "w", encoding="utf-8") as out:
        for idx_tuple, count in items_sorted:
            itemset = [client_ids[i] for i in idx_tuple]
            out.write(json.dumps([itemset, int(count)]) + "\n")


# ===================== MAIN =====================
if __name__ == "__main__":
    t0 = time.perf_counter()
    log = _get_logger()

    log("🚀 Pipeline: FP-Growth global (2024 + 2025 juntos) sin matriz...")
    log(f"📁 Entradas: {', '.join(MAIN_FILES)}")
    log_system_usage("Inicio:")

    # 0) Universo de items
    client_ids, cid_to_idx = load_client_ids(MAPPING_FILE)
    num_items = len(client_ids)
    log(f"✅ Items totales (client_id únicos en mapping): {num_items:,}")

    # 1) Sharding multi-zip — saltar si ya existen
    if SKIP_SHARDING_IF_EXISTS and shards_exist(SHARD_DIR, K_SHARDS):
        log(f"♻️ Shards ya existen en '{SHARD_DIR}' (≥{K_SHARDS}). Saltando sharding multi-zip.")
    else:
        preprocess_multiple_zips_to_shards(MAIN_FILES, SHARD_DIR, cid_to_idx, k=K_SHARDS)
    log_system_usage("Después de sharding:")

    # 2) Pass 1: conteos globales (N y soporte por item) SOBRE ambos años
    log("🔢 Pass 1: contando soporte global por item (desde shards, ambos años)...")
    item_counts, N, malformed, out_of_range = shards_pass1_counts(SHARD_DIR, num_items)
    if N == 0:
        raise RuntimeError("No se encontraron usuarios/transacciones.")

    support_eff = SUPPORT
    min_count = int(support_eff * N) or 1

    L1 = [i for i, c in enumerate(item_counts) if c >= min_count]
    log(f"✅ N usuarios (transacciones unificadas): {N:,}")
    log(f"✅ min_support={support_eff} → min_count={min_count:,}")
    log(f"✅ Items frecuentes (L1): {len(L1):,}")
    if malformed:
        log(f"⚠️ Líneas malformadas ignoradas: {malformed:,}")
    if out_of_range:
        log(f"⚠️ Índices fuera de rango ignorados: {out_of_range:,}")

    # Guardrail #1: si L1 es enorme, subimos support
    if len(L1) > GUARD_MAX_L1:
        support_eff = SUPPORT * GUARD_SUPPORT_MULT
        min_count = int(support_eff * N) or 1
        L1 = [i for i, c in enumerate(item_counts) if c >= min_count]
        log(f"🛡️ Guardrail: L1 muy grande → min_support={support_eff} (min_count={min_count:,})")
        log(f"🛡️ Nuevo L1: {len(L1):,}")

    header_counts = {i: item_counts[i] for i in L1}

    # Máscara booleana para filtrar (más rápida que set)
    is_freq_mask = None
    if USE_BOOL_MASK_FOR_L1:
        is_freq_mask = [False] * num_items
        for i in L1:
            is_freq_mask[i] = True

    # 3) Pass 2 (resume) + stats (para guardrails de densidad)
    avg_apps = None
    max_apps = None
    N_written = None

    if SKIP_TRANSACTIONS_IF_EXISTS and os.path.exists(TRANSACTIONS_FILE):
        n_lines = count_lines_fast(TRANSACTIONS_FILE)
        if n_lines > 0:
            log(f"♻️ {TRANSACTIONS_FILE} ya existe ({n_lines:,} líneas).")

            stats = load_transactions_stats(STATS_FILE)
            if stats and int(stats.get("N", -1)) == n_lines:
                avg_apps = float(stats["avg_apps"])
                max_apps = int(stats["max_apps"])
                N_written = int(stats["N"])
                log(f"✅ Stats cargadas desde {STATS_FILE}: avg={avg_apps:.2f}, max={max_apps}, N={N_written:,}")
            else:
                log("📊 Calculando stats desde transactions_fp (1 pasada)...")
                avg_apps, max_apps, N_written, bad = compute_stats_from_transactions(TRANSACTIONS_FILE)
                log(f"✅ Stats: avg={avg_apps:.2f}, max={max_apps}, N={N_written:,} (bad_lines={bad:,})")
                save_transactions_stats(STATS_FILE, {"avg_apps": avg_apps, "max_apps": max_apps, "N": N_written})
                log(f"💾 Stats guardadas en {STATS_FILE}")
        else:
            log(f"⚠️ {TRANSACTIONS_FILE} existe pero está vacío. Rehaciendo Pass 2.")
            avg_apps, max_apps, N_written = write_transactions_file_from_shards(
                shard_dir=SHARD_DIR,
                transactions_file=TRANSACTIONS_FILE,
                header_counts=header_counts,
                is_freq_mask=is_freq_mask,
            )
            save_transactions_stats(STATS_FILE, {"avg_apps": avg_apps, "max_apps": max_apps, "N": N_written})
            log(f"💾 Stats guardadas en {STATS_FILE}")
    else:
        log("🧾 Pass 2: escribiendo transacciones filtradas (unificadas 2024+2025)...")
        avg_apps, max_apps, N_written = write_transactions_file_from_shards(
            shard_dir=SHARD_DIR,
            transactions_file=TRANSACTIONS_FILE,
            header_counts=header_counts,
            is_freq_mask=is_freq_mask,
        )
        log(f"✅ Transacciones escritas: {N_written:,} (de N={N:,})")
        log(f"📊 Apps/usuario (tras filtro L1): promedio={avg_apps:.2f}, máximo={max_apps}")
        save_transactions_stats(STATS_FILE, {"avg_apps": avg_apps, "max_apps": max_apps, "N": N_written})
        log(f"💾 Stats guardadas en {STATS_FILE}")
        log_system_usage("Después de transacciones filtradas:")

    # Guardrail #2: densidad
    zmax_eff = ZMAX
    if avg_apps is not None and max_apps is not None:
        if avg_apps > GUARD_MAX_AVG_APPS or max_apps > GUARD_MAX_APPS:
            zmax_eff = min(ZMAX, 3)
            log(f"🛡️ Guardrail: transacciones densas → limitando ZMAX a {zmax_eff}")

    # 4) Minería global sobre ambos años
    if SKIP_MINING_IF_OUTPUT_EXISTS and os.path.exists(FINAL_OUTPUT):
        log(f"♻️ {FINAL_OUTPUT} ya existe. Saltando minería.")
    else:
        results = fpgrowth_global_no_matrix(
            transactions_file=TRANSACTIONS_FILE,
            header_counts=header_counts,
            min_count=min_count,
            zmin=ZMIN,
            zmax=zmax_eff,
        )
        log(f"✅ Itemsets encontrados (z={ZMIN}..{zmax_eff}): {len(results):,}")
        log_system_usage("Después de minería:")

        write_final_output(results, client_ids, FINAL_OUTPUT)

    # 5) Traducción
    traducir_final_output(FINAL_OUTPUT, TRANSLATED_OUTPUT, MAPPING_FILE)

    elapsed = time.perf_counter() - t0
    log("🏁 Finalizado")
    log(f"⏱️ Tiempo total: {_fmt_hms(elapsed)} ({elapsed:.2f} s)")
