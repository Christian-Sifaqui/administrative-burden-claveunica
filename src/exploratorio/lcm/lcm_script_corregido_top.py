import os
import io
import gc
import csv
import json
import time
import psutil
import zipfile
import hashlib
from bisect import bisect_left
from collections import defaultdict, OrderedDict
from tqdm import tqdm

# ===================== CONFIG =====================
MAIN_FILES = [
    "cu_user_client-2024.zip",
    "cu_user_client-2025.zip",
]

MAPPING_FILE = "dim_integracion_cu.csv"

SHARD_DIR = "shards_2024_2025"
K_SHARDS = 1024
MAX_OPEN_FILES = 64
FILE_BUFFERING = 256 * 1024  # 256 KB

TX_BUCKET_DIR = "tx_buckets_lcm"
TX_BUCKETS = 256

WEIGHTED_TX_FILE = "weighted_transactions_lcm_2024_2025.tsv"
WEIGHTED_STATS_FILE = "weighted_transactions_lcm_stats_2024_2025.json"
RUN_METADATA_FILE = "lcm_run_metadata_2024_2025.json"

FINAL_OUTPUT = "final_output_lcm_closed_2024_2025.jsonl"
TRANSLATED_OUTPUT = "final_output_lcm_closed_traducido_2024_2025.jsonl"

SUPPORT = 0.0005
ZMIN = 1
ZMAX = 5

# Guardrails
GUARD_MAX_L1 = 1200
GUARD_MAX_AVG_APPS = 25.0
GUARD_MAX_APPS = 200
GUARD_SUPPORT_MULT = 2.0
GUARD_MAX_UNIQUE_TX = 1_000_000
GUARD_MAX_WEIGHTED_DB_ROWS = 1_200_000

# Resume
SKIP_SHARDING_IF_EXISTS = True
SKIP_WEIGHTED_TX_IF_EXISTS = True
SKIP_MINING_IF_OUTPUT_EXISTS = False

USE_BOOL_MASK_FOR_L1 = True

# GC tuning
GC_EVERY_N_SHARDS_PASS1 = 100
GC_EVERY_N_SHARDS_PASS2 = 50
GC_EVERY_N_BUCKETS_MERGE = 50

# Optional fast hash
try:
    import xxhash  # type: ignore
    HAS_XXHASH = True
except Exception:
    xxhash = None
    HAS_XXHASH = False


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


def log(msg: str):
    _get_logger()(msg)


def log_system_usage(prefix=""):
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.3)
    _get_logger()(
        f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024 ** 3):.2f} GB), CPU: {cpu}%"
    )


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def build_run_metadata(main_files, mapping_file, k_shards, tx_buckets, support, zmin, zmax, num_items):
    hash_backend = "xxhash" if HAS_XXHASH else "md5"
    return {
        "main_files": list(main_files),
        "mapping_file": mapping_file,
        "k_shards": int(k_shards),
        "tx_buckets": int(tx_buckets),
        "support": float(support),
        "zmin": int(zmin),
        "zmax": int(zmax),
        "num_items": int(num_items),
        "hash_backend": hash_backend,
    }


def same_run_metadata(old_meta, new_meta):
    return old_meta == new_meta


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
            items, support_abs = json.loads(line)
            traducidos = [mapping.get(i, f"{i} --> Desconocido") for i in items]
            fout.write(json.dumps([traducidos, support_abs], ensure_ascii=False) + "\n")
    log(f"✅ Archivo traducido guardado en: {output_file}")


def _stable_shard(run: str, k: int) -> int:
    raw = run.encode("utf-8")
    if HAS_XXHASH:
        return xxhash.xxh64(raw).intdigest() % k
    d = hashlib.md5(raw).digest()
    return int.from_bytes(d[:8], "big") % k


def _stable_tuple_bucket_from_bytes(raw: bytes, k: int) -> int:
    if HAS_XXHASH:
        return xxhash.xxh64(raw).intdigest() % k
    d = hashlib.md5(raw).digest()
    return int.from_bytes(d[:8], "big") % k


def iter_set_bits(bits: int):
    x = bits
    while x:
        lsb = x & -x
        i = lsb.bit_length() - 1
        yield i
        x ^= lsb


def shards_exist(shard_dir: str, k: int) -> bool:
    if not os.path.isdir(shard_dir):
        return False
    files = [f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")]
    return len(files) >= k


def tx_buckets_exist(tx_bucket_dir: str, k: int) -> bool:
    if not os.path.isdir(tx_bucket_dir):
        return False
    files = [f for f in os.listdir(tx_bucket_dir) if f.startswith("txb_") and f.endswith(".tsv")]
    return len(files) >= k


class LRUTextWriter:
    def __init__(self, out_dir: str, prefix: str, suffix: str, k: int, max_open: int = 64):
        self.out_dir = out_dir
        self.prefix = prefix
        self.suffix = suffix
        self.k = k
        self.max_open = max_open
        self.handles = OrderedDict()
        os.makedirs(out_dir, exist_ok=True)

    def _path(self, idx: int) -> str:
        return os.path.join(self.out_dir, f"{self.prefix}_{idx:04d}.{self.suffix}")

    def write_line(self, idx: int, line: str):
        if idx in self.handles:
            fp = self.handles.pop(idx)
            self.handles[idx] = fp
        else:
            if len(self.handles) >= self.max_open:
                _, old_fp = self.handles.popitem(last=False)
                old_fp.close()
            fp = open(self._path(idx), "a", encoding="utf-8", buffering=FILE_BUFFERING)
            self.handles[idx] = fp
        fp.write(line)

    def close(self):
        for fp in self.handles.values():
            fp.close()
        self.handles.clear()


# ===================== FASE 1: MULTI-ZIP -> SHARDS =====================
def preprocess_multiple_zips_to_shards(main_zip_paths, shard_dir: str, cid_to_idx: dict, k: int):
    os.makedirs(shard_dir, exist_ok=True)

    writer = LRUTextWriter(shard_dir, "pairs", "tsv", k=k, max_open=MAX_OPEN_FILES)

    stats = {
        "raw_rows": 0,
        "rows_valid_run": 0,
        "rows_missing_run": 0,
        "rows_missing_client_field": 0,
        "unknown_items": 0,
        "pair_rows_written": 0,
        "csv_missing_expected_cols": 0,
        "inner_zip_without_csv": 0,
    }

    for main_zip_path in main_zip_paths:
        log(f"📦 Procesando ZIP principal: {main_zip_path}")
        with zipfile.ZipFile(main_zip_path, "r") as top_zip:
            inner_zip_names = [n for n in top_zip.namelist() if n.lower().endswith(".zip")]
            inner_zip_names.sort()
            log(f"   └─ ZIPs internos: {len(inner_zip_names)}")

            for inner_name in tqdm(
                inner_zip_names,
                desc=f"Leyendo internos ({os.path.basename(main_zip_path)})",
                dynamic_ncols=True,
            ):
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    continue

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith(".csv")]
                    if not csv_members:
                        stats["inner_zip_without_csv"] += 1
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
                                stats["csv_missing_expected_cols"] += 1
                                continue

                            for row in reader:
                                stats["raw_rows"] += 1
                                if not row or len(row) <= max(idx_run, idx_client):
                                    continue

                                run = str(row[idx_run]).strip()
                                if not run:
                                    stats["rows_missing_run"] += 1
                                    continue
                                stats["rows_valid_run"] += 1

                                client_field = str(row[idx_client]).strip().strip('"')
                                if not client_field:
                                    stats["rows_missing_client_field"] += 1
                                    continue

                                shard = _stable_shard(run, k)

                                for cid in client_field.split(","):
                                    cid = cid.strip()
                                    if not cid:
                                        continue
                                    item_idx = cid_to_idx.get(cid)
                                    if item_idx is None:
                                        stats["unknown_items"] += 1
                                        continue

                                    writer.write_line(shard, f"{run}\t{item_idx}\n")
                                    stats["pair_rows_written"] += 1

    writer.close()
    log(f"✅ Shards escritos en: {shard_dir} (K={k}, max_open={MAX_OPEN_FILES})")
    return stats


# ===================== FASE 2: SHARDS -> CONTEOS GLOBALES =====================
def shards_pass1_counts(shard_dir: str, num_items: int):
    shard_files = sorted(
        f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")
    )

    item_counts = [0] * num_items
    total_users = 0
    malformed_lines = 0
    out_of_range = 0

    for shard_idx, sf in enumerate(tqdm(shard_files, desc="Pass 1 (counts)", dynamic_ncols=True), 1):
        path = os.path.join(shard_dir, sf)
        user_bits = {}

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                tab_pos = line.find("\t")
                if tab_pos <= 0:
                    malformed_lines += 1
                    continue

                run = line[:tab_pos]
                b_str = line[tab_pos + 1:].rstrip("\r\n")
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
            for i in iter_set_bits(bits):
                item_counts[i] += 1

        user_bits.clear()
        if shard_idx % GC_EVERY_N_SHARDS_PASS1 == 0:
            gc.collect()

    return item_counts, total_users, malformed_lines, out_of_range


# ===================== FASE 3: SHARDS -> BUCKETS DE TRANSACCIONES =====================
def build_tx_bucket_files_from_shards(
    shard_dir: str,
    tx_bucket_dir: str,
    header_counts: dict,
    num_items: int,
    is_freq_mask,
    tx_buckets: int = 256,
):
    os.makedirs(tx_bucket_dir, exist_ok=True)

    shard_files = sorted(
        f for f in os.listdir(shard_dir) if f.startswith("pairs_") and f.endswith(".tsv")
    )

    writer = LRUTextWriter(tx_bucket_dir, "txb", "tsv", k=tx_buckets, max_open=MAX_OPEN_FILES)

    total_users = 0
    total_apps = 0
    max_apps = 0
    local_unique_total = 0

    for shard_idx, sf in enumerate(tqdm(shard_files, desc="Pass 2 (tx buckets)", dynamic_ncols=True), 1):
        path = os.path.join(shard_dir, sf)
        user_bits = {}
        local_counter = defaultdict(int)

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                tab_pos = line.find("\t")
                if tab_pos <= 0:
                    continue

                run = line[:tab_pos]
                b_str = line[tab_pos + 1:].rstrip("\r\n")
                if not b_str:
                    continue

                try:
                    b = int(b_str)
                except ValueError:
                    continue

                if 0 <= b < num_items and is_freq_mask[b]:
                    user_bits[run] = user_bits.get(run, 0) | (1 << b)

        for bits in user_bits.values():
            items = list(iter_set_bits(bits))
            if not items:
                continue

            items.sort()  # orden entero ascendente; consistente con bisect_left
            tx_tuple = tuple(items)
            local_counter[tx_tuple] += 1

            n = len(items)
            total_users += 1
            total_apps += n
            if n > max_apps:
                max_apps = n

        local_unique_total += len(local_counter)

        for tx_tuple, cnt in local_counter.items():
            tx_str = " ".join(map(str, tx_tuple))
            tx_bytes = tx_str.encode("utf-8")
            bucket_idx = _stable_tuple_bucket_from_bytes(tx_bytes, tx_buckets)
            writer.write_line(bucket_idx, tx_str + "\t" + str(cnt) + "\n")

        user_bits.clear()
        local_counter.clear()
        if shard_idx % GC_EVERY_N_SHARDS_PASS2 == 0:
            gc.collect()

    writer.close()

    avg_apps = (total_apps / total_users) if total_users else 0.0
    stats = {
        "avg_apps": avg_apps,
        "max_apps": max_apps,
        "N": total_users,
        "local_unique_total_premerge": local_unique_total,
        "tx_buckets": tx_buckets,
    }
    return stats


def merge_tx_buckets_to_weighted_db(tx_bucket_dir: str, weighted_tx_file: str):
    bucket_files = sorted(
        f for f in os.listdir(tx_bucket_dir) if f.startswith("txb_") and f.endswith(".tsv")
    )

    unique_tx = 0
    total_weight = 0

    with open(weighted_tx_file, "w", encoding="utf-8") as out:
        for bucket_idx, bf in enumerate(tqdm(bucket_files, desc="Merge tx buckets", dynamic_ncols=True), 1):
            path = os.path.join(tx_bucket_dir, bf)
            counter = defaultdict(int)

            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line:
                        continue

                    tab_pos = line.rfind("\t")
                    if tab_pos <= 0:
                        continue

                    tx_str = line[:tab_pos]
                    cnt_str = line[tab_pos + 1:]

                    try:
                        cnt = int(cnt_str)
                    except ValueError:
                        continue

                    counter[tx_str] += cnt

            for tx_str, cnt in counter.items():
                out.write(f"{tx_str}\t{cnt}\n")
                unique_tx += 1
                total_weight += cnt

            counter.clear()
            if bucket_idx % GC_EVERY_N_BUCKETS_MERGE == 0:
                gc.collect()

    return {"unique_tx": unique_tx, "total_weight": total_weight}


# ===================== FASE 4: CARGA WEIGHTED DB =====================
def load_weighted_transactions(weighted_tx_file: str):
    weighted_db = []
    bad_lines = 0

    with open(weighted_tx_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Cargando weighted DB", dynamic_ncols=True):
            line = line.rstrip("\n")
            if not line:
                continue

            tab_pos = line.rfind("\t")
            if tab_pos <= 0:
                bad_lines += 1
                continue

            tx_str = line[:tab_pos]
            cnt_str = line[tab_pos + 1:]

            try:
                cnt = int(cnt_str)
                tx = tuple(int(x) for x in tx_str.split())
            except ValueError:
                bad_lines += 1
                continue

            if tx and cnt > 0:
                weighted_db.append((tx, cnt))

    return weighted_db, bad_lines


# ===================== LCM-STYLE CLOSED MINER =====================
def project_db_on_item(db, item):
    projected = []
    supp = 0

    for tx, cnt in db:
        pos = bisect_left(tx, item)
        if pos < len(tx) and tx[pos] == item:
            supp += cnt
            suffix = tx[pos + 1:]
            if suffix:
                projected.append((suffix, cnt))

    return projected, supp


def item_supports_in_db(db):
    counts = defaultdict(int)
    for tx, cnt in db:
        for x in tx:
            counts[x] += cnt
    return counts


def lcm_mine_closed(
    db,
    min_count,
    zmin,
    zmax,
    prefix=(),
    prefix_support=None,
    out_results=None,
):
    """
    Variante LCM-style adaptada a Python.
    Devuelve itemsets frecuentes cerrados con soporte absoluto.
    """
    if out_results is None:
        out_results = {}

    if prefix_support is None:
        counts = item_supports_in_db(db)
        candidates = sorted(
            ((i, c) for i, c in counts.items() if c >= min_count),
            key=lambda x: (x[1], x[0]),
        )

        for item, supp in candidates:
            proj_db, _ = project_db_on_item(db, item)
            lcm_mine_closed(
                db=proj_db,
                min_count=min_count,
                zmin=zmin,
                zmax=zmax,
                prefix=(item,),
                prefix_support=supp,
                out_results=out_results,
            )
        return out_results

    counts = item_supports_in_db(db) if db else {}

    closure_ext = [i for i, c in counts.items() if c == prefix_support]
    closure_ext.sort()
    if __debug__ and prefix and closure_ext:
        assert closure_ext[0] > prefix[-1], "Closure extension fuera de orden respecto del prefijo"
    closed_prefix = prefix + tuple(closure_ext)

    if zmin <= len(closed_prefix) <= zmax:
        prev = out_results.get(closed_prefix)
        if prev is None or prefix_support > prev:
            out_results[closed_prefix] = prefix_support

    if len(closed_prefix) >= zmax or not db:
        return out_results

    last_item = closed_prefix[-1] if closed_prefix else -1

    candidates = sorted(
        ((item, c) for item, c in counts.items() if c >= min_count and item > last_item),
        key=lambda t: (t[1], t[0]),
    )

    for item, supp in candidates:
        proj_db, _ = project_db_on_item(db, item)
        new_prefix = closed_prefix + (item,)
        if len(new_prefix) > zmax:
            continue

        lcm_mine_closed(
            db=proj_db,
            min_count=min_count,
            zmin=zmin,
            zmax=zmax,
            prefix=new_prefix,
            prefix_support=supp,
            out_results=out_results,
        )

    return out_results


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
            out.write(json.dumps([itemset, int(count)], ensure_ascii=False) + "\n")


# ===================== MAIN =====================
if __name__ == "__main__":
    t0 = time.perf_counter()

    log("🚀 Pipeline: LCM global (2024 + 2025 juntos) sin matriz...")
    log(f"📁 Entradas: {', '.join(MAIN_FILES)}")
    if HAS_XXHASH:
        log("⚡ Hash rápido disponible: xxhash")
    else:
        log("ℹ️ Hash rápido no disponible; se usa MD5 estable")
    log_system_usage("Inicio:")

    # 0) Universo de ítems
    client_ids, cid_to_idx = load_client_ids(MAPPING_FILE)
    num_items = len(client_ids)
    log(f"✅ Items totales (client_id únicos en mapping): {num_items:,}")

    current_meta = build_run_metadata(
        main_files=MAIN_FILES,
        mapping_file=MAPPING_FILE,
        k_shards=K_SHARDS,
        tx_buckets=TX_BUCKETS,
        support=SUPPORT,
        zmin=ZMIN,
        zmax=ZMAX,
        num_items=num_items,
    )
    saved_meta = load_json(RUN_METADATA_FILE)
    can_resume = saved_meta is not None and same_run_metadata(saved_meta, current_meta)

    if saved_meta is None:
        log("ℹ️ No hay metadatos previos de corrida.")
    elif can_resume:
        log("♻️ Metadatos compatibles detectados: se puede reutilizar intermedios.")
    else:
        log("⚠️ Los metadatos previos no coinciden con la configuración actual. No se reutilizarán intermedios incompatibles.")

    # 1) Sharding multi-zip
    preprocess_stats = None
    if SKIP_SHARDING_IF_EXISTS and can_resume and shards_exist(SHARD_DIR, K_SHARDS):
        log(f"♻️ Shards ya existen en '{SHARD_DIR}' (≥{K_SHARDS}) y son compatibles. Saltando sharding.")
    else:
        preprocess_stats = preprocess_multiple_zips_to_shards(MAIN_FILES, SHARD_DIR, cid_to_idx, k=K_SHARDS)
        save_json(RUN_METADATA_FILE, current_meta)

    if preprocess_stats:
        log("=" * 90)
        log("REPORTE DIAGNÓSTICO INGESTA")
        log("=" * 90)
        log(f"Filas leídas totales                : {preprocess_stats['raw_rows']:,}")
        log(f"Filas con run válido                : {preprocess_stats['rows_valid_run']:,}")
        log(f"Filas sin run                       : {preprocess_stats['rows_missing_run']:,}")
        log(f"Filas sin client_id_traza           : {preprocess_stats['rows_missing_client_field']:,}")
        log(f"Ítems desconocidos omitidos         : {preprocess_stats['unknown_items']:,}")
        log(f"Pares run-item observados           : {preprocess_stats['pair_rows_written']:,}")
        log(f"CSV sin columnas esperadas          : {preprocess_stats['csv_missing_expected_cols']:,}")
        log(f"ZIP internos sin CSV                : {preprocess_stats['inner_zip_without_csv']:,}")

    log_system_usage("Después de sharding:")

    # 2) Pass 1: conteos globales
    log("🔢 Pass 1: contando soporte global por item...")
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

    # Guardrail #1: si L1 es enorme, subir soporte
    if len(L1) > GUARD_MAX_L1:
        support_eff = SUPPORT * GUARD_SUPPORT_MULT
        min_count = int(support_eff * N) or 1
        L1 = [i for i, c in enumerate(item_counts) if c >= min_count]
        log(f"🛡️ Guardrail: L1 muy grande → min_support={support_eff} (min_count={min_count:,})")
        log(f"🛡️ Nuevo L1: {len(L1):,}")

    if not USE_BOOL_MASK_FOR_L1:
        raise RuntimeError("Este script asume USE_BOOL_MASK_FOR_L1=True para el hot path de Pass 2.")

    is_freq_mask = [False] * num_items
    for i in L1:
        is_freq_mask[i] = True

    # 3) Construcción de weighted DB
    weighted_stats = None
    if (
        SKIP_WEIGHTED_TX_IF_EXISTS
        and can_resume
        and os.path.exists(WEIGHTED_TX_FILE)
        and os.path.exists(WEIGHTED_STATS_FILE)
        and tx_buckets_exist(TX_BUCKET_DIR, TX_BUCKETS)
    ):
        weighted_stats = load_json(WEIGHTED_STATS_FILE)
        if weighted_stats:
            log(f"♻️ Reutilizando {WEIGHTED_TX_FILE} y stats compatibles.")
    else:
        log("🧾 Pass 2: reconstruyendo y comprimiendo transacciones...")
        tx_stats = build_tx_bucket_files_from_shards(
            shard_dir=SHARD_DIR,
            tx_bucket_dir=TX_BUCKET_DIR,
            header_counts={i: item_counts[i] for i in L1},
            num_items=num_items,
            is_freq_mask=is_freq_mask,
            tx_buckets=TX_BUCKETS,
        )

        log(f"✅ Transacciones reconstruidas: {tx_stats['N']:,}")
        log(f"📊 Apps/usuario tras L1: promedio={tx_stats['avg_apps']:.2f}, máximo={tx_stats['max_apps']}")
        log(
            f"📦 Transacciones únicas locales acumuladas (pre-merge): "
            f"{tx_stats['local_unique_total_premerge']:,}"
        )

        if tx_stats["avg_apps"] > GUARD_MAX_AVG_APPS or tx_stats["max_apps"] > GUARD_MAX_APPS:
            log("🛡️ Guardrail: transacciones densas detectadas; probablemente conviene limitar ZMAX a 3.")

        merge_stats = merge_tx_buckets_to_weighted_db(TX_BUCKET_DIR, WEIGHTED_TX_FILE)
        weighted_stats = {
            **tx_stats,
            **merge_stats,
            "support_eff": support_eff,
            "min_count": min_count,
        }
        save_json(WEIGHTED_STATS_FILE, weighted_stats)
        save_json(RUN_METADATA_FILE, current_meta)
        log(
            f"✅ Weighted DB creada: filas únicas={merge_stats['unique_tx']:,}, "
            f"peso total={merge_stats['total_weight']:,}"
        )

    log_system_usage("Después de compresión transaccional:")

    if weighted_stats is None:
        raise RuntimeError("No se pudieron obtener stats de la weighted DB.")

    avg_apps = weighted_stats["avg_apps"]
    max_apps = weighted_stats["max_apps"]
    unique_tx = weighted_stats["unique_tx"]

    zmax_eff = ZMAX
    if avg_apps > GUARD_MAX_AVG_APPS or max_apps > GUARD_MAX_APPS:
        zmax_eff = min(ZMAX, 3)
        log(f"🛡️ Guardrail: transacciones densas → limitando ZMAX a {zmax_eff}")

    if unique_tx > GUARD_MAX_UNIQUE_TX:
        suggested_support = support_eff * GUARD_SUPPORT_MULT
        raise RuntimeError(
            "La base comprimida quedó demasiado grande para minar LCM de forma segura. "
            "Debes rehacer la corrida completa desde Pass 1/Pass 2 con mayor support "
            f"(sugerido: {suggested_support}) o menor ZMAX."
        )

    # 4) Cargar weighted DB
    weighted_db, bad_lines = load_weighted_transactions(WEIGHTED_TX_FILE)

    if bad_lines:
        log(f"⚠️ Líneas inválidas ignoradas en weighted DB: {bad_lines:,}")

    if len(weighted_db) == 0:
        raise RuntimeError("La weighted DB quedó vacía.")

    if len(weighted_db) > GUARD_MAX_WEIGHTED_DB_ROWS:
        raise RuntimeError(
            f"La weighted DB tiene {len(weighted_db):,} filas únicas, sobre el guardrail "
            f"{GUARD_MAX_WEIGHTED_DB_ROWS:,}. Sube support o reduce ZMAX."
        )

    log(f"✅ Weighted DB cargada en memoria: {len(weighted_db):,} filas únicas")
    log_system_usage("Después de cargar weighted DB:")

    # 5) Minería
    if SKIP_MINING_IF_OUTPUT_EXISTS and os.path.exists(FINAL_OUTPUT):
        log(f"♻️ {FINAL_OUTPUT} ya existe. Saltando minería.")
    else:
        log("⛏️ Minando itemsets frecuentes cerrados (LCM-style, no todos los frecuentes)...")
        results = lcm_mine_closed(
            db=weighted_db,
            min_count=min_count,
            zmin=ZMIN,
            zmax=zmax_eff,
        )

        log(f"✅ Itemsets frecuentes cerrados encontrados: {len(results):,}")
        log_system_usage("Después de minería:")

        write_final_output(results, client_ids, FINAL_OUTPUT)
        log(f"💾 Resultado guardado en {FINAL_OUTPUT}")

    # 6) Traducción
    if os.path.exists(FINAL_OUTPUT):
        traducir_final_output(FINAL_OUTPUT, TRANSLATED_OUTPUT, MAPPING_FILE)
    else:
        log(f"⚠️ No existe {FINAL_OUTPUT}; se omite traducción.")

    elapsed = time.perf_counter() - t0
    log("🏁 Finalizado")
    log(f"⏱️ Tiempo total: {_fmt_hms(elapsed)} ({elapsed:.2f} s)")
