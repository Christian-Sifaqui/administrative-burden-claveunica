import os
import io
import json
import gc
import csv
import psutil
import zipfile
import argparse
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Pool
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.preprocessing import TransactionEncoder
import time
from typing import List, Tuple, Optional

# -------- DEFAULT CONFIG (se puede sobreescribir por CLI) --------
MAPPING_FILE = 'dim_integracion_cu.csv'
CHUNK_DIR = 'chunks'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'final_output.jsonl'

CHUNKSIZE = 100_000  # filas leídas aprox. antes de volcar a un chunk
SUPPORT = 0.0005
ZMIN = 1
ZMAX = 5
NUM_WORKERS = os.cpu_count()
# -------------------------


# ---------------- UTILIDADES ----------------
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
    cpu = psutil.cpu_percent(interval=1)
    _get_logger()(f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024**3):.2f} GB), CPU: {cpu}%")


def load_client_names_mapping(filename):
    mapping = {}
    if not os.path.exists(filename):
        _get_logger()(f"⚠️ No se encontró mapping '{filename}', se dejarán IDs sin traducir.")
        return mapping
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row.get('nombre_app', '').strip()
            inst = row.get('institucion', '').strip()
            client_id = row.get('client_id', '').strip()
            if client_id:
                mapping[client_id] = f"{name}: {inst}" if name or inst else client_id
    return mapping


# ----------------- DETECCIÓN DE COLUMNA RUN RANGO -----------------
RUN_RANGE_CANDIDATES = [
    'run_rango_millones',
    'rango_run_millones',
    'run_rango',
    'rango_run',
    'run_millones',
    'rango_de_run',
    'run_rango_millon',
    'run_rango_mill',
]

def _detect_run_range_index(header: List[str]) -> Optional[int]:
    """Devuelve el índice de la columna que contiene el RANGO DE RUN (en millones).
    Estrategia:
      1) Buscar por nombre entre candidatos (case-insensitive).
      2) Si no se encuentra, asumir 3a columna (índice 2) tal como en tu ejemplo.
    """
    header_lower = [h.strip().strip('"').lower() for h in header]
    for cand in RUN_RANGE_CANDIDATES:
        if cand in header_lower:
            return header_lower.index(cand)
    # fallback: tercera columna
    return 2 if len(header) > 2 else None


# ===================== PREPROCESAMIENTO PARA ZIP ANIDADO =====================

def _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=_get_logger()):
    """Escribe chunk_{i}.jsonl (nombre temporal sin padding)."""
    if not user_to_client_ids:
        return chunk_index, None
    os.makedirs(chunk_dir, exist_ok=True)
    tmp_name = f"chunk_{chunk_index}.jsonl"  # temporal; luego renombramos con padding uniforme
    chunk_path = os.path.join(chunk_dir, tmp_name)
    with open(chunk_path, 'w', encoding='utf-8') as f:
        for client_ids in user_to_client_ids.values():
            print(json.dumps(list(client_ids)), file=f)
    log(f"🧩 Chunk escrito (tmp): {chunk_path} ( transacciones={len(user_to_client_ids)} )")
    user_to_client_ids.clear()
    gc.collect()
    return chunk_index + 1, chunk_path


def _renombrar_chunks_con_padding(chunk_paths: List[str]):
    """Renombra todos los chunks temporales a nombres con padding uniforme.
       Ej: si hay 100 → ancho 3 -> chunk_000.jsonl ... chunk_099.jsonl
    """
    if not chunk_paths:
        return
    # ordenar por índice numérico (extraído del nombre tmp chunk_{i}.jsonl)
    def idx_of(p):
        base = os.path.basename(p)
        n = base.replace('chunk_', '').replace('.jsonl', '')
        return int(n)

    chunk_paths_sorted = sorted(chunk_paths, key=idx_of)
    total = len(chunk_paths_sorted)
    width = max(3, len(str(total - 1)))  # 100 -> 3, 1000 -> 4, etc.

    renamed = []
    for p in chunk_paths_sorted:
        i = idx_of(p)
        new_name = f"chunk_{str(i).zfill(width)}.jsonl"
        new_path = os.path.join(os.path.dirname(p), new_name)
        os.replace(p, new_path)
        renamed.append(new_path)
    _get_logger()(f"🔤 Renombrados {total} chunks con padding ancho={width}.")
    return renamed


def preprocess_top_zip_to_chunks(main_zip_path, chunk_dir, chunksize: int,
                                 run_min: float, run_max: float) -> List[str]:
    """
    Lee ZIP anidado, filtra por rango de RUN [run_min, run_max] y genera chunks JSONL
    con transacciones (conjuntos de client_ids por usuario RUN anonimizado).
    Devuelve la lista de paths de chunks (ya renombrados con padding).
    """
    os.makedirs(chunk_dir, exist_ok=True)

    user_to_client_ids = defaultdict(set)
    rows_since_last_flush = 0
    chunk_index = 0
    log = _get_logger()
    tmp_chunk_paths = []

    with zipfile.ZipFile(main_zip_path, 'r') as top_zip:
        inner_zip_names = [n for n in top_zip.namelist() if n.lower().endswith('.zip')]
        inner_zip_names.sort()
        log(f"📦 ZIP principal: {main_zip_path} — ZIPs internos: {len(inner_zip_names)}")

        with tqdm(inner_zip_names, desc="Leyendo ZIPs internos", dynamic_ncols=True) as bar:
            for inner_name in bar:
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    log(f"⚠️ No se pudo leer entrada: {inner_name}")
                    continue

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith('.csv')]
                    if not csv_members:
                        log(f"⚠️ Sin CSV dentro de {inner_name}")
                        continue
                    csv_name = csv_members[0]

                    with inner_zip.open(csv_name, 'r') as csv_file_bin:
                        with io.TextIOWrapper(csv_file_bin, encoding='utf-8', newline='') as csv_text:
                            reader = csv.reader(csv_text, delimiter=';', quotechar='"')
                            header = next(reader, None)
                            if header is None:
                                continue

                            # Detectar índice de columna RUN RANGO (en millones)
                            run_range_idx = _detect_run_range_index(header)
                            if run_range_idx is None:
                                log(f"⚠️ No se detectó columna de RANGO RUN en {csv_name}; se omite.")
                                continue

                            # Detectar columnas útiles para transacciones
                            # - ID usuario anonimizado (si existe) para agrupar, si no, se usa la cadena del rango
                            # - client_id_traza (servicios) para items
                            header_norm = [h.strip().strip('"') for h in header]
                            try:
                                idx_user = header_norm.index('run_anonimizado')  # agrupar por usuario
                            except ValueError:
                                idx_user = None
                            try:
                                idx_client = header_norm.index('client_id_traza')
                            except ValueError:
                                idx_client = None

                            if idx_client is None:
                                log(f"⚠️ No se encontró 'client_id_traza' en {csv_name}; se omite.")
                                continue

                            for row in reader:
                                if not row or len(row) <= max(run_range_idx, idx_client if idx_client else 0):
                                    continue
                                # filtro por rango de RUN (millones)
                                try:
                                    run_range_val = float(str(row[run_range_idx]).strip().strip('"'))
                                except Exception:
                                    continue
                                if not (run_min <= run_range_val <= run_max):
                                    continue

                                # clave de transacción: usuario anonimizado si existe, si no, algo estable
                                if idx_user is not None and len(row) > idx_user:
                                    txn_key = str(row[idx_user]).strip()
                                    if not txn_key:
                                        txn_key = f"__anon__{run_range_val:.1f}"
                                else:
                                    # fallback: agrupar por rango (menos ideal, pero evita perder sesiones)
                                    txn_key = f"__anon__{run_range_val:.1f}"

                                # items (servicios)
                                client_field = str(row[idx_client]).strip().strip('"')
                                if not client_field:
                                    continue
                                for cid in client_field.split(','):
                                    cid = cid.strip()
                                    if cid:
                                        user_to_client_ids[txn_key].add(cid)

                                rows_since_last_flush += 1
                                if rows_since_last_flush >= chunksize:
                                    chunk_index, path_out = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=log)
                                    if path_out:
                                        tmp_chunk_paths.append(path_out)
                                    rows_since_last_flush = 0

    # Flush final
    if user_to_client_ids:
        _, path_out = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=log)
        if path_out:
            tmp_chunk_paths.append(path_out)

    # Renombrar con padding
    padded_paths = _renombrar_chunks_con_padding(tmp_chunk_paths) or tmp_chunk_paths
    return padded_paths


# ===================== FP-GROWTH POR CHUNK =====================

def process_chunk_fpgrowth(chunk_file, output_file, supp=0.0005, zmin=1, zmax=5):
    if os.path.exists(output_file):
        return f"Ya procesado: {os.path.basename(chunk_file)}"

    log_system_usage(f"[Procesando {os.path.basename(chunk_file)}]")

    with open(chunk_file, 'r', encoding='utf-8') as f:
        transactions = [json.loads(line) for line in f]

    if not transactions:
        with open(output_file, 'w', encoding='utf-8') as _:
            pass
        return f"Vacío: {os.path.basename(chunk_file)}"

    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df = pd.DataFrame(te_ary, columns=te.columns_)

    freq_itemsets = fpgrowth(df, min_support=supp, use_colnames=True)
    freq_itemsets = freq_itemsets[freq_itemsets['itemsets'].apply(lambda x: zmin <= len(x) <= zmax)]

    with open(output_file, 'w', encoding='utf-8') as out:
        for _, row in freq_itemsets.iterrows():
            print(json.dumps([list(row['itemsets']), int(row['support'] * len(df))]), file=out)

    del transactions, freq_itemsets, df, te_ary
    gc.collect()
    return f"Procesado: {os.path.basename(chunk_file)}"


def wrapper_process_chunk(args):
    return process_chunk_fpgrowth(*args)


def run_all_chunks(chunk_dir, output_dir, workers=4, supp=SUPPORT, zmin=ZMIN, zmax=ZMAX):
    os.makedirs(output_dir, exist_ok=True)
    chunk_files = sorted([f for f in os.listdir(chunk_dir) if f.endswith('.jsonl')])

    tasks = []
    for f in chunk_files:
        chunk_path = os.path.join(chunk_dir, f)
        output_path = os.path.join(output_dir, f.replace('.jsonl', '_output.jsonl'))
        if not os.path.exists(output_path):
            tasks.append((chunk_path, output_path, supp, zmin, zmax))

    if not tasks:
        tqdm.write("✅ Todos los bloques ya fueron procesados.")
        return

    tqdm.write(f"🔁 Procesando {len(tasks)} bloques con {workers} procesos...")
    with tqdm(total=len(tasks), desc="FP-Growth chunks", dynamic_ncols=True) as pbar:
        with Pool(processes=workers) as pool:
            for result in pool.imap_unordered(wrapper_process_chunk, tasks):
                pbar.update(1)
                tqdm.write(result)


def merge_results(output_dir, final_output):
    itemsets = {}
    files = sorted([f for f in os.listdir(output_dir) if f.endswith('_output.jsonl')])

    tqdm.write("📦 Uniendo resultados parciales...")
    for fname in tqdm(files, desc="Merge parciales", dynamic_ncols=True):
        with open(os.path.join(output_dir, fname), 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                item, support = json.loads(line)
                key = tuple(sorted(item))
                itemsets[key] = itemsets.get(key, 0) + support

    with open(final_output, 'w', encoding='utf-8') as f:
        for item, support in itemsets.items():
            print(json.dumps([list(item), support]), file=f)
    tqdm.write(f"✅ Resultado final guardado en: {final_output}")


def traducir_final_output(input_file, output_file, mapping_file):
    mapping = load_client_names_mapping(mapping_file)
    with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'w', encoding='utf-8') as fout:
        for line in fin:
            if not line.strip():
                continue
            items, support = json.loads(line)
            traducidos = [mapping.get(i, f"{i} --> Desconocido") for i in items]
            print(json.dumps([traducidos, support], ensure_ascii=False), file=fout)
    tqdm.write(f"✅ Archivo traducido guardado en: {output_file}")


# ----------- CLI / MAIN ------------
def parse_args():
    ap = argparse.ArgumentParser(description="FP-Growth sobre ZIP anidado con filtro por rango de RUN (millones).")
    ap.add_argument("--main-zip", required=True, help="Ruta al ZIP principal que contiene directorios con ZIPs internos.")
    ap.add_argument("--run-min", type=float, required=True, help="Rango RUN mínimo (millones), inclusivo. Ej: 22")
    ap.add_argument("--run-max", type=float, required=True, help="Rango RUN máximo (millones), inclusivo. Ej: 23")
    ap.add_argument("--chunksize", type=int, default=CHUNKSIZE, help="Filas aprox. antes de volcar un chunk.")
    ap.add_argument("--support", type=float, default=SUPPORT, help="min_support para FP-Growth.")
    ap.add_argument("--zmin", type=int, default=ZMIN, help="Tamaño mínimo del itemset.")
    ap.add_argument("--zmax", type=int, default=ZMAX, help="Tamaño máximo del itemset.")
    ap.add_argument("--workers", type=int, default=NUM_WORKERS, help="Procesos paralelos para chunks.")
    ap.add_argument("--mapping-file", default=MAPPING_FILE, help="CSV para traducir client_id -> nombre.")
    ap.add_argument("--chunk-dir", default=CHUNK_DIR, help="Directorio donde escribir chunks.")
    ap.add_argument("--output-dir", default=OUTPUT_DIR, help="Directorio para salidas parciales.")
    ap.add_argument("--final-output", default=FINAL_OUTPUT, help="Archivo de salida consolidado (IDs).")
    return ap.parse_args()


if __name__ == '__main__':
    args = parse_args()
    t0 = time.perf_counter()
    tqdm.write("🚀 Iniciando procesamiento FP-Growth (ZIP anidado, con filtro de RUN rango)...")
    log_system_usage("Inicio:")

    os.makedirs(args.chunk_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    # 1) Preprocesamiento y chunks (con padding uniforme)
    padded_chunk_paths = preprocess_top_zip_to_chunks(
        args.main_zip,
        args.chunk_dir,
        chunksize=args.chunksize,
        run_min=args.run_min,
        run_max=args.run_max
    )

    # 2) FP-Growth por chunk
    run_all_chunks(args.chunk_dir, args.output_dir, workers=args.workers,
                   supp=args.support, zmin=args.zmin, zmax=args.zmax)

    # 3) Merge parcial -> final IDs
    merge_results(args.output_dir, args.final_output)

    # 4) Traducido con sufijo {run-min}_{run-max}
    translated = f"final_output_traducido_{int(args.run_min)}_{int(args.run_max)}.jsonl"
    traducir_final_output(args.final_output, translated, args.mapping_file)

    tqdm.write("🏁 Todo finalizado.")
    elapsed = time.perf_counter() - t0
    print(f"⏱️ Tiempo total: {_fmt_hms(elapsed)} ({elapsed:.2f} s)")
