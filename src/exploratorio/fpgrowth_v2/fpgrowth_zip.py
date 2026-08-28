import os
import io
import json
import gc
import csv
import psutil
import zipfile
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Pool
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.preprocessing import TransactionEncoder
import time

# -------- CONFIG --------
MAIN_FILE = 'cu_user_client_id-20250922T144900Z-1-001.zip'
MAPPING_FILE = 'dim_integracion_cu.csv'
CHUNK_DIR = 'chunks'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'final_output.jsonl'
TRANSLATED_OUTPUT = 'final_output_traducido.jsonl'

CHUNKSIZE = 100_000  # filas leídas aprox. antes de volcar a un chunk
SUPPORT = 0.0005
ZMIN = 1
ZMAX = 5
NUM_WORKERS = os.cpu_count()
# -------------------------
# ---------------- UTILIDADES ----------------
# Formatea segundos a HH:MM:SS
def _fmt_hms(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# ---------------- UTIL LOGGING AMIGABLE CON TQDM ----------------
# Para no "romper" la barra de progreso, usamos una función de log que por defecto
# usa tqdm.write (si hay una barra activa) o print si no la hay.

def _get_logger():
    try:
        # Si existe al menos una instancia, tqdm.write funciona bien
        return tqdm.write
    except Exception:
        return print


def log_system_usage(prefix=""):
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    _get_logger()(f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024**3):.2f} GB), CPU: {cpu}%")


def load_client_names_mapping(filename):
    mapping = {}
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row['nombre_app'].strip()
            inst = row['institucion'].strip()
            client_id = row['client_id'].strip()
            mapping[client_id] = f"{name}: {inst}"
    return mapping


# ===================== PREPROCESAMIENTO PARA ZIP ANIDADO =====================

def _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=_get_logger()):
    """Escribe chunk_{i}.jsonl con las transacciones acumuladas sin romper tqdm."""
    if not user_to_client_ids:
        return chunk_index
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index}.jsonl")
    with open(chunk_path, 'w', encoding='utf-8') as f:
        for client_ids in user_to_client_ids.values():
            print(json.dumps(list(client_ids)), file=f)
    log(f"🧩 Chunk escrito: {chunk_path} ( transacciones={len(user_to_client_ids)} )")
    user_to_client_ids.clear()
    gc.collect()
    return chunk_index + 1


def preprocess_top_zip_to_chunks(main_zip_path, chunk_dir, chunksize=100_000):
    os.makedirs(chunk_dir, exist_ok=True)

    user_to_client_ids = defaultdict(set)
    rows_since_last_flush = 0
    chunk_index = 0
    log = _get_logger()

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
                            try:
                                idx_run = header.index('run_anonimizado')
                                idx_client = header.index('client_id_traza')
                            except ValueError:
                                header_norm = [h.strip('"') for h in header]
                                idx_run = header_norm.index('run_anonimizado')
                                idx_client = header_norm.index('client_id_traza')

                            for row in reader:
                                if not row or len(row) <= max(idx_run, idx_client):
                                    continue
                                run = str(row[idx_run]).strip()
                                if not run:
                                    continue
                                client_field = str(row[idx_client]).strip().strip('"')
                                if not client_field:
                                    continue
                                for cid in client_field.split(','):
                                    cid = cid.strip()
                                    if cid:
                                        user_to_client_ids[run].add(cid)

                                rows_since_last_flush += 1
                                if rows_since_last_flush >= chunksize:
                                    chunk_index = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=log)
                                    rows_since_last_flush = 0

    if user_to_client_ids:
        _ = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index, log=log)


# ===================== FP-GROWTH POR CHUNK =====================

def process_chunk_fpgrowth(chunk_file, output_file, supp=0.0005, zmin=1, zmax=5):
    if os.path.exists(output_file):
        return f"Ya procesado: {chunk_file}"

    log_system_usage(f"[Procesando {os.path.basename(chunk_file)}]")

    with open(chunk_file, 'r', encoding='utf-8') as f:
        transactions = [json.loads(line) for line in f]

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
    return f"Procesado: {chunk_file}"


def wrapper_process_chunk(args):
    return process_chunk_fpgrowth(*args, supp=SUPPORT, zmin=ZMIN, zmax=ZMAX)


def run_all_chunks(chunk_dir, output_dir, workers=4):
    os.makedirs(output_dir, exist_ok=True)
    chunk_files = sorted([f for f in os.listdir(chunk_dir) if f.endswith('.jsonl')])

    tasks = []
    for f in chunk_files:
        chunk_path = os.path.join(chunk_dir, f)
        output_path = os.path.join(output_dir, f.replace('.jsonl', '_output.jsonl'))
        if not os.path.exists(output_path):
            tasks.append((chunk_path, output_path))

    if not tasks:
        tqdm.write("✅ Todos los bloques ya fueron procesados.")
        return

    tqdm.write(f"🔁 Procesando {len(tasks)} bloques con {workers} procesos...")
    with tqdm(total=len(tasks), desc="FP-Growth chunks", dynamic_ncols=True) as pbar:
        with Pool(processes=workers) as pool:
            for result in pool.imap_unordered(wrapper_process_chunk, tasks):
                pbar.update(1)
                # Si quieres ver el resultado de cada chunk, usa pbar.write en vez de print
                tqdm.write(result)


def merge_results(output_dir, final_output):
    itemsets = {}
    files = sorted([f for f in os.listdir(output_dir) if f.endswith('_output.jsonl')])

    tqdm.write("📦 Uniendo resultados parciales...")
    for fname in tqdm(files, desc="Merge parciales", dynamic_ncols=True):
        with open(os.path.join(output_dir, fname), 'r', encoding='utf-8') as f:
            for line in f:
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
            items, support = json.loads(line)
            traducidos = [mapping.get(i, f"{i} --> Desconocido") for i in items]
            print(json.dumps([traducidos, support], ensure_ascii=False), file=fout)
    tqdm.write(f"✅ Archivo traducido guardado en: {output_file}")


# ----------- MAIN ------------
if __name__ == '__main__':
    t0 = time.perf_counter()  # iniciar cronómetro
    tqdm.write("🚀 Iniciando procesamiento FP-Growth (ZIP anidado)...")
    log_system_usage("Inicio:")

    preprocess_top_zip_to_chunks(MAIN_FILE, CHUNK_DIR, chunksize=CHUNKSIZE)
    run_all_chunks(CHUNK_DIR, OUTPUT_DIR, workers=NUM_WORKERS)
    merge_results(OUTPUT_DIR, FINAL_OUTPUT)
    traducir_final_output(FINAL_OUTPUT, TRANSLATED_OUTPUT, MAPPING_FILE)
    tqdm.write("🏁 Todo finalizado.")
    elapsed = time.perf_counter() - t0
    print(f"⏱️ Tiempo total: {_fmt_hms(elapsed)} ({elapsed:.2f} s)")
