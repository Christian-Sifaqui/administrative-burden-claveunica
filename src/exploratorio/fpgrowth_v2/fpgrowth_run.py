# -*- coding: utf-8 -*-
"""
Pipeline sobre ZIP anidado que:
- Filtra por rango RUN (millones): --run-min / --run-max (rango [min, max))
- Preprocesa a chunks JSONL (cada línea = conjunto de client_ids por usuario/sesión)
- Renombra chunks con ceros a la izquierda según el total generado
- Procesa en paralelo: SOLO CUENTA client_id (sin itemsets)
- Une resultados y traduce client_id a nombres (si hay mapping)
- Mantiene la barra tqdm limpia (sin prints en workers)
"""

import os
import io
import re
import gc
import json
import csv
import time
import zipfile
import argparse
from collections import defaultdict, Counter
from multiprocessing import Pool, cpu_count

import pandas as pd  # solo para quien lo requiera más adelante; no es obligatorio aquí
from tqdm import tqdm

# ===================== CONFIG POR DEFECTO =====================
MAIN_FILE = 'cu_user_client_id-20250922T144900Z-1-001.zip'
MAPPING_FILE = 'dim_integracion_cu.csv'
CHUNK_DIR = 'chunks'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'final_output.jsonl'              # líneas: [client_id, soporte]
TRANSLATED_OUTPUT = 'final_output_traducido.jsonl'

CHUNKSIZE = 100_000
SUPPORT = 0.0005           # soporte mínimo relativo (por chunk)
NUM_WORKERS = cpu_count()
# =============================================================


# ---------------- UTILIDADES ----------------
def _fmt_hms(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _detect_idx(names, candidates, fallback_idx=None):
    if not names:
        return fallback_idx
    for cand in candidates:
        if cand in names:
            return names.index(cand)
    names_norm = [str(h).strip().strip('"').strip() for h in names]
    for cand in candidates:
        cand_norm = str(cand).strip().strip('"').strip()
        if cand_norm in names_norm:
            return names_norm.index(cand_norm)
    return fallback_idx


def _parse_float_safe(x):
    try:
        return float(str(x).strip().replace(',', '.'))
    except Exception:
        return None


def load_client_names_mapping(filename):
    mapping = {}
    if not os.path.exists(filename):
        return mapping
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = (row.get('nombre_app') or '').strip()
            inst = (row.get('institucion') or '').strip()
            client_id = (row.get('client_id') or '').strip()
            if client_id:
                mapping[client_id] = f"{name}: {inst}"
    return mapping


# ===================== PREPROCESAMIENTO =====================
def _flush_chunk(user_to_client_ids, chunk_dir, chunk_index):
    """
    Escribe chunk_{i}.jsonl con una transacción (set de client_ids) por línea.
    Sin prints (barra limpia).
    """
    if not user_to_client_ids:
        return chunk_index
    os.makedirs(chunk_dir, exist_ok=True)
    path = os.path.join(chunk_dir, f"chunk_{chunk_index}.jsonl")
    with open(path, 'w', encoding='utf-8') as f:
        for client_ids in user_to_client_ids.values():
            print(json.dumps(list(client_ids)), file=f)
    user_to_client_ids.clear()
    gc.collect()
    return chunk_index + 1


def preprocess_top_zip_to_chunks(main_zip_path, chunk_dir, chunksize=100_000, run_min=None, run_max=None):
    """
    Lee ZIP principal → ZIPs internos → CSVs con ';' y comillas.
    Agrega por usuario el set de client_ids y vuelca en chunks.
    Aplica filtro de rango RUN (millones) si corresponde.
    Devuelve total de chunks creados.
    """
    os.makedirs(chunk_dir, exist_ok=True)
    user_to_client_ids = defaultdict(set)
    rows_since_last_flush = 0
    chunk_index = 0

    with zipfile.ZipFile(main_zip_path, 'r') as top_zip:
        inner_zip_names = [n for n in top_zip.namelist() if n.lower().endswith('.zip')]
        inner_zip_names.sort()

        with tqdm(inner_zip_names, desc="Leyendo ZIPs internos", dynamic_ncols=True) as bar:
            for inner_name in bar:
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    continue

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith('.csv')]
                    if not csv_members:
                        continue
                    csv_name = csv_members[0]

                    with inner_zip.open(csv_name, 'r') as csv_file_bin:
                        with io.TextIOWrapper(csv_file_bin, encoding='utf-8', newline='') as csv_text:
                            reader = csv.reader(csv_text, delimiter=';', quotechar='"')
                            header = next(reader, None)
                            if header is None:
                                continue

                            idx_run_anon = _detect_idx(header, ['run_anonimizado', 'run_anon'], fallback_idx=None)
                            idx_client   = _detect_idx(header, ['client_id_traza', 'client_id', 'clientids', 'clientes'], fallback_idx=None)
                            idx_run_range = _detect_idx(header, ['rango_run_millones', 'rango_run', 'run_range_millions'], fallback_idx=2)

                            if idx_client is None:
                                continue

                            for row in reader:
                                if not row:
                                    continue

                                # Filtro por rango RUN (millones)
                                if idx_run_range is not None and 0 <= idx_run_range < len(row):
                                    rr = _parse_float_safe(row[idx_run_range])
                                    if rr is None:
                                        if (run_min is not None) or (run_max is not None):
                                            continue
                                    else:
                                        if (run_min is not None) and (rr < run_min):
                                            continue
                                        if (run_max is not None) and (rr >= run_max):
                                            continue

                                # Clave de usuario
                                if idx_run_anon is not None and 0 <= idx_run_anon < len(row):
                                    user_key = str(row[idx_run_anon]).strip()
                                else:
                                    user_key = None
                                    for cell in row:
                                        if str(cell).strip():
                                            user_key = str(cell).strip()
                                            break
                                    if not user_key:
                                        continue

                                # Campo client_id (posible lista separada por comas)
                                client_field = str(row[idx_client]).strip().strip('"') if idx_client < len(row) else ''
                                if not client_field:
                                    continue

                                for cid in client_field.split(','):
                                    cid = cid.strip()
                                    if cid:
                                        user_to_client_ids[user_key].add(cid)

                                rows_since_last_flush += 1
                                if rows_since_last_flush >= chunksize:
                                    chunk_index = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index)
                                    rows_since_last_flush = 0

    if user_to_client_ids:
        chunk_index = _flush_chunk(user_to_client_ids, chunk_dir, chunk_index)

    return chunk_index  # total chunks


# ===================== RENOMBRE CON CEROS =====================
def pad_chunk_filenames(chunk_dir: str, total_chunks: int):
    """
    Renombra chunk_i.jsonl -> chunk_{i:0Wd}.jsonl, con W = len(str(total_chunks)).
    Sin prints (barra limpia).
    """
    if total_chunks <= 0:
        return
    width = len(str(total_chunks))  # p.ej. total=100 -> 3
    for i in range(total_chunks):
        old_name = os.path.join(chunk_dir, f"chunk_{i}.jsonl")
        new_name = os.path.join(chunk_dir, f"chunk_{i:0{width}d}.jsonl")
        if old_name == new_name:
            continue
        if os.path.exists(old_name) and not os.path.exists(new_name):
            os.rename(old_name, new_name)


def _chunk_num(name: str) -> int:
    m = re.search(r'chunk_(\d+)\.jsonl$', name)
    return int(m.group(1)) if m else -1

def worker_process(args):
    """
    Función top-level (pickleable) para multiprocessing.
    args = (chunk_path, output_path, support)
    Retorna (nombre_chunk, hecho: bool)
    """
    chunk_path, output_path, support = args
    done = process_chunk_counts(chunk_path, output_file=output_path, supp=support)
    return os.path.basename(chunk_path), done

# ===================== PROCESAMIENTO (SIN ITEMSETS) =====================
def process_chunk_counts(chunk_file, output_file, supp=0.0005):
    """
    Cuenta occurrences por client_id en el chunk (sin itemsets).
    - Cada línea del chunk es una lista de client_ids (transacción/usuario).
    - Para no duplicar dentro de la misma transacción, usamos set().
    - Aplica soporte mínimo relativo al tamaño del chunk.
    - Escribe líneas: [client_id, soporte]
    - NO imprime (workers silenciosos).
    """
    if os.path.exists(output_file):
        return False

    counts = Counter()
    n_lines = 0
    with open(chunk_file, 'r', encoding='utf-8') as f:
        for line in f:
            n_lines += 1
            txn = json.loads(line)
            counts.update(set(txn))  # evitar duplicados dentro de la misma transacción

    if n_lines == 0:
        # crear archivo vacío para marcar como procesado
        open(output_file, 'w', encoding='utf-8').close()
        return True

    min_count = max(1, int(supp * n_lines)) if supp and supp > 0 else 1

    with open(output_file, 'w', encoding='utf-8') as out:
        for item, c in counts.items():
            if c >= min_count:
                print(json.dumps([item, c]), file=out)

    del counts
    gc.collect()
    return True


def wrapper_process_chunk(args):
    return process_chunk_counts(*args, supp=SUPPORT)


def run_all_chunks(chunk_dir, output_dir, workers=4, support=0.0005, verbose=True):
    """
    Lanza conteo en paralelo y muestra una barra limpia.
    Si verbose=True, el proceso principal imprime una línea por chunk procesado.
    """
    os.makedirs(output_dir, exist_ok=True)
    chunk_files = sorted(
        [f for f in os.listdir(chunk_dir) if f.endswith('.jsonl')],
        key=_chunk_num
    )

    tasks = []
    for f in chunk_files:
        chunk_path = os.path.join(chunk_dir, f)
        output_path = os.path.join(output_dir, f.replace('.jsonl', '_output.jsonl'))
        if not os.path.exists(output_path):
            tasks.append((chunk_path, output_path, support))

    if not tasks:
        return

    with tqdm(total=len(tasks), desc="Procesando chunks (conteo)", dynamic_ncols=True) as pbar:
        with Pool(processes=workers) as pool:
            for name, _ in pool.imap_unordered(worker_process, tasks):
                pbar.update(1)
                if verbose:
                    # Solo el proceso principal escribe (no rompe la barra)
                    tqdm.write(f"Procesado: {name}")



# ===================== MERGE Y TRADUCCIÓN =====================
def merge_results(output_dir, final_output):
    """
    Une todos los *_output.jsonl sumando soportes por client_id.
    Escribe [client_id, soporte_total] en final_output.
    """
    totals = Counter()
    files = sorted([f for f in os.listdir(output_dir) if f.endswith('_output.jsonl')])

    for fname in tqdm(files, desc="Unificando resultados", dynamic_ncols=True):
        with open(os.path.join(output_dir, fname), 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                item, support = json.loads(line)
                totals[item] += int(support)

    with open(final_output, 'w', encoding='utf-8') as f:
        for item, support in totals.items():
            print(json.dumps([item, support]), file=f)


def traducir_final_output(input_file, output_file, mapping_file):
    """
    Traduce client_id a "nombre_app: institucion".
    Entrada: líneas [client_id, soporte]
    Salida:  líneas [nombre_app: institucion (o client_id --> Desconocido), soporte]
    """
    mapping = load_client_names_mapping(mapping_file)
    try:
        total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8'))
    except Exception:
        total_lines = 0

    iterator = open(input_file, 'r', encoding='utf-8')
    use_bar = total_lines > 0
    if use_bar:
        iterator = tqdm(iterator, total=total_lines, desc="Traduciendo salida", dynamic_ncols=True)

    with open(output_file, 'w', encoding='utf-8') as fout:
        for line in iterator:
            client_id, support = json.loads(line)
            nombre = mapping.get(client_id, f"{client_id} --> Desconocido")
            print(json.dumps([nombre, support], ensure_ascii=False), file=fout)


# ===================== MAIN =====================
def main():
    parser = argparse.ArgumentParser(
        description="Conteo por client_id (sin itemsets) con filtro RUN (millones) y barra limpia."
    )
    parser.add_argument('--run-min', type=float, default=None, help='Rango RUN mínimo (millones), inclusivo. Ej: 22')
    parser.add_argument('--run-max', type=float, default=None, help='Rango RUN máximo (millones), exclusivo. Ej: 23')
    parser.add_argument('--main-zip', type=str, default=MAIN_FILE, help='Ruta al ZIP principal.')
    parser.add_argument('--chunksize', type=int, default=CHUNKSIZE, help='Tamaño de volcado por chunk.')
    parser.add_argument('--workers', type=int, default=NUM_WORKERS, help='Núm. de procesos paralelos.')
    parser.add_argument('--support', type=float, default=SUPPORT, help='Soporte mínimo relativo por chunk.')
    parser.add_argument('--verbose', action='store_true', help='Imprime una línea por chunk procesado.')
    args = parser.parse_args()

    t0 = time.perf_counter()

    # 1) Preprocesar → chunks sin padding
    total_chunks = preprocess_top_zip_to_chunks(
        args.main_zip,
        CHUNK_DIR,
        chunksize=args.chunksize,
        run_min=args.run_min,
        run_max=args.run_max
    )

    # 2) Renombrar chunks con padding según total
    pad_chunk_filenames(CHUNK_DIR, total_chunks)

    # 3) Procesar conteo (sin itemsets) en paralelo
    run_all_chunks(CHUNK_DIR, OUTPUT_DIR, workers=args.workers, support=args.support, verbose=args.verbose)

    # 4) Merge
    merge_results(OUTPUT_DIR, FINAL_OUTPUT)

    # 5) Traducción
    traducir_final_output(FINAL_OUTPUT, TRANSLATED_OUTPUT, MAPPING_FILE)

    elapsed = time.perf_counter() - t0
    print(f"⏱️ Tiempo total: {_fmt_hms(elapsed)} ({elapsed:.2f} s)")


if __name__ == '__main__':
    main()
