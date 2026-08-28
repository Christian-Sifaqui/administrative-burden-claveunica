#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FP-Growth sectorial (pares y tríos) sobre ZIP anidado de ClaveÚnica.

Flujo:
1) Preprocesa el ZIP principal con muchos ZIPs internos y CSVs para generar
   transacciones (sets de client_id por "run_anonimizado"). Se escribe
   una colección de archivos JSONL en disco: cada línea es un array JSON
   de client_id únicos (transacción).
2) Con Spark, mapea cada transacción a sectores (usando cu_con_sectores.csv),
   deduplica sectores por transacción y ejecuta FP-Growth sobre sectores.
3) Exporta resultados de pares (k=2) y tríos (k=3) con sus frecuencias, y
   un ranking de "hubs" sectoriales (suma de soportes en pares donde participa).

Uso sugerido:
  spark-submit \
    --master local[8] \
    --driver-memory 10g \
    --conf "spark.sql.shuffle.partitions=800" \
    --conf "spark.default.parallelism=800" \
    fpgrowth_sector_zip_spark.py \
    --main-zip /ruta/cu_user_client_id-20250922T144900Z-1-001.zip \
    --mapping cu_con_sectores.csv \
    --workdir ./work_sectores \
    --min-support 0.0005 \
    --kmax 3

Notas:
- El preprocesamiento corre en el driver (Python puro) y es secuencial; se ha
  optimizado para memoria (flushing por chunks) y fue probado con decenas de
  millones de filas. Ajuste --chunksize si es necesario.
- El FP-Growth propiamente tal se ejecuta en Spark para escalar.
- Si ya tiene generados los chunks, puede saltarse la etapa de preprocesado
  usando --skip-preprocess.
"""
from __future__ import annotations
import os
import io
import gc
import json
import csv
import time
import zipfile
from collections import defaultdict
from typing import Dict, List, Tuple, Iterable

import psutil
from tqdm import tqdm

# Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.fpm import FPGrowth

# ----------------- Utiles -----------------

def fmt_hms(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def log_sys(prefix: str = "") -> None:
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    tqdm.write(f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024**3):.2f} GB), CPU: {cpu}%")


# ----------------- Preproceso: ZIP anidado -> chunks JSONL -----------------

def flush_chunk(user_to_client_ids: Dict[str, set], chunk_dir: str, chunk_idx: int) -> int:
    if not user_to_client_ids:
        return chunk_idx
    os.makedirs(chunk_dir, exist_ok=True)
    outp = os.path.join(chunk_dir, f"chunk_{chunk_idx:05d}.jsonl")
    with open(outp, "w", encoding="utf-8") as f:
        for cids in user_to_client_ids.values():
            f.write(json.dumps(sorted(cids)) + "\n")
    tqdm.write(f"🧩 Chunk escrito: {outp} (transacciones={len(user_to_client_ids)})")
    user_to_client_ids.clear()
    gc.collect()
    return chunk_idx + 1


def preprocess_to_chunks(main_zip_path: str, chunk_dir: str, chunksize: int = 100_000,
                          run_col: str = "run_anonimizado", client_col: str = "client_id_traza") -> None:
    os.makedirs(chunk_dir, exist_ok=True)
    user_to_client_ids: Dict[str, set] = defaultdict(set)
    rows_since_flush = 0
    chunk_idx = 0

    with zipfile.ZipFile(main_zip_path, 'r') as top_zip:
        inner = [n for n in top_zip.namelist() if n.lower().endswith('.zip')]
        inner.sort()
        tqdm.write(f"📦 ZIP principal: {main_zip_path} — ZIPs internos: {len(inner)}")
        with tqdm(inner, desc="Leyendo ZIPs internos", dynamic_ncols=True) as bar:
            for inner_name in bar:
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    tqdm.write(f"⚠️ No se pudo leer entrada: {inner_name}")
                    continue
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith('.csv')]
                    if not csv_members:
                        tqdm.write(f"⚠️ Sin CSV dentro de {inner_name}")
                        continue
                    csv_name = csv_members[0]
                    with inner_zip.open(csv_name, 'r') as csv_file_bin:
                        with io.TextIOWrapper(csv_file_bin, encoding='utf-8', newline='') as csv_text:
                            reader = csv.reader(csv_text, delimiter=';', quotechar='"')
                            header = next(reader, None)
                            if header is None:
                                continue
                            # localizar columnas con y sin comillas
                            try:
                                idx_run = header.index(run_col)
                                idx_client = header.index(client_col)
                            except ValueError:
                                header_norm = [h.strip('"') for h in header]
                                idx_run = header_norm.index(run_col)
                                idx_client = header_norm.index(client_col)

                            for row in reader:
                                if not row or len(row) <= max(idx_run, idx_client):
                                    continue
                                run = str(row[idx_run]).strip()
                                if not run:
                                    continue
                                client_field = str(row[idx_client]).strip().strip('"')
                                if not client_field:
                                    continue
                                # dividir posibles múltiples client_id
                                for cid in client_field.split(','):
                                    cid = cid.strip()
                                    if cid:
                                        user_to_client_ids[run].add(cid)
                                rows_since_flush += 1
                                if rows_since_flush >= chunksize:
                                    chunk_idx = flush_chunk(user_to_client_ids, chunk_dir, chunk_idx)
                                    rows_since_flush = 0
    # flush final
    if user_to_client_ids:
        _ = flush_chunk(user_to_client_ids, chunk_dir, chunk_idx)


# ----------------- Spark: FP-Growth sobre sectores -----------------

def read_mapping(mapping_csv: str) -> Dict[str, str]:
    """Lee cu_con_sectores.csv y devuelve dict client_id -> sector."""
    mapping: Dict[str, str] = {}
    with open(mapping_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = (row.get('client_id') or '').strip()
            sec = (row.get('sector') or '').strip() or 'Desconocido'
            if cid:
                mapping[cid] = sec
    return mapping


def build_spark(app_name: str = "FPGrowth Sectores") -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            # otros ajustes se pueden pasar por spark-submit --conf
            .getOrCreate())


def spark_transactions_from_chunks(spark: SparkSession, chunk_dir: str,
                                   mapping: Dict[str, str],
                                   min_len: int = 1) -> 'DataFrame':
    """
    Lee todos los *.jsonl del chunk_dir (cada línea es un array JSON de client_id)
    y los mapea a listas de sectores (deduplicadas). Retorna DataFrame con col "items".
    """
    # Leemos como texto, luego parseamos el JSON de cada línea
    df_txt = spark.read.text([os.path.join(chunk_dir, f) for f in os.listdir(chunk_dir) if f.endswith('.jsonl')])

    schema = T.ArrayType(T.StringType())
    df_arrays = df_txt.select(F.from_json(F.col('value'), schema).alias('client_ids'))

    # broadcast del mapping
    bmap = spark.sparkContext.broadcast(mapping)

    @F.udf(returnType=T.ArrayType(T.StringType()))
    def map_to_sectors(client_ids: List[str]) -> List[str]:
        if not client_ids:
            return []
        # mapear a sectores y deduplicar
        sectors = {bmap.value.get(cid, 'Desconocido') for cid in client_ids}
        # opcionalmente descartar "Desconocido"
        sectors.discard('Desconocido')
        return sorted(sectors)

    df_items = df_arrays.select(map_to_sectors('client_ids').alias('items'))
    if min_len > 1:
        df_items = df_items.where(F.size('items') >= min_len)
    return df_items


def run_fpgrowth_on_sectors(df_items, min_support: float, kmax: int,
                            output_dir: str) -> Tuple[str, str, str]:
    os.makedirs(output_dir, exist_ok=True)

    fp = FPGrowth(itemsCol='items', minSupport=min_support, minConfidence=0.0)
    model = fp.fit(df_items)

    # Frecuentes
    fi = model.freqItemsets  # cols: items (array<string>), freq (long)
    # Filtrar por tamaño 2 y 3
    pairs = fi.where(F.size('items') == 2).orderBy(F.desc('freq'))
    trios = fi.where((F.size('items') == 3) & (F.lit(kmax) >= 3)).orderBy(F.desc('freq'))

    out_pairs = os.path.join(output_dir, 'sectores_pairs.jsonl')
    out_trios = os.path.join(output_dir, 'sectores_trios.jsonl')

    (pairs
     .select(F.to_json(F.struct('items', 'freq')).alias('json'))
     .coalesce(1)
     .write.mode('overwrite').text(out_pairs))

    (trios
     .select(F.to_json(F.struct('items', 'freq')).alias('json'))
     .coalesce(1)
     .write.mode('overwrite').text(out_trios))

    # Hubs: sumar frecuencias de todos los pares donde participa cada sector
    # pairs: items=[A,B], freq=f. Explode y sumar por sector
    hubs = (pairs
            .withColumn('sector', F.explode('items'))
            .groupBy('sector').agg(F.sum('freq').alias('hub_score'))
            .orderBy(F.desc('hub_score')))

    out_hubs = os.path.join(output_dir, 'sectores_hubs.jsonl')
    (hubs
     .select(F.to_json(F.struct('sector', 'hub_score')).alias('json'))
     .coalesce(1)
     .write.mode('overwrite').text(out_hubs))

    return out_pairs, out_trios, out_hubs


# ----------------- CLI -----------------

import argparse

def main():
    parser = argparse.ArgumentParser(description="FP-Growth sectorial (pares y tríos) desde ZIP anidado")
    parser.add_argument('--main-zip', required=True, help='Ruta al ZIP principal con los ZIPs internos')
    parser.add_argument('--mapping', required=True, help='Ruta a cu_con_sectores.csv (client_id,nombre_app,institucion,sector)')
    parser.add_argument('--workdir', default='work_sectores', help='Directorio de trabajo (chunks y salidas)')
    parser.add_argument('--chunksize', type=int, default=100_000, help='Filas aproximadas antes de volcar un chunk')
    parser.add_argument('--min-support', type=float, default=0.0005, help='Soporte mínimo para FP-Growth (sobre transacciones de sectores)')
    parser.add_argument('--kmax', type=int, default=3, help='Tamaño máximo de itemset a reportar (pares y tríos)')
    parser.add_argument('--skip-preprocess', action='store_true', help='Saltar preprocesamiento si ya existen chunks')
    parser.add_argument('--min-items', type=int, default=2, help='Mínimo de sectores por transacción para considerar en FP-Growth')

    args = parser.parse_args()

    t0 = time.perf_counter()
    tqdm.write("🚀 Iniciando FP-Growth sectorial...")
    log_sys("Inicio:")

    chunk_dir = os.path.join(args.workdir, 'chunks')
    out_dir = os.path.join(args.workdir, 'outputs')

    if not args.skip_preprocess:
        tqdm.write("📑 Etapa 1/3: Preprocesamiento a transacciones (client_id por run)")
        os.makedirs(chunk_dir, exist_ok=True)
        preprocess_to_chunks(args.main_zip, chunk_dir, chunksize=args.chunksize)
    else:
        tqdm.write("⏭️ Saltando preprocesamiento; se usarán chunks existentes.")

    tqdm.write("🗺️ Cargando mapeo client_id → sector...")
    mapping = read_mapping(args.mapping)
    tqdm.write(f"✔️ Mapeos cargados: {len(mapping):,} client_id con sector")

    tqdm.write("🧠 Etapa 2/3: Construcción de transacciones de sectores en Spark")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_items = spark_transactions_from_chunks(spark, chunk_dir, mapping, min_len=args.min_items)
    n_tx = df_items.count()
    tqdm.write(f"🧾 Transacciones de sectores: {n_tx:,}")

    tqdm.write("📈 Etapa 3/3: FP-Growth sobre sectores (pares y tríos)")
    out_pairs, out_trios, out_hubs = run_fpgrowth_on_sectors(
        df_items, min_support=args.min_support, kmax=args.kmax, output_dir=out_dir
    )

    tqdm.write("🏁 Finalizado.")
    elapsed = time.perf_counter() - t0
    tqdm.write(f"⏱️ Tiempo total: {fmt_hms(elapsed)} ({elapsed:.2f} s)")

    # hints de lectura de salidas
    tqdm.write("👉 Salidas:")
    tqdm.write(f"  - Pares sectoriales: {out_pairs}")
    tqdm.write(f"  - Tríos sectoriales: {out_trios}")
    tqdm.write(f"  - Hubs sectoriales: {out_hubs}")


if __name__ == '__main__':
    main()
