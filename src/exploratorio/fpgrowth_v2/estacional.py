#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark FP-Growth para detección de patrones de co-uso con foco en estacionalidad por ventanas de tiempo.

USO BÁSICO (CSV planos):
    spark-submit --driver-memory 4g --executor-memory 4g spark_fpgrowth_estacionalidad.py \
      --input-path /ruta/a/csvs/ \
      --input-format csv \
      --limit-lines 1000000 \
      --time-bucket day \
      --min-support 0.001 \
      --output-path /ruta/a/salida/

USO (ZIP con CSV internos):
    spark-submit --driver-memory 6g --executor-memory 6g spark_fpgrowth_estacionalidad.py \
      --input-path /ruta/a/zips/ \
      --input-format zip \
      --limit-lines 1000000 \
      --time-bucket week \
      --min-support 0.002 \
      --output-path /ruta/a/salida/

Parámetros clave:
  --input-path        Carpeta o patrón con CSV o ZIP.
  --input-format      'csv' | 'zip'
  --limit-lines       Límite superior de líneas a leer (por defecto: 1_000_000).
  --time-bucket       'none' | 'day' | 'week' | 'month' (por defecto: 'day').
  --min-support       Soporte mínimo relativo para FP-Growth de Spark (p.ej., 0.001).
  --min-confidence    Confianza mínima para generar reglas (por defecto: 0.0 → no guarda reglas).
  --run-start         Filtro RUN inicial (float en millones), opcional.
  --run-end           Filtro RUN final (float en millones), opcional.
  --output-path       Carpeta de salida (se crearán subcarpetas por bucket).
  --infer-partitions  Particiones objetivo para la fase RDD→DF (por defecto: auto).
  --sample-seed       Semilla para downsampling estable si hay sobreflujo.

Salida:
  {output}/itemsets/particiones_por_bucket/...
  {output}/rules/particiones_por_bucket/...   (si min-confidence > 0)
  {output}/metrics/metrics.json               (resumen global)

NOTAS:
- El script intenta manejar diferentes formas del campo "servicios": separado por comas, por espacios o un único id.
- Para estacionalidad, revisa la evolución de soportes entre buckets (day/week/month).
"""

import argparse
import json
import os
import sys
import re
from datetime import datetime
from typing import Iterable, List, Tuple, Optional

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.fpm import FPGrowth


# ----------------------
# Utilidades de parsing
# ----------------------

SEPARATOR = ';'

def safe_float(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def parse_date_bucket(date_str: str, mode: str) -> str:
    # Espera ISO-YYYY-MM-DD; si viene con hora "YYYY-MM-DD HH:MM:SS", usa solo fecha
    if not date_str:
        return "UNK"
    try:
        if ' ' in date_str or 'T' in date_str:
            # Normaliza: intenta YYYY-MM-DD[ T]HH:MM[:SS]
            ds = date_str.split('T')[0].split(' ')[0]
        else:
            ds = date_str
        dt = datetime.fromisoformat(ds)
    except Exception:
        # último intento: cortar por ';' o ']' en casos raros
        # y tratar de extraer YYYY-MM-DD
        m = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
        if m:
            try:
                dt = datetime.fromisoformat(m.group(1))
            except Exception:
                return "UNK"
        else:
            return "UNK"

    if mode == 'day':
        return dt.strftime('%Y-%m-%d')
    elif mode == 'week':
        iso = dt.isocalendar()  # (year, week, weekday)
        return f"{iso[0]}-W{iso[1]:02d}"
    elif mode == 'month':
        return dt.strftime('%Y-%m')
    else:
        return 'ALL'

def split_services(raw: str) -> List[str]:
    """
    Intenta separar servicios si vienen múltiples; limpia comillas y espacios.
    Casos:
      - "a,b,c"
      - "a b c"
      - "abc123xyz" (uno solo)
    """
    if raw is None:
        return []
    s = raw.strip().strip('"').strip("'")
    if not s:
        return []
    # Si hay coma, dividir por comas
    if ',' in s:
        parts = [p.strip() for p in s.split(',') if p.strip()]
        return parts
    # Si hay espacios múltiples (y no parece un hash de 32-64 chars)
    if ' ' in s and not re.fullmatch(r'[A-Za-z0-9]{24,64}', s):
        parts = [p.strip() for p in s.split() if p.strip()]
        return parts
    # Por defecto, uno solo
    return [s]


# ---------------------------------------
# Lectura: CSV nativo o ZIP (binaryFiles)
# ---------------------------------------

def read_csv_native(spark: SparkSession, input_path: str, limit_lines: int):
    """
    Lee CSV/Texto con Spark (más eficiente).
    Asume formato: fecha ; user_id ; run_float ; servicios
    Devuelve DataFrame con columnas: raw_line (string)
    """
    df = spark.read.text(input_path)
    if limit_lines and limit_lines > 0:
        df = df.limit(limit_lines)
    return df

def read_zip_parallel(spark: SparkSession, input_path: str, limit_lines: int):
    """
    Lee uno o muchos ZIPs con sc.binaryFiles y extrae líneas desde:
      - CSV/TXT en la raíz del ZIP
      - CSV/TXT dentro de subdirectorios del ZIP
      - CSV/TXT dentro de ZIPs anidados (ZIP dentro de ZIP) con profundidad limitada
    Devuelve DataFrame con columna: raw_line (string)
    """
    sc = spark.sparkContext
    rdd = sc.binaryFiles(input_path)  # (zip_path, PortableDataStream)

    def extract_lines(iter_zips):
        import zipfile, io
        MAX_NESTING = 3  # evita loops y consumos extremos

        def process_zip_bytes(zbytes: bytes, emit, limit_left: list, depth: int = 0):
            if limit_left[0] <= 0 or depth > MAX_NESTING:
                return
            try:
                with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                    for name in zf.namelist():
                        if limit_left[0] <= 0:
                            return
                        # directorio → continuar
                        if name.endswith('/') or name.endswith('\\'):
                            continue
                        # lee bytes del entry
                        try:
                            with zf.open(name, 'r') as fh:
                                data = fh.read()
                        except Exception:
                            continue

                        lname = name.lower()
                        # si es otro ZIP → recursión
                        if lname.endswith('.zip'):
                            process_zip_bytes(data, emit, limit_left, depth + 1)
                            continue
                        # si es CSV/TXT → emitir líneas
                        if lname.endswith('.csv') or lname.endswith('.txt'):
                            # stream en memoria; iterar por líneas
                            bio = io.BytesIO(data)
                            for bline in bio.readlines():
                                if limit_left[0] <= 0:
                                    return
                                try:
                                    line = bline.decode('utf-8', errors='ignore').rstrip('\n\r')
                                except Exception:
                                    continue
                                emit(line)
                                limit_left[0] -= 1
                        # de lo contrario, ignorar (p.ej. .json, .parquet)

            except Exception:
                # archivo corrupto o no es un zip real
                return

        for path, pds in iter_zips:
            # pds puede ser PortableDataStream; conviene materializar a bytes
            try:
                outer_bytes = pds if isinstance(pds, bytes) else pds.read()
            except Exception:
                continue

            # contador mutable por partición: [restantes]
            limit_left = [limit_lines if limit_lines and limit_lines > 0 else 2**63 - 1]

            def emit(line):
                # función local para hacer yield desde un scope más profundo
                nonlocal _yield_buffer
                _yield_buffer.append(line)
                if len(_yield_buffer) >= 1024:
                    for x in _yield_buffer:
                        yield x
                    _yield_buffer = []

            # buffer para batching de yields
            _yield_buffer = []

            # Procesa el ZIP (que puede contener directorios y ZIPs anidados)
            # Como 'emit' necesita yield, hacemos una lista temporal:
            lines_local = []

            def emit_collect(l):
                lines_local.append(l)

            process_zip_bytes(outer_bytes, emit_collect, limit_left, depth=0)

            for l in lines_local:
                yield l

    rdd_lines = rdd.mapPartitions(extract_lines)
    df = spark.createDataFrame(
        rdd_lines.map(lambda x: (x,)),
        schema=T.StructType([T.StructField("raw_line", T.StringType(), True)])
    )
    return df



# ---------------------------------------
# Transformación → transacciones por bucket
# ---------------------------------------

def build_transactions_df(spark: SparkSession,
                          df_raw,
                          time_bucket: str,
                          run_start: Optional[float],
                          run_end: Optional[float],
                          infer_partitions: Optional[int]):
    """
    df_raw: DataFrame con 'raw_line'
    Retorna DataFrame con columnas:
      bucket: string
      user_id: string
      items: array<string>
    (items es el set de servicios por usuario dentro del bucket)
    """
    # RDD parsing para máxima flexibilidad
    rdd = df_raw.rdd.map(lambda r: r['raw_line']).filter(lambda ln: ln is not None and ln.strip() != '')

    def parse_line(ln: str):
        # Quitar decoraciones como "[ ... ]"
        s = ln.strip()
        if s.startswith('[') and s.endswith(']'):
            s = s[1:-1]
        parts = s.split(SEPARATOR)
        if len(parts) < 4:
            return None
        date_str = parts[0].strip()
        user_id  = parts[1].strip()
        run_str  = parts[2].strip()
        svc_raw  = SEPARATOR.join(parts[3:]).strip()  # por si el 4º campo trae ';' internos

        run_val = safe_float(run_str)
        if run_val is None:
            return None
        # Filtro por RUN
        if run_start is not None and run_val < run_start:
            return None
        if run_end is not None and run_val > run_end:
            return None

        bucket = parse_date_bucket(date_str, time_bucket)
        services = split_services(svc_raw)
        if not services:
            return None
        return (bucket, user_id, services)

    parsed = rdd.map(parse_line).filter(lambda x: x is not None)

    # Agrupar por (bucket, user_id) y unir servicios en set
    def merge_sets(a: List[str], b: List[str]) -> List[str]:
        return list(set(a).union(b))

    keyed = parsed.map(lambda x: ((x[0], x[1]), x[2])) \
                  .reduceByKey(merge_sets) \
                  .map(lambda kv: (kv[0][0], kv[0][1], kv[1]))  # (bucket, user_id, items)

    if infer_partitions and infer_partitions > 0:
        keyed = keyed.repartition(infer_partitions)

    df_tx = spark.createDataFrame(keyed, schema=T.StructType([
        T.StructField("bucket", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("items",  T.ArrayType(T.StringType()), False),
    ]))

    # Filtra transacciones vacías o de 1 ítem si no son útiles
    df_tx = df_tx.where(F.size(F.col("items")) >= 1)

    return df_tx


# ---------------------------------------
# FP-Growth por bucket
# ---------------------------------------

def run_fpgrowth_per_bucket(spark: SparkSession,
                            df_tx,
                            min_support: float,
                            min_confidence: float,
                            output_path: str):
    """
    Para cada bucket, ejecuta FP-Growth y guarda:
      - frequent itemsets: {bucket, items, freq, support}
      - association rules (si min_confidence > 0)
    """
    buckets = [r['bucket'] for r in df_tx.select('bucket').distinct().collect()]
    summary = {
        "buckets": [],
        "total_transactions": df_tx.count(),
        "min_support": min_support,
        "min_confidence": min_confidence
    }

    for b in buckets:
        df_b = df_tx.where(F.col('bucket') == F.lit(b)).select('items')
        n_b = df_b.count()
        if n_b == 0:
            continue

        fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence, numPartitions=None)
        model = fp.fit(df_b)

        # Frequent itemsets
        fi = model.freqItemsets.withColumn("bucket", F.lit(b)) \
                               .withColumn("support", (F.col("freq") / F.lit(n_b)).cast("double")) \
                               .select("bucket", "items", "freq", "support")

        out_fi = os.path.join(output_path, "itemsets", f"bucket={b}")
        fi.write.mode("overwrite").json(out_fi)

        saved = {"bucket": b, "transactions": n_b, "itemsets_path": out_fi}

        # Association rules (opcional)
        if min_confidence and min_confidence > 0.0:
            rules = model.associationRules.withColumn("bucket", F.lit(b)) \
                                          .select("bucket", "antecedent", "consequent", "confidence", "lift")
            out_rules = os.path.join(output_path, "rules", f"bucket={b}")
            rules.write.mode("overwrite").json(out_rules)
            saved["rules_path"] = out_rules

        summary["buckets"].append(saved)

    # Guardar un resumen global (métricas)
    metrics_path = os.path.join(output_path, "metrics", "metrics.json")
    (spark.createDataFrame([ (json.dumps(summary),) ], schema=T.StructType([T.StructField("json", T.StringType(), False)]))
          .write.mode("overwrite").text(metrics_path))

    return summary


# ---------------------------------------
# Main
# ---------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', required=True, help='Carpeta o patrón con CSV o ZIP')
    parser.add_argument('--input-format', choices=['csv', 'zip'], default='csv')
    parser.add_argument('--limit-lines', type=int, default=1000000)
    parser.add_argument('--time-bucket', choices=['none', 'day', 'week', 'month'], default='day')
    parser.add_argument('--min-support', type=float, default=0.001)
    parser.add_argument('--min-confidence', type=float, default=0.0)
    parser.add_argument('--run-start', type=float, default=None)
    parser.add_argument('--run-end', type=float, default=None)
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--infer-partitions', type=int, default=None)
    parser.add_argument('--sample-seed', type=int, default=42)

    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("FP-Growth Estacionalidad")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.kryoserializer.buffer.max", "256m")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    print(">>> Parámetros:", vars(args))

    # 1) Lectura
    if args.input_format == 'csv':
        df_raw = read_csv_native(spark, args.input_path, args.limit_lines)
    else:
        df_raw = read_zip_parallel(spark, args.input_path, args.limit_lines)

    total_raw = df_raw.count()
    print(f">>> Líneas crudas leídas: {total_raw}")

    # 2) Transformación → transacciones por (bucket, user)
    df_tx = build_transactions_df(
        spark=spark,
        df_raw=df_raw,
        time_bucket=args.time_bucket,
        run_start=args.run_start,
        run_end=args.run_end,
        infer_partitions=args.infer_partitions
    )

    df_tx.cache()
    n_tx = df_tx.count()
    tx_per_bucket = (df_tx.groupBy('bucket').agg(F.count('*').alias('n_tx'))
                           .orderBy(F.desc('n_tx')))
    print(">>> Transacciones por bucket (top 10):")
    for r in tx_per_bucket.limit(10).collect():
        print(r)

    # 3) FP-Growth por bucket
    summary = run_fpgrowth_per_bucket(
        spark=spark,
        df_tx=df_tx,
        min_support=args.min_support,
        min_confidence=args.min_confidence,
        output_path=args.output_path
    )

    # 4) Heurísticas de recursos/sugerencias para la corrida completa
    #    (en base a la densidad observada en la muestra)
    # Métricas básicas:
    metrics = {}
    metrics['n_raw_lines'] = total_raw
    metrics['n_transactions'] = n_tx
    metrics['avg_items_per_tx'] = df_tx.select(F.size('items').alias('k')).agg(F.avg('k')).first()[0]

    # Estimar cardinalidad de ítems
    items_exploded = df_tx.select(F.explode('items').alias('item'))
    metrics['unique_items'] = items_exploded.select('item').distinct().count()

    # Sugerencias simples:
    suggestions = []

    if metrics['avg_items_per_tx'] and metrics['avg_items_per_tx'] > 10:
        suggestions.append("Transacciones densas: considera subir min-support o prefiltrar a top-K servicios por frecuencia.")

    if metrics['unique_items'] and metrics['unique_items'] > 50000:
        suggestions.append("Muchos ítems (>50k): sube min-support y/o limita a top-50k servicios más frecuentes por ventana.")

    # Propuesta de soporte “seguro” por escala:
    # Si hay < 100k transacciones por bucket: prueba min-support >= 0.002–0.005
    # Si hay 100k–1M por bucket: 0.001–0.003
    # Si hay > 1M por bucket: 0.0005–0.002 (ojo con explosión de itemsets)
    buckets_stats = tx_per_bucket.collect()
    if buckets_stats:
        max_bucket_tx = max(r['n_tx'] for r in buckets_stats)
        if max_bucket_tx < 100_000:
            suggestions.append("Sugerencia soporte: 0.002–0.005 (por bucket pequeño).")
        elif max_bucket_tx < 1_000_000:
            suggestions.append("Sugerencia soporte: 0.001–0.003 (por bucket mediano).")
        else:
            suggestions.append("Sugerencia soporte: 0.0005–0.002 (por bucket grande, precaución por cardinalidad).")

    # Sugerencia de recursos (muy general, ajusta a tu clúster):
    # Basado en n_tx y unique_items
    if n_tx < 2_000_000 and metrics['unique_items'] < 100_000:
        suggestions.append("Recursos: driver 4–8GB, executors 4–8GB, 4–8 cores c/u, 200–400 partitions.")
    else:
        suggestions.append("Recursos: driver 8–16GB, executors 8–16GB, 8+ cores c/u, 400–800 partitions.")

    metrics['suggestions'] = suggestions

    # Guardar métricas/sugerencias
    out_metrics_dir = os.path.join(args.output_path, "metrics")
    (spark.createDataFrame([ (json.dumps(metrics),) ], schema=T.StructType([T.StructField("json", T.StringType(), False)]))
          .write.mode("overwrite").text(os.path.join(out_metrics_dir, "observations.json")))

    print(">>> MÉTRICAS (muestra):", json.dumps(metrics, ensure_ascii=False, indent=2))

    spark.stop()


if __name__ == '__main__':
    main()
