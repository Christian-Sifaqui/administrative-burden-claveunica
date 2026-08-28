#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, csv, re, zipfile, argparse, time
from typing import Iterator, Tuple, Iterable
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# --------- utilidades locales ---------
def fmt_hms(sec: float) -> str:
    m, s = divmod(int(sec + 0.5), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# mapeo A–F con solapes (ambos extremos inclusivos)
def groups_from_rango_int(v: int):
    g = []
    if 1  <= v <= 10: g.append("F")
    if 10 <= v <= 14: g.append("E")
    if 14 <= v <= 17: g.append("D")
    if 17 <= v <= 19: g.append("C")
    if 20 <= v <= 22: g.append("B")
    if 22 <= v <= 23: g.append("A")
    return g

# --------- parseo distribuido de ZIP->filas ---------
def parse_top_zip(record: Tuple[str, bytes], sep: str) -> Iterable[Tuple[str, str, str]]:
    """
    Entrada: (path, bytes_del_zip_principal)
    Salida: iter de (grupo, client_id, run_anon)
    CSV interno (sin header): fecha;run_anon;rango_run;client_id(s)
    """
    path, blob = record
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as top:
            for inner_name in top.namelist():
                if not inner_name.lower().endswith(".zip"):
                    continue
                try:
                    inner_bytes = top.read(inner_name)
                except KeyError:
                    continue
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
                    for csv_name in inner.namelist():
                        if not csv_name.lower().endswith(".csv"):
                            continue
                        with inner.open(csv_name, 'r') as fhb:
                            with io.TextIOWrapper(fhb, encoding='utf-8', newline='') as fht:
                                reader = csv.reader(fht, delimiter=sep, quotechar='"')
                                # detectar si la primera fila es dato o header
                                first = next(reader, None)
                                if first is None:
                                    continue
                                def looks_like_date(x):
                                    return bool(re.match(r"\d{4}-\d{2}-\d{2}", str(x).strip()))
                                if len(first) >= 4 and looks_like_date(first[0]):
                                    rows_iter = [first]
                                    rows_iter.extend(reader)
                                else:
                                    rows_iter = reader
                                # procesar filas
                                for row in rows_iter:
                                    if not row or len(row) < 4:
                                        continue
                                    run_anon = str(row[1]).strip().strip('"')
                                    rango_s = str(row[2]).strip().strip('"')
                                    cids = str(row[3]).strip().strip('"')
                                    if not run_anon or not rango_s or not cids:
                                        continue
                                    m = re.search(r"-?\d+", rango_s.replace(",", "."))
                                    if not m:
                                        continue
                                    try:
                                        rango = int(m.group())
                                    except Exception:
                                        continue
                                    grupos = groups_from_rango_int(rango)
                                    if not grupos:
                                        continue
                                    for cid in cids.split(','):
                                        cid = cid.strip().strip('"')
                                        if not cid:
                                            continue
                                        for g in grupos:
                                            # (grupo, client_id, run_anon)
                                            yield (g, cid, run_anon)
    except zipfile.BadZipFile:
        return

# --------- main spark job ---------
def main():
    ap = argparse.ArgumentParser(description="Tabla A (Top-10 por 1000 usuarios) con Spark desde ZIP anidado")
    ap.add_argument("--zip-glob", required=True,
                   help="Ruta/GLob a ZIPs principales (ej: hdfs:///path/*.zip o file:///.../*.zip)")
    ap.add_argument("--dim", required=True,
                   help="dim_integracion_cu.csv (client_id,nombre_app,institucion)")
    ap.add_argument("--cu", default=None,
                   help="(opcional) cu_con_sectores.csv (client_id,nombre_app,institucion,sector)")
    ap.add_argument("--out", required=True, help="Carpeta de salida (CSV)")
    ap.add_argument("--sep", default=";", help="Separador en CSVs internos (default ';')")
    ap.add_argument("--partitions", type=int, default=None, help="Reparticiones del RDD tras parseo (opcional)")
    ap.add_argument("--shuffle", type=int, default=200, help="spark.sql.shuffle.partitions (default 200)")
    ap.add_argument("--app-name", default="TablaA_Spark_Zip", help="Nombre de la app Spark")
    ap.add_argument("--tz", default="America/Santiago", help="Zona horaria Spark")
    args = ap.parse_args()

    t0 = time.time()
    spark = (SparkSession.builder
             .appName(args.app_name)
             .config("spark.sql.session.timeZone", args.tz)
             .config("spark.sql.shuffle.partitions", str(args.shuffle))
             .getOrCreate())
    sc = spark.sparkContext

    # 1) Leer ZIP(s) principal(es) en paralelo
    #    binaryFiles soporta glob y reparte por archivo (ideal si tienes varios ZIP grandes)
    rdd = sc.binaryFiles(args.zip_glob)

    # 2) Parseo distribuido a (grupo, client_id, run_anon)
    parsed = rdd.flatMap(lambda rec: parse_top_zip(rec, args.sep))
    if args.partitions:
        parsed = parsed.repartition(args.partitions)

    schema = T.StructType([
        T.StructField("grupo", T.StringType(), False),
        T.StructField("client_id", T.StringType(), False),
        T.StructField("run_anon", T.StringType(), False),
    ])
    df = spark.createDataFrame(parsed, schema=schema).persist()

    # 3) Agregaciones
    # usuarios_totales_grupo
    usuarios_grupo = (df.groupBy("grupo")
                        .agg(F.countDistinct("run_anon").alias("usuarios_totales_grupo")))

    # por (grupo, client_id): eventos y usuarios únicos
    acc = (df.groupBy("grupo", "client_id")
             .agg(F.count(F.lit(1)).alias("frecuencia_eventos"),
                  F.countDistinct("run_anon").alias("usuarios_unicos_servicio")))

    # 4) Dimensiones
    dim = (spark.read
           .option("header", True)
           .option("multiLine", False)
           .csv(args.dim)
           .select(
               F.col("client_id").alias("dim_client_id"),
               F.col("nombre_app"),
               F.col("institucion")
           ))

    # (opcional) sectores
    if args.cu:
        cu = (spark.read
              .option("header", True)
              .csv(args.cu)
              .select(F.col("client_id").alias("cu_client_id"),
                      F.col("sector")))
    else:
        cu = None

    # 5) Join + tasa
    base = (acc.join(usuarios_grupo, "grupo")
              .join(F.broadcast(dim), acc.client_id == F.col("dim_client_id"), "left")
              .drop("dim_client_id"))

    if cu is not None:
        base = (base.join(F.broadcast(cu), base.client_id == F.col("cu_client_id"), "left")
                    .drop("cu_client_id"))

    base = (base
            .withColumn("tasa_por_1000_usuarios",
                        F.when(F.col("usuarios_totales_grupo") > 0,
                               1000.0 * F.col("usuarios_unicos_servicio") / F.col("usuarios_totales_grupo"))
                         .otherwise(F.lit(None)))
            )

    # 6) Top-10 por grupo
    w = Window.partitionBy("grupo").orderBy(F.col("tasa_por_1000_usuarios").desc(),
                                           F.col("usuarios_unicos_servicio").desc(),
                                           F.col("frecuencia_eventos").desc())
    top10 = (base
             .withColumn("rk", F.row_number().over(w))
             .filter(F.col("rk") <= 10)
             .drop("rk"))

    # 7) Orden de columnas y escritura
    cols = ["grupo", "nombre_app", "institucion", "client_id",
            "frecuencia_eventos", "usuarios_unicos_servicio", "usuarios_totales_grupo",
            "tasa_por_1000_usuarios"]
    if "sector" in top10.columns:
        cols = ["grupo", "nombre_app", "institucion", "sector", "client_id",
                "frecuencia_eventos", "usuarios_unicos_servicio", "usuarios_totales_grupo",
                "tasa_por_1000_usuarios"]

    (top10.select(*cols)
          .coalesce(1)
          .write.mode("overwrite").option("header", True)
          .csv(args.out + "/tablaA_top10_all"))

    # por grupo
    for g in ["A","B","C","D","E","F"]:
        tg = top10.filter(F.col("grupo")==g)
        if tg.rdd.isEmpty():
            continue
        (tg.select(*cols)
           .coalesce(1)
           .write.mode("overwrite").option("header", True)
           .csv(f"{args.out}/tablaA_top10_{g}"))

    # resumen auxiliar
    resumen = (base.groupBy("grupo")
               .agg(F.countDistinct("client_id").alias("n_servicios"),
                    F.sum("frecuencia_eventos").alias("eventos_total"),
                    F.sum("usuarios_unicos_servicio").alias("usuarios_unicos_total"),
                    F.max("usuarios_totales_grupo").alias("usuarios_totales_grupo"),
                    F.round(F.avg("tasa_por_1000_usuarios"), 3).alias("tasa_media_por_1000_usuarios"))
               .orderBy("grupo"))

    (resumen.coalesce(1)
            .write.mode("overwrite").option("header", True)
            .csv(args.out + "/resumen_por_grupo"))

    spark.stop()
    print(f"✅ Listo. Salidas en: {args.out}  |  tiempo: {fmt_hms(time.time()-t0)}")

if __name__ == "__main__":
    main()
