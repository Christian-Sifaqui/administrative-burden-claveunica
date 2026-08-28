#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Estacionalidad mensual por sector desde ZIP principal con ZIPs internos y CSVs.

Formato de cada fila en los CSV internos (del ejemplo que diste):
  fecha;run_anon;run_rango_mm;client_ids
Ejemplos de client_ids:
  ab2dd6e0..., "2549b5...", b4206a...,c5b83b...,d602a0...
Puede venir entrecomillado y con múltiples IDs separados por coma.

Métrica por defecto:
  - persona-día por sector (una persona en un día cuenta 1 por sector,
    aunque use varios servicios de ese mismo sector ese día).
  - Luego se agregan los conteos por mes (YYYY-MM).

Requisitos:
  - cu_con_sectores.csv con columnas al menos: client_id, sector
  - Python 3.8+, psutil, tqdm, pandas (solo para export opcional ordenada)

Uso sugerido:
  python estacionalidad_mensual_sector.py \
    --main-zip /ruta/al/zip_principal.zip \
    --mapping /ruta/cu_con_sectores.csv \
    --out-csv ./estacionalidad_mensual_sector.csv \
    --plots-dir ./plots  # opcional

Opciones adicionales:
  --include-unknown          Incluir sector 'Desconocido' (por defecto se excluye)
  --min-month 2024-01        Filtra resultados a partir de este mes (inclusive)
  --max-month 2025-12        Filtra resultados hasta este mes (inclusive)
"""

from __future__ import annotations
import os
import io
import csv
import sys
import zipfile
from collections import defaultdict
from typing import Dict, Iterable, Tuple, List, Optional, Set

import psutil
from tqdm import tqdm

# matplotlib es opcional (solo si se pide --plots-dir)
try:
    import matplotlib.pyplot as plt
except Exception:
    plt = None

# pandas solo para ordenar y escribir más cómodo (puedes quitarlo si no lo quieres)
try:
    import pandas as pd
except Exception:
    pd = None


def log_sys(prefix: str = "") -> None:
    try:
        mem = psutil.virtual_memory()
        tqdm.write(f"{prefix} RAM: {mem.percent:.2f}% usada ({mem.used / (1024**3):.2f} GB)")
    except Exception:
        pass


def read_mapping(mapping_csv: str) -> Dict[str, str]:
    """
    Lee cu_con_sectores.csv y devuelve dict client_id -> sector.
    Si 'sector' vacío, asigna 'Desconocido'.
    """
    mapping: Dict[str, str] = {}
    with open(mapping_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # detectar posibles nombres alternativos
        fieldnames = [c.strip() for c in (reader.fieldnames or [])]
        try:
            ci_col = next(c for c in fieldnames if c.lower() == "client_id")
        except StopIteration:
            raise ValueError("No se encontró columna 'client_id' en el mapping CSV.")
        # 'sector' puede existir con ese nombre; si no, intentamos alternativas
        sec_col = "sector" if "sector" in fieldnames else None
        if sec_col is None:
            # intenta algunas variantes:
            for cand in ["Sector", "SECTOR"]:
                if cand in fieldnames:
                    sec_col = cand
                    break
        if sec_col is None:
            raise ValueError("No se encontró columna 'sector' en el mapping CSV.")

        for row in reader:
            cid = (row.get(ci_col) or "").strip().strip('"').strip("'")
            sec = (row.get(sec_col) or "").strip() or "Desconocido"
            if cid:
                mapping[cid] = sec
    return mapping


def month_from_date_str(datestr: str) -> Optional[str]:
    """
    Espera 'YYYY-MM-DD'. Devuelve 'YYYY-MM' o None si no parseable.
    """
    if not datestr or len(datestr) < 7:
        return None
    # formato muy regular: yyyy-mm-dd; validamos mínimo yyyy-mm
    try:
        y = datestr[0:4]
        m = datestr[5:7]
        # validaciones simples
        if not (y.isdigit() and m.isdigit()):
            return None
        return f"{y}-{m}"
    except Exception:
        return None


def parse_client_ids(raw_field: str) -> List[str]:
    """
    Recibe el cuarto campo (posible entrecomillado) y devuelve lista de client_id limpios.
    Soporta múltiples IDs separados por coma. Filtra vacíos.
    """
    if raw_field is None:
        return []
    s = raw_field.strip().strip('"').strip("'")
    if not s:
        return []
    parts = [x.strip().strip('"').strip("'") for x in s.split(",")]
    return [p for p in parts if p]


def process_inner_csv(file_obj, mapping: Dict[str, str], counters: Dict[Tuple[str, str], int],
                      include_unknown: bool) -> None:
    """
    Procesa un CSV (con ';') con columnas:
      0: fecha YYYY-MM-DD
      1: run_anon (no lo usamos para el conteo, pero la métrica es persona-día-sector, así que deduplicamos por sector en la fila)
      2: rango_run_mm (no se usa)
      3: client_ids (lista separada por coma; puede venir entrecomillada)
    Lógica:
      - Para cada fila, se mapean client_ids -> sectores
      - Se deduplican sectores por fila (persona-día)
      - Se incrementa en 1 el contador (mes, sector) por cada sector presente en esa fila
    """
    # Leemos como CSV sin cabecera, delimitador ';'
    reader = csv.reader(io.TextIOWrapper(file_obj, encoding="utf-8", newline=""), delimiter=";", quotechar='"')
    for row in reader:
        if not row or len(row) < 4:
            continue
        datestr = (row[0] or "").strip()
        month = month_from_date_str(datestr)
        if not month:
            continue

        # run_anon = (row[1] or "").strip()  # no requerido para el conteo agregado
        # rango = (row[2] or "").strip()     # no requerido

        client_field = row[3]
        cids = parse_client_ids(client_field)
        if not cids:
            continue

        # mapear a sectores y deduplicar por fila (persona-día)
        sectors: Set[str] = set()
        for cid in cids:
            sec = mapping.get(cid, "Desconocido")
            if sec == "Desconocido" and not include_unknown:
                continue
            sectors.add(sec)

        if not sectors:
            continue

        for sec in sectors:
            counters[(month, sec)] += 1


def iterate_zip_like(main_zip_path: str) -> Iterable[Tuple[str, bytes]]:
    """
    Itera el ZIP principal y rinde tuplas (nombre_csv, bytes_csv) para cada CSV
    encontrado dentro de ZIPs internos.
    """
    with zipfile.ZipFile(main_zip_path, "r") as top_zip:
        inner = [n for n in top_zip.namelist() if n.lower().endswith(".zip")]
        inner.sort()
        tqdm.write(f"ZIP principal: {main_zip_path} — ZIPs internos: {len(inner)}")
        with tqdm(inner, desc="Leyendo ZIPs internos", dynamic_ncols=True) as bar:
            for inner_name in bar:
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    tqdm.write(f"⚠️ No se pudo leer entrada: {inner_name}")
                    continue
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith(".csv")]
                    if not csv_members:
                        tqdm.write(f"⚠️ Sin CSV dentro de {inner_name}")
                        continue
                    # asume 1 CSV por ZIP interno
                    csv_name = csv_members[0]
                    yield csv_name, inner_zip.read(csv_name)


def write_output_csv(counters: Dict[Tuple[str, str], int], out_csv: str,
                     min_month: Optional[str] = None, max_month: Optional[str] = None) -> None:
    """
    Escribe CSV con columnas: month, sector, count
    Ordenado por month asc, count desc (por mes).
    """
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)) or ".", exist_ok=True)

    records = []
    for (month, sector), cnt in counters.items():
        if min_month and month < min_month:
            continue
        if max_month and month > max_month:
            continue
        records.append((month, sector, cnt))

    if not records:
        # crear archivo vacío con cabecera
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["month", "sector", "count"])
        return

    # Si hay pandas, ordenamos bonito; si no, orden manual
    if pd is not None:
        df = pd.DataFrame(records, columns=["month", "sector", "count"])
        # ordenar por month asc, y dentro del mes por count desc
        df["__order__"] = df["month"]
        df = df.sort_values(by=["__order__", "count"], ascending=[True, False]).drop(columns="__order__")
        df.to_csv(out_csv, index=False)
    else:
        # orden básico: month asc, count desc
        records.sort(key=lambda x: (x[0], -x[2], x[1]))
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["month", "sector", "count"])
            w.writerows(records)


def maybe_make_plots(counters: Dict[Tuple[str, str], int], plots_dir: Optional[str],
                     min_month: Optional[str] = None, max_month: Optional[str] = None) -> None:
    """
    Genera una serie temporal por sector (un PNG por sector), con conteos mensuales.
    Requiere matplotlib. Se salta si no está disponible o no se especifica plots_dir.
    """
    if not plots_dir or plt is None:
        return

    os.makedirs(plots_dir, exist_ok=True)

    # construir estructura: sector -> {month -> count}
    per_sector: Dict[str, Dict[str, int]] = defaultdict(dict)
    months_all: Set[str] = set()

    for (month, sector), cnt in counters.items():
        if min_month and month < min_month:
            continue
        if max_month and month > max_month:
            continue
        per_sector[sector][month] = cnt
        months_all.add(month)

    if not per_sector:
        return

    months_sorted = sorted(months_all)

    for sector, mm in per_sector.items():
        ys = [mm.get(m, 0) for m in months_sorted]
        plt.figure(figsize=(10, 5))
        plt.plot(months_sorted, ys, marker="o")
        plt.title(f"Estacionalidad mensual — {sector}")
        plt.xlabel("Mes (YYYY-MM)")
        plt.ylabel("Personas-día (conteo mensual)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        out_path = os.path.join(plots_dir, f"estacionalidad_{sector.replace('/', '_').replace(' ', '_')}.png")
        plt.savefig(out_path, dpi=150)
        plt.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Estacionalidad mensual por sector desde ZIPs con CSVs")
    parser.add_argument("--main-zip", required=True, help="Ruta al ZIP principal que contiene ZIPs internos con CSVs")
    parser.add_argument("--mapping", required=True, help="Ruta a cu_con_sectores.csv (client_id, sector, ...)")
    parser.add_argument("--out-csv", default="./estacionalidad_mensual_sector.csv", help="Ruta de salida CSV")
    parser.add_argument("--plots-dir", default=None, help="Directorio para PNGs por sector (opcional)")
    parser.add_argument("--include-unknown", action="store_true", help="Incluir sector 'Desconocido'")
    parser.add_argument("--min-month", default=None, help="Filtrar desde este mes inclusive (formato YYYY-MM)")
    parser.add_argument("--max-month", default=None, help="Filtrar hasta este mes inclusive (formato YYYY-MM)")
    args = parser.parse_args()

    tqdm.write("🚀 Estacionalidad mensual por sector")
    log_sys("Inicio — ")

    # 1) Mapping client_id -> sector
    mapping = read_mapping(args.mapping)
    tqdm.write(f"✔️ Mapeos cargados: {len(mapping):,} client_id con sector")

    # 2) Contadores (month, sector) -> personas-día
    counters: Dict[Tuple[str, str], int] = defaultdict(int)

    # 3) Recorrer ZIP principal y CSVs internos
    for csv_name, csv_bytes in iterate_zip_like(args.main_zip):
        tqdm.write(f"📑 Procesando CSV: {csv_name}")
        with io.BytesIO(csv_bytes) as fb:
            process_inner_csv(fb, mapping, counters, include_unknown=args.include_unknown)

    # 4) Exportar
    write_output_csv(counters, args.out_csv, args.min_month, args.max_month)
    tqdm.write(f"💾 CSV escrito: {args.out_csv}")

    # 5) Graficar (opcional)
    if args.plots_dir:
        if plt is None:
            tqdm.write("ℹ️ matplotlib no está disponible; no se generarán gráficos.")
        else:
            tqdm.write("📈 Generando gráficos por sector...")
            maybe_make_plots(counters, args.plots_dir, args.min_month, args.max_month)
            tqdm.write(f"🖼️ PNGs en: {args.plots_dir}")

    log_sys("Fin — ")
    tqdm.write("✅ Listo.")


if __name__ == "__main__":
    main()
