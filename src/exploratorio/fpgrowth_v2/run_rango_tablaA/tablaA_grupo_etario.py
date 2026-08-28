#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, io, csv, re, zipfile, argparse, time
from collections import defaultdict, Counter
from tqdm import tqdm
import pandas as pd

# ---------------- Utilidades ----------------

def _fmt_hms(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def _clean(s: str) -> str:
    return str(s).strip().strip('"').strip()

def load_client_mapping(filepath):
    """
    Carga dim_integracion_cu.csv con columnas:
      client_id,nombre_app,institucion
    Retorna: dict[client_id] -> (nombre_app, institucion)
    """
    mapping = {}
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = [c.strip().lower() for c in (reader.fieldnames or [])]
        required = {"client_id", "nombre_app", "institucion"}
        missing = required - set(cols)
        if missing:
            raise ValueError(f"Faltan columnas en {os.path.basename(filepath)}: {missing}. Presentes: {cols}")
        for row in reader:
            cid = _clean(row["client_id"])
            nombre = _clean(row["nombre_app"])
            inst = _clean(row["institucion"])
            if cid:
                mapping[cid] = (nombre or cid, inst or "")
    return mapping

def load_sector_mapping(filepath):
    """
    Carga cu_con_sectores.csv (opcional) con columnas:
      client_id,nombre_app,institucion,sector
    Retorna: dict[client_id] -> sector
    """
    sector = {}
    if not filepath:
        return sector
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = [c.strip().lower() for c in (reader.fieldnames or [])]
        if "client_id" not in cols or "sector" not in cols:
            return sector
        for row in reader:
            cid = _clean(row.get("client_id", ""))
            sec = _clean(row.get("sector", ""))
            if cid and sec:
                sector[cid] = sec
    return sector

# --------- Grupos A–F desde rango_run (columna 3) ---------

def groups_from_rango(rango_str):
    """
    Rango RUN → grupos A–F con FRONTERAS SOLAPADAS (ambos extremos inclusivos):
      F:  1–10   | E: 10–14 | D: 14–17 | C: 17–19 | B: 20–22 | A: 22–23
    p.ej.: 10→{E,F}, 14→{D,E}, 17→{C,D}, 22→{A,B}
    """
    s = _clean(rango_str).replace(",", ".")
    if not s:
        return set()
    m = re.search(r"-?\d+", s)
    if not m:
        return set()
    try:
        v = int(m.group())
    except Exception:
        return set()

    g = set()
    if 1  <= v <= 10: g.add("F")
    if 10 <= v <= 14: g.add("E")
    if 14 <= v <= 17: g.add("D")
    if 17 <= v <= 19: g.add("C")
    if 20 <= v <= 22: g.add("B")
    if 22 <= v <= 23: g.add("A")
    return g

# --------------- Motor principal ----------------

def build_tablaA_from_zip(zip_path, dim_csv, out_dir, sep=";", cu_csv=None):
    """
    Lee ZIP principal con ZIPs internos. Cada CSV interno (sin encabezado) tiene filas:
      0: fecha        (YYYY-MM-DD)
      1: run_anon     (ID de persona anonimizado y estable)   <-- denominador usuarios por grupo
      2: rango_run    (entero, asigna grupos A–F con solapes) <-- define grupo(s)
      3: client_id(s) (1..N códigos de servicio separados por coma)
    Usa dim_integracion_cu.csv para traducir client_id -> nombre_app, institucion.
    Opcionalmente cu_con_sectores.csv para añadir 'sector'.

    Salidas:
      - tablaA_top10_all.csv (Top-10 por grupo A–F, combinados)
      - tablaA_top10_A..F.csv (uno por grupo)
      - resumen_por_grupo.csv (estadísticos auxiliares)
    """
    os.makedirs(out_dir, exist_ok=True)

    # Dimensiones
    name_map = load_client_mapping(dim_csv)       # client_id -> (nombre_app, institucion)
    sector_map = load_sector_mapping(cu_csv)      # client_id -> sector (opcional)

    # Acumuladores
    freq_events = Counter()                       # (grupo, client_id) -> conteo de eventos
    users_per_group = defaultdict(set)            # grupo -> set(run_anon)
    users_per_group_service = defaultdict(set)    # (grupo, client_id) -> set(run_anon)

    # Recorrer ZIP principal
    with zipfile.ZipFile(zip_path, "r") as top_zip:
        inner_zips = [n for n in top_zip.namelist() if n.lower().endswith(".zip")]
        inner_zips.sort()
        tqdm.write(f"📦 ZIP principal: {zip_path} — ZIPs internos: {len(inner_zips)}")

        with tqdm(inner_zips, desc="Leyendo ZIPs internos", dynamic_ncols=True) as bar:
            for inner_name in bar:
                try:
                    inner_bytes = top_zip.read(inner_name)
                except KeyError:
                    tqdm.write(f"⚠️ No se pudo leer: {inner_name}")
                    continue

                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_members = [m for m in inner_zip.namelist() if m.lower().endswith(".csv")]
                    if not csv_members:
                        tqdm.write(f"⚠️ Sin CSV en {inner_name}")
                        continue

                    for csv_name in csv_members:
                        with inner_zip.open(csv_name, "r") as fh_bin:
                            with io.TextIOWrapper(fh_bin, encoding="utf-8", newline="") as fh:
                                reader = csv.reader(fh, delimiter=sep, quotechar='"')
                                first = next(reader, None)
                                if first is None:
                                    continue

                                # Si no hay header: 1ª celda parece fecha yyyy-mm-dd y hay >= 4 columnas
                                def looks_like_date(x):
                                    return bool(re.match(r"\d{4}-\d{2}-\d{2}", _clean(x)))

                                if len(first) >= 4 and looks_like_date(first[0]):
                                    rows_iter = [first]
                                    rows_iter.extend(reader)
                                else:
                                    # Hay encabezado o 1ª fila no es fecha: asumimos header y seguimos
                                    rows_iter = reader

                                for row in rows_iter:
                                    if not row or len(row) < 4:
                                        continue
                                    run_anon = _clean(row[1])
                                    rango = _clean(row[2])
                                    cids_field = _clean(row[3])
                                    if not run_anon or not rango or not cids_field:
                                        continue

                                    grupos = groups_from_rango(rango)
                                    if not grupos:
                                        continue

                                    # Múltiples client_id por coma
                                    for cid in cids_field.split(","):
                                        cid = _clean(cid)
                                        if not cid:
                                            continue

                                        # Para cada grupo aplicable (fronteras solapadas)
                                        for g in grupos:
                                            freq_events[(g, cid)] += 1
                                            users_per_group[g].add(run_anon)
                                            users_per_group_service[(g, cid)].add(run_anon)

    # Construir filas finales
    rows = []
    for (g, cid), eventos in freq_events.items():
        usuarios_tot_grupo = len(users_per_group[g])
        usuarios_serv_grupo = len(users_per_group_service[(g, cid)])
        tasa = 1000.0 * usuarios_serv_grupo / usuarios_tot_grupo if usuarios_tot_grupo > 0 else None
        nombre_app, institucion = name_map.get(cid, (cid, ""))
        sector = sector_map.get(cid, "")

        r = {
            "grupo": g,
            "nombre_app": nombre_app,
            "institucion": institucion,
            "frecuencia_eventos": eventos,
            "usuarios_unicos_servicio": usuarios_serv_grupo,
            "usuarios_totales_grupo": usuarios_tot_grupo,
            "tasa_por_1000_usuarios": round(tasa, 3) if tasa is not None else None
        }
        if sector:
            r["sector"] = sector
        rows.append(r)

    if not rows:
        tqdm.write("⚠️ No se generaron filas: revisa formato (4 columnas) y contenidos.")
        return

    df = pd.DataFrame(rows)

    # Top-10 por grupo (orden: tasa, usuarios servicio, eventos)
    df["__ord__"] = df.groupby("grupo")["tasa_por_1000_usuarios"].rank(method="first", ascending=False)
    top10 = (df[df["__ord__"] <= 10]
             .drop(columns=["__ord__"])
             .sort_values(["grupo", "tasa_por_1000_usuarios", "usuarios_unicos_servicio", "frecuencia_eventos"],
                          ascending=[True, False, False, False]))

    os.makedirs(out_dir, exist_ok=True)

    # CSV por grupo
    for g, dfg in top10.groupby("grupo"):
        dfg.to_csv(os.path.join(out_dir, f"tablaA_top10_{g}.csv"), index=False, encoding="utf-8")

    # CSV combinado
    top10.to_csv(os.path.join(out_dir, "tablaA_top10_all.csv"), index=False, encoding="utf-8")

    # Resumen auxiliar
    resumen = (df.groupby("grupo", as_index=False)
                 .agg(n_servicios=("nombre_app", "nunique"),
                      eventos_total=("frecuencia_eventos", "sum"),
                      usuarios_unicos_total=("usuarios_unicos_servicio", "sum"),
                      usuarios_totales_grupo=("usuarios_totales_grupo", "max"),
                      tasa_media_por_1000_usuarios=("tasa_por_1000_usuarios", "mean"))
                 .sort_values("grupo"))
    resumen.to_csv(os.path.join(out_dir, "resumen_por_grupo.csv"), index=False, encoding="utf-8")

    print(f"✅ Tabla A (por 1.000 usuarios) generada en: {out_dir}")

# ---------------- CLI ----------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Genera Tabla A (Top-10 por tasa por 1.000 usuarios) por grupos A–F desde ZIP anidado."
    )
    p.add_argument("--zip", required=True, help="ZIP principal con ZIPs internos. CSVs sin header: fecha;run_anon;rango_run;client_id(s)")
    p.add_argument("--dim", required=True, help="dim_integracion_cu.csv (client_id,nombre_app,institucion)")
    p.add_argument("--out", required=True, help="Carpeta de salida para CSVs.")
    p.add_argument("--sep", default=";", help="Separador de columnas en CSV internos (default ';').")
    p.add_argument("--cu", default=None, help="(Opcional) cu_con_sectores.csv para añadir 'sector' por client_id.")
    return p.parse_args()

def main():
    args = parse_args()
    t0 = time.perf_counter()
    build_tablaA_from_zip(args.zip, args.dim, args.out, sep=args.sep, cu_csv=args.cu)
    elapsed = time.perf_counter() - t0
    print(f"⏱️ Tiempo total: {_fmt_hms(elapsed)}")

if __name__ == "__main__":
    main()
