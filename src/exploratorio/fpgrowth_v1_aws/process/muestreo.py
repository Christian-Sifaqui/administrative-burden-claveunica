import io
import os
import zipfile
import tempfile
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import polars as pl
from tqdm import tqdm

# ---------------- CONFIG ----------------
ZIP_PATHS = [
    "../data/cu_user_client-2024.zip",
    "../data/cu_user_client-2025.zip",
]

MES_OBJETIVO = "2024-03" # formato YYYY-MM, coincide con inicio de la fecha en el CSV
OUTPUT_DIR = Path("output_mes")
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_SERVICIOS_POR_PERSONA = 100
SOPORTE_MIN = 20
LIFT_MIN = 1.5

# ---------------- FUNCIONES ----------------
def map_rango(rango: float) -> str:
    if 22 <= rango <= 23: return "A"
    if 20 <= rango < 22: return "B"
    if 17 <= rango < 20: return "C"
    if 14 <= rango < 17: return "D"
    if 10 <= rango < 14: return "E"
    return "F"

def parse_line(line: str):
    try:
        p = line.strip().split(";")
        if len(p) < 4: return None
        fecha = p[0] # ej: 2024-01-15
        persona = p[1]
        rango = float(p[2])
        servicios = [s.strip() for s in p[3].replace('"','').split(",") if s.strip()]
        if not persona or not servicios: return None
        return fecha, persona, rango, servicios
    except:
        return None

# ---------------- PROCESAR MES POR FECHA ----------------
def procesar_mes():
    historial = {}
    servicio_id = {}
    next_id = 0
    lineas_leidas = 0
    lineas_mes = 0

    for zip_path in ZIP_PATHS:
        with zipfile.ZipFile(zip_path) as z:
            inners = [n for n in z.namelist() if n.endswith(".zip")]
            for inner in tqdm(inners, desc=f"Escaneando {Path(zip_path).name}"):
                with z.open(inner) as src:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp.write(src.read())
                        tmp_path = tmp.name
                try:
                    with zipfile.ZipFile(tmp_path) as inner_zip:
                        for csv_name in [n for n in inner_zip.namelist() if n.endswith(".csv")]:
                            with inner_zip.open(csv_name) as f:
                                txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                                next(txt, None) # header
                                for line in txt:
                                    lineas_leidas += 1
                                    parsed = parse_line(line)
                                    if not parsed: continue
                                    fecha, persona, rango, servicios = parsed

                                    # FILTRO POR MES
                                    if not fecha.startswith(MES_OBJETIVO):
                                        continue

                                    lineas_mes += 1
                                    grupo = map_rango(rango)

                                    # codifica servicios
                                    serv_ids = []
                                    for s in servicios:
                                        if s not in servicio_id:
                                            servicio_id[s] = next_id
                                            next_id += 1
                                        serv_ids.append(servicio_id[s])

                                    if persona not in historial:
                                        historial[persona] = {"grupo": grupo, "servicios": set()}
                                    historial[persona]["servicios"].update(serv_ids[:MAX_SERVICIOS_POR_PERSONA])
                finally:
                    os.unlink(tmp_path)

    print(f"Líneas totales leídas: {lineas_leidas:,}")
    print(f"Líneas del mes {MES_OBJETIVO}: {lineas_mes:,}")
    print(f"Personas únicas en el mes: {len(historial):,}")

    id_servicio = {v:k for k,v in servicio_id.items()}

    # conteos por grupo
    conteo_serv = defaultdict(lambda: defaultdict(int))
    conteo_pares = defaultdict(lambda: defaultdict(int))
    total_por_grupo = defaultdict(int)

    for data in tqdm(historial.values(), desc="Contando"):
        g = data["grupo"]
        servs = list(data["servicios"])
        total_por_grupo[g] += 1
        for s in servs:
            conteo_serv[g][s] += 1
        for a,b in combinations(sorted(servs), 2):
            conteo_pares[g][(a,b)] += 1

    filas = []
    for g, total in total_por_grupo.items():
        for (a,b), n_ab in conteo_pares[g].items():
            if n_ab < SOPORTE_MIN: continue
            n_a = conteo_serv[g][a]
            n_b = conteo_serv[g][b]
            p_ab = n_ab / total
            lift = p_ab / ((n_a/total)*(n_b/total))
            if lift < LIFT_MIN: continue
            filas.append({
                "grupo": g,
                "servicio_a": id_servicio[a],
                "servicio_b": id_servicio[b],
                "n_ab": n_ab,
                "lift": round(lift, 3),
                "total_personas": total
            })

    df = pl.DataFrame(filas).sort(["grupo","lift"], descending=[False, True])
    out = OUTPUT_DIR / f"lift_{MES_OBJETIVO}.parquet"
    df.write_parquet(out)
    print(f"\nGuardado: {out} con {len(df):,} pares")
    print(df.group_by("grupo").agg(pl.len().alias("pares")).sort("grupo"))

if __name__ == "__main__":
    procesar_mes()
