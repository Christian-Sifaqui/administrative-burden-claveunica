"""
Camino A · corrida completa (AWS) — co-ocurrencia intra-día, orden real y lags en minutos
=============================================================================================
Port a escala completa del prototipo de sandbox (paso0/1/3 en camino_a_precedencia/).
Procesa TODOS los outer zips de datos_2026/ en streaming (io.BytesIO, sin descomprimir
a disco — mismo patrón ya usado en computador_aws/process/*/reglas_temporales*.py),
acumulando por PAR DIRECCIONAL de servicios (a ocurre antes que b, misma persona-día):
    - soporte direccional (conteo)
    - suma y suma-de-cuadrados del lag en minutos (media/desvest exactas)
    - una muestra-reservorio de lags (percentiles aproximados, sin guardar cada lag)

Cada fila del CSV ya es una persona-día completa (columna hora_client_id trae TODOS los
servicios de ese día para esa persona) — no hace falta agrupar entre filas, cada fila se
procesa de forma independiente. Por eso no hace falta guardar eventos crudos en memoria:
se streamea línea por línea y solo se acumulan contadores agregados por par de servicio.

CHECKPOINTING: después de cada inner zip procesado se guarda el estado completo
(checkpoints_full/estado_acumulador.pkl). Si el proceso se cae o se reinicia, retoma
desde ahí sin reprocesar.Seguro para dejar corriendo desatendido.

Requiere: pip install polars tqdm psutil

AJUSTAR ANTES DE CORRER: ZIP_PATHS y DIM_PATH a las rutas reales en la máquina AWS.
"""

import csv
import io
import pickle
import random
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import polars as pl
import psutil
from tqdm import tqdm

csv.field_size_limit(sys.maxsize)

# ── CONFIGURACIÓN — AWS ──────────────────────────────────────────────────────
ZIP_PATHS = sorted(Path("/home/ubuntu/codigos/python/data").glob("cu_user_client-2026_*.zip"))
DIM_PATH = "/home/ubuntu/codigos/python/data/dim_integracion_cu.csv"  # <- CONFIRMAR: asumido junto a los zips, ajustar si está en otro lado
OUTPUT_DIR = Path("output_full"); OUTPUT_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR = Path("checkpoints_full"); CHECKPOINT_DIR.mkdir(exist_ok=True)
ESTADO_PATH = CHECKPOINT_DIR / "estado_acumulador.pkl"

MIN_SOPORTE = 50             # subir respecto al prototipo (5) al tener ~500x más datos
UMBRAL_CONSISTENTE = 0.9
RESERVOIR_SIZE = 300          # muestras por par direccional para percentiles aproximados de lag

proc = psutil.Process()
def ram_gb() -> float:
    return proc.memory_info().rss / 1024**3


# ── ESTADO / CHECKPOINT ──────────────────────────────────────────────────────

def estado_nuevo() -> dict:
    return {
        "procesados": set(),                    # {(outer_zip_name, inner_name), ...}
        "pair_counts": defaultdict(int),         # (a,b) -> n veces a antes de b, misma persona-día
        "pair_lag_sum": defaultdict(float),
        "pair_lag_sumsq": defaultdict(float),
        "pair_lag_sample": defaultdict(list),    # reservorio de lags en minutos
    }


def cargar_estado() -> dict:
    if ESTADO_PATH.exists():
        with open(ESTADO_PATH, "rb") as f:
            return pickle.load(f)
    return estado_nuevo()


def guardar_estado(estado: dict) -> None:
    tmp = ESTADO_PATH.with_suffix(".pkl.tmp")
    with open(tmp, "wb") as f:
        pickle.dump(estado, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(ESTADO_PATH)


# ── PARSEO ────────────────────────────────────────────────────────────────

def hhmm_a_minutos(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def parse_hora_client_id(raw: str) -> list[tuple[int, str]]:
    """'20:24->ae4c...,20:28->d602...' -> [(1224,'ae4c...'), (1228,'d602...')] ordenado por hora."""
    pares = []
    for token in raw.strip().strip('"').split(","):
        token = token.strip()
        if not token or "->" not in token:
            continue
        hora, servicio = token.split("->", 1)
        hora, servicio = hora.strip(), servicio.strip()
        if not hora or not servicio:
            continue
        try:
            pares.append((hhmm_a_minutos(hora), servicio))
        except ValueError:
            continue
    pares.sort(key=lambda x: x[0])
    return pares


def pares_de_linea(horas_raw: str):
    """Todos los pares (a antes que b, lag_minutos) dentro de una persona-día."""
    pares = parse_hora_client_id(horas_raw)
    if len(pares) < 2:
        return
    vistos = []
    for minutos, s in pares:
        for prev_min, prev_s in vistos:
            if prev_s == s:
                continue
            yield prev_s, s, minutos - prev_min
        vistos.append((minutos, s))


# ── ACUMULACIÓN ──────────────────────────────────────────────────────────

def procesar_inner_csv(txt, estado: dict) -> int:
    reader = csv.reader(txt, delimiter=";")
    header = next(reader)
    assert header == ["fecha", "run_anonimizado", "rango_run", "hora_client_id"], header

    n_filas = 0
    pair_counts = estado["pair_counts"]
    pair_lag_sum = estado["pair_lag_sum"]
    pair_lag_sumsq = estado["pair_lag_sumsq"]
    pair_lag_sample = estado["pair_lag_sample"]

    for row in reader:
        if len(row) != 4:
            continue
        n_filas += 1
        horas_raw = row[3]
        for a, b, lag in pares_de_linea(horas_raw):
            key = (a, b)
            pair_counts[key] += 1
            pair_lag_sum[key] += lag
            pair_lag_sumsq[key] += lag * lag
            n = pair_counts[key]
            reservorio = pair_lag_sample[key]
            if len(reservorio) < RESERVOIR_SIZE:
                reservorio.append(lag)
            else:
                j = random.randint(1, n)
                if j <= RESERVOIR_SIZE:
                    reservorio[j - 1] = lag
    return n_filas


def main():
    if not ZIP_PATHS:
        raise SystemExit("ZIP_PATHS vacío — ajustar la ruta a datos_2026/ en este archivo antes de correr.")

    estado = cargar_estado()
    print(f"Estado cargado: {len(estado['procesados']):,} inner zips ya procesados, "
          f"{len(estado['pair_counts']):,} pares direccionales acumulados, RAM {ram_gb():.2f} GB")

    for outer_path in ZIP_PATHS:
        with zipfile.ZipFile(outer_path) as outer_zip:
            inner_names = sorted(n for n in outer_zip.namelist() if n.lower().endswith(".zip"))
            for inner_name in tqdm(inner_names, desc=outer_path.name):
                clave = (outer_path.name, inner_name)
                if clave in estado["procesados"]:
                    continue
                inner_bytes = outer_zip.read(inner_name)
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    csv_name = next((n for n in inner_zip.namelist() if n.lower().endswith(".csv")), None)
                    if csv_name is not None:
                        with inner_zip.open(csv_name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                            procesar_inner_csv(txt, estado)
                estado["procesados"].add(clave)
                guardar_estado(estado)
            print(f"  ✓ {outer_path.name} completo | {len(estado['pair_counts']):,} pares | RAM {ram_gb():.2f} GB")

    consolidar(estado)


def cargar_nombres() -> dict:
    dim = pl.read_csv(DIM_PATH)
    dim = dim.with_columns((pl.col("nombre_app") + " - " + pl.col("institucion")).alias("nombre"))
    return dict(zip(dim["client_id"].to_list(), dim["nombre"].to_list()))


def consolidar(estado: dict) -> None:
    print("Consolidando resultado final...")
    pair_counts = estado["pair_counts"]
    pair_lag_sum = estado["pair_lag_sum"]
    pair_lag_sumsq = estado["pair_lag_sumsq"]
    pair_lag_sample = estado["pair_lag_sample"]

    # unificar por par NO ordenado, quedándonos con la dirección dominante
    agregados: dict[tuple[str, str], dict] = {}
    for (a, b), n in pair_counts.items():
        par = tuple(sorted((a, b)))
        entry = agregados.setdefault(par, {"a_antes_b": 0, "b_antes_a": 0})
        if (a, b) == par:
            entry["a_antes_b"] += n
        else:
            entry["b_antes_a"] += n

    filas = []
    for (svc_a, svc_b), c in agregados.items():
        soporte = c["a_antes_b"] + c["b_antes_a"]
        if soporte < MIN_SOPORTE:
            continue
        dominante_n = max(c["a_antes_b"], c["b_antes_a"])
        frac_dominante = dominante_n / soporte
        origen, destino = (svc_a, svc_b) if c["a_antes_b"] >= c["b_antes_a"] else (svc_b, svc_a)
        key_dom = (origen, destino)

        n_dom = pair_counts.get(key_dom, 0)
        suma = pair_lag_sum.get(key_dom, 0.0)
        sumsq = pair_lag_sumsq.get(key_dom, 0.0)
        media = suma / n_dom if n_dom else None
        varianza = (sumsq / n_dom - media ** 2) if n_dom and media is not None else None
        desvest = varianza ** 0.5 if varianza and varianza > 0 else 0.0

        muestra = sorted(pair_lag_sample.get(key_dom, []))

        def percentil(p, m=muestra):
            if not m:
                return None
            idx = min(int(p * len(m)), len(m) - 1)
            return round(m[idx], 2)

        filas.append({
            "servicio_origen": origen,
            "servicio_destino": destino,
            "soporte_co_ocurrencia": soporte,
            "n_origen_antes_destino": n_dom,
            "frac_direccion_dominante": round(frac_dominante, 4),
            "clasificacion": "consistente" if frac_dominante >= UMBRAL_CONSISTENTE else "mixto",
            "lag_min_media": round(media, 2) if media is not None else None,
            "lag_min_desvest": round(desvest, 2) if desvest is not None else None,
            "lag_min_p25": percentil(0.25),
            "lag_min_mediana": percentil(0.50),
            "lag_min_p75": percentil(0.75),
            "lag_min_p95": percentil(0.95),
        })

    df = pl.DataFrame(filas).sort("soporte_co_ocurrencia", descending=True)

    nombres = cargar_nombres()
    df = df.with_columns([
        pl.Series("nombre_origen", [nombres.get(s, s) for s in df["servicio_origen"].to_list()]),
        pl.Series("nombre_destino", [nombres.get(s, s) for s in df["servicio_destino"].to_list()]),
    ])

    df.write_parquet(OUTPUT_DIR / "pares_orden_lags_full.parquet")
    df.write_csv(OUTPUT_DIR / "pares_orden_lags_full.csv")

    print(f"  {df.height:,} pares con soporte >= {MIN_SOPORTE}")
    print(f"  consistentes: {(df['clasificacion'] == 'consistente').sum():,}")
    print(f"  mixtos:       {(df['clasificacion'] == 'mixto').sum():,}")
    print(f"  guardado en {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
