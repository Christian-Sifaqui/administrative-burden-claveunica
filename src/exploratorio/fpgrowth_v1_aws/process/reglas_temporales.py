import io, os, zipfile, tempfile
from collections import defaultdict
from pathlib import Path
import polars as pl
from tqdm import tqdm
import unicodedata

# --- CONFIG ---
ZIP_PATHS = ["../data/cu_user_client-2024.zip", "../data/cu_user_client-2025.zip"]
MESES = ["2024-01", "2024-02", "2024-03", "2024-04"]
DIM_PATH = "../data/dim_integracion_cu.csv"
OUTPUT_DIR = Path("output_reglas"); OUTPUT_DIR.mkdir(exist_ok=True)

MIN_SOPORTE = 200
LIFT_MIN = 2.0

def map_rango(r):
    return "A" if 22 <= r <= 23 else "B" if 20 <= r < 22 else "C" if 17 <= r < 20 else "D" if 14 <= r < 17 else "E" if 10 <= r < 14 else "F"

def limpiar(t):
    return unicodedata.normalize('NFKD', t).encode('ASCII','ignore').decode().lower()

# --- 1. Cargar dimensión ---
dim = pl.read_csv(DIM_PATH, infer_schema_length=10000)
dim = dim.with_columns(
    (pl.col("nombre_app") + " - " + pl.col("institucion")).alias("nombre")
).select(["client_id", "nombre"])

# --- 2. Leer zips y construir historiales por mes ---
historiales = {m: {} for m in MESES}
serv_id, next_id = {}, 0

for zp in ZIP_PATHS:
    with zipfile.ZipFile(zp) as z:
        inners = [n for n in z.namelist() if n.endswith(".zip")]
        for inner in tqdm(inners, desc=zp):
            with z.open(inner) as src:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    tmp.write(src.read()); p = tmp.name
            try:
                with zipfile.ZipFile(p) as iz:
                    for csv in [n for n in iz.namelist() if n.endswith(".csv")]:
                        with iz.open(csv) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                            next(txt, None)
                            for line in txt:
                                try:
                                    fe, pe, ra, se = line.strip().split(";", 3)
                                    mes = fe[:7]
                                    if mes not in MESES: continue
                                    ra = float(ra)
                                    servs = [s.strip() for s in se.replace('"','').split(",") if s.strip()]
                                    if not servs: continue
                                    g = map_rango(ra)
                                    ids = []
                                    for s in servs:
                                        if s not in serv_id:
                                            serv_id[s] = next_id; next_id += 1
                                        ids.append(serv_id[s])
                                    h = historiales[mes]
                                    if pe not in h:
                                        h[pe] = {"grupo": g, "servicios": set()}
                                    h[pe]["servicios"].update(ids[:100])
                                except:
                                    pass
            finally:
                os.unlink(p)

id_serv = {v: k for k, v in serv_id.items()}

# --- 3. Función para reglas temporales ---
def calcular_reglas(mes_a, mes_b):
    ha = historiales[mes_a]
    hb = historiales[mes_b]
    personas = set(ha.keys()) & set(hb.keys())

    conteo_a = defaultdict(lambda: defaultdict(int))
    trans = defaultdict(lambda: defaultdict(int))
    total_b = defaultdict(lambda: defaultdict(int))
    n_por_grupo = defaultdict(int)

    for p in personas:
        ga = ha[p]["grupo"]
        n_por_grupo[ga] += 1
        sa = ha[p]["servicios"]
        sb = hb[p]["servicios"]
        for a in sa:
            conteo_a[ga][a] += 1
            for b in sb:
                if a!= b:
                    trans[ga][(a, b)] += 1
        for b in sb:
            total_b[ga][b] += 1

    filas = []
    for g, pares in trans.items():
        for (a, b), nab in pares.items():
            if nab < MIN_SOPORTE: continue
            conf = nab / conteo_a[g][a]
            p_b = total_b[g][b] / n_por_grupo[g]
            lift = conf / p_b if p_b > 0 else 0
            if lift >= LIFT_MIN:
                filas.append({
                    "grupo": g,
                    "antecedente_id": id_serv[a],
                    "consecuente_id": id_serv[b],
                    "soporte": nab,
                    "confianza": round(conf, 4),
                    "lift_temporal": round(lift, 3),
                    "mes_a": mes_a,
                    "mes_b": mes_b
                })
    return pl.DataFrame(filas)
# --- 4. Calcular reglas para TODAS las combinaciones de meses (A < B) ---
from itertools import combinations

dfs = []
for mes_a, mes_b in combinations(MESES, 2):  # Genera todos los pares con mes_a < mes_b
    print(f"Calculando reglas: {mes_a} → {mes_b}")
    df_temp = calcular_reglas(mes_a, mes_b)
    dfs.append(df_temp)

df = pl.concat(dfs)

# --- 5. Traducir IDs a nombres (igual que antes) ---
df = df.join(dim, left_on="antecedente_id", right_on="client_id", how="left") \
      .rename({"nombre": "antecedente"})
df = df.join(dim, left_on="consecuente_id", right_on="client_id", how="left") \
      .rename({"nombre": "consecuente"})

df = df.with_columns([
    pl.col("antecedente").fill_null(pl.col("antecedente_id")),
    pl.col("consecuente").fill_null(pl.col("consecuente_id"))
])

# --- 6. Guardar y mostrar top (ahora con más flexibilidad) ---
df.write_parquet(OUTPUT_DIR / "reglas_temporales.parquet")

# Mostrar top 10 por grupo, pero puedes elegir filtrar por meses específicos
for g in ["A","B","C","D","E","F"]:
    out = df.filter(pl.col("grupo")==g).sort("lift_temporal", descending=True).head(10)
    print(f"\n=== GRUPO {g} - TOP 10 reglas (todos los períodos) ===")
    print(out.select(["antecedente","consecuente","soporte","confianza","lift_temporal","mes_a","mes_b"]))

# Opcional: Mostrar estadísticas de cuántas reglas por par de meses
print("\n=== Resumen de reglas por par de meses ===")
resumen = df.group_by(["mes_a", "mes_b"]).agg(pl.len().alias("num_reglas"))
print(resumen.sort(["mes_a", "mes_b"]))
