import io, os, zipfile, tempfile, pickle
from collections import defaultdict
from pathlib import Path
import polars as pl
from tqdm import tqdm
import unicodedata
import gc
from itertools import combinations

# --- CONFIG ---
ZIP_PATHS = ["../data/cu_user_client-2024.zip"]
MESES = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06", 
         "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"]
DIM_PATH = "../data/dim_integracion_cu.csv"
OUTPUT_DIR = Path("output_reglas"); OUTPUT_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR = Path("checkpoints_historiales"); CHECKPOINT_DIR.mkdir(exist_ok=True)

MIN_SOPORTE = 200
LIFT_MIN = 2.0

def map_rango(r):
    return "A" if 22 <= r <= 23 else "B" if 20 <= r < 22 else "C" if 17 <= r < 20 else "D" if 14 <= r < 17 else "E" if 10 <= r < 14 else "F"

def limpiar(t):
    return unicodedata.normalize('NFKD', t).encode('ASCII','ignore').decode().lower()

# --- 1. Cargar dimensión (pequeño, cabe en RAM) ---
dim = pl.read_csv(DIM_PATH, infer_schema_length=10000)
dim = dim.with_columns(
    (pl.col("nombre_app") + " - " + pl.col("institucion")).alias("nombre")
).select(["client_id", "nombre"])

# --- 2. Función para guardar/cargar historial de un mes ---
def guardar_historial(mes, historial, serv_id):
    """Guarda el historial de un mes en disco"""
    historial_serializable = {}
    for usuario, data in historial.items():
        historial_serializable[usuario] = {
            "grupo": data["grupo"],
            "servicios": list(data["servicios"])
        }
    
    checkpoint_file = CHECKPOINT_DIR / f"historial_{mes}.pkl"
    with open(checkpoint_file, 'wb') as f:
        pickle.dump({
            'historial': historial_serializable,
            'serv_id': serv_id,
        }, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    print(f"  ✓ Guardado checkpoint: {checkpoint_file} ({len(historial)} usuarios)")

def cargar_historial(mes):
    """Carga el historial de un mes desde disco"""
    checkpoint_file = CHECKPOINT_DIR / f"historial_{mes}.pkl"
    
    if not checkpoint_file.exists():
        return None, None
    
    with open(checkpoint_file, 'rb') as f:
        data = pickle.load(f)
    
    historial = {}
    for usuario, info in data['historial'].items():
        historial[usuario] = {
            "grupo": info["grupo"],
            "servicios": set(info["servicios"])
        }
    
    print(f"  ✓ Cargado checkpoint: {checkpoint_file} ({len(historial)} usuarios)")
    return historial, data['serv_id']

# --- 3. Procesar un mes específico (leyendo los ZIPs una sola vez por mes) ---
def procesar_mes(mes):
    """Procesa un mes completo y lo guarda en checkpoint si no existe"""
    print(f"\n📅 Procesando mes: {mes}")
    
    # Verificar si ya existe checkpoint
    historial, serv_id = cargar_historial(mes)
    if historial is not None:
        print(f"  → Usando checkpoint existente para {mes}")
        return
    
    print(f"  → Procesando desde cero (serie: {mes})...")
    
    # Procesar desde cero
    historial = {}
    serv_id = {}
    next_id = 0
    
    for zp_path in ZIP_PATHS:
        with zipfile.ZipFile(zp_path) as z:
            inners = [n for n in z.namelist() if n.endswith(".zip")]
            for inner in tqdm(inners, desc=f"    Leyendo {Path(zp_path).name}"):
                with z.open(inner) as src:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp.write(src.read())
                        p = tmp.name
                try:
                    with zipfile.ZipFile(p) as iz:
                        for csv in [n for n in iz.namelist() if n.endswith(".csv")]:
                            with iz.open(csv) as f:
                                txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                                next(txt, None)
                                for line in txt:
                                    try:
                                        fe, pe, ra, se = line.strip().split(";", 3)
                                        if fe[:7] != mes: 
                                            continue
                                        
                                        ra = float(ra)
                                        servs = [s.strip() for s in se.replace('"','').split(",") if s.strip()]
                                        if not servs: 
                                            continue
                                        
                                        g = map_rango(ra)
                                        ids = []
                                        for s in servs:
                                            if s not in serv_id:
                                                serv_id[s] = next_id
                                                next_id += 1
                                            ids.append(serv_id[s])
                                        
                                        if pe not in historial:
                                            historial[pe] = {"grupo": g, "servicios": set()}
                                        if len(historial[pe]["servicios"]) < 100:
                                            historial[pe]["servicios"].update(ids[:100])
                                    except:
                                        pass
                finally:
                    os.unlink(p)
    
    # Guardar checkpoint
    guardar_historial(mes, historial, serv_id)
    
    # Liberar memoria inmediatamente
    del historial
    del serv_id
    gc.collect()
    
    print(f"  → Finalizado y liberado {mes}")

# --- 4. Calcular reglas entre dos meses (si no existe ya) ---
def calcular_y_guardar_reglas(mes_a, mes_b):
    """Calcula reglas entre mes_a y mes_b, guarda resultado si no existe"""
    
    archivo_salida = OUTPUT_DIR / f"reglas_{mes_a}_{mes_b}.parquet"
    
    # Verificar si ya existe el resultado
    if archivo_salida.exists():
        print(f"  ⏭️  Reglas {mes_a}→{mes_b} ya existen, saltando...")
        return None
    
    print(f"\n  🔗 Calculando reglas: {mes_a} → {mes_b}")
    
    # Cargar historiales de ambos meses
    historial_a, serv_id_a = cargar_historial(mes_a)
    historial_b, serv_id_b = cargar_historial(mes_b)
    
    if historial_a is None or historial_b is None:
        print(f"  ❌ Error: No se pudieron cargar los historiales de {mes_a} o {mes_b}")
        return None
    
    # Calcular reglas
    all_serv_ids = {}
    all_serv_ids.update(serv_id_a)
    all_serv_ids.update(serv_id_b)
    id_serv = {v: k for k, v in all_serv_ids.items()}
    
    personas = set(historial_a.keys()) & set(historial_b.keys())
    print(f"    Personas comunes: {len(personas)}")
    
    conteo_a = defaultdict(lambda: defaultdict(int))
    trans = defaultdict(lambda: defaultdict(int))
    total_b = defaultdict(lambda: defaultdict(int))
    n_por_grupo = defaultdict(int)
    
    for p in personas:
        ga = historial_a[p]["grupo"]
        n_por_grupo[ga] += 1
        sa = historial_a[p]["servicios"]
        sb = historial_b[p]["servicios"]
        
        for a in sa:
            conteo_a[ga][a] += 1
            for b in sb:
                if a != b:
                    trans[ga][(a, b)] += 1
        
        for b in sb:
            total_b[ga][b] += 1
    
    filas = []
    for g, pares in trans.items():
        n_grupo = n_por_grupo[g]
        if n_grupo == 0:
            continue
            
        for (a, b), nab in pares.items():
            if nab < MIN_SOPORTE:
                continue
            
            conf = nab / conteo_a[g][a] if conteo_a[g][a] > 0 else 0
            p_b = total_b[g][b] / n_grupo
            lift = conf / p_b if p_b > 0 else 0
            
            if lift >= LIFT_MIN:
                filas.append({
                    "grupo": g,
                    "antecedente_id": id_serv.get(a, f"ID_{a}"),
                    "consecuente_id": id_serv.get(b, f"ID_{b}"),
                    "soporte": nab,
                    "confianza": round(conf, 4),
                    "lift_temporal": round(lift, 3),
                    "mes_a": mes_a,
                    "mes_b": mes_b
                })
    
    print(f"    → Generadas {len(filas)} reglas")
    
    # Guardar resultado
    if filas:
        df = pl.DataFrame(filas)
        df.write_parquet(archivo_salida)
        print(f"    ✓ Guardado: {archivo_salida}")
        
        # Liberar memoria
        del df
    else:
        print(f"    ⚠️ No se generaron reglas para {mes_a}→{mes_b}")
    
    # Liberar memoria de los historiales
    del historial_a, historial_b, serv_id_a, serv_id_b
    gc.collect()
    
    return archivo_salida if filas else None

# --- 5. Procesamiento principal ---
def main():
    print("="*70)
    print("📊 PROCESAMIENTO POR MESES CON LIBERACIÓN DE MEMORIA")
    print("="*70)
    
    # --- PASO 1: Procesar cada mes individualmente (uno por uno, liberando memoria) ---
    print("\n" + "="*70)
    print("PASO 1: Procesando meses individualmente (checkpoints)")
    print("="*70)
    
    for mes in MESES:
        procesar_mes(mes)  # Esta función ya libera memoria internamente
        
        # Liberación adicional por si acaso
        gc.collect()
    
    # --- PASO 2: Calcular reglas de A→B para todos los pares A < B ---
    print("\n" + "="*70)
    print("PASO 2: Calculando reglas temporales (A→B con A < B)")
    print("="*70)
    
    archivos_reglas = []
    
    # Para cada mes A (desde el primero hasta el penúltimo)
    for i, mes_a in enumerate(MESES[:-1]):  # Enero a Noviembre
        print(f"\n{'='*70}")
        print(f"📌 Procesando {mes_a} como antecedente...")
        print(f"{'='*70}")
        
        # Cargar historial de A UNA SOLA VEZ (se mantiene para todos sus B)
        print(f"  Cargando mes base: {mes_a}")
        historial_a, serv_id_a = cargar_historial(mes_a)
        
        if historial_a is None:
            print(f"  ❌ Error: No se pudo cargar {mes_a}")
            continue
        
        # Para cada mes B > A (febrero, marzo, ..., diciembre)
        for mes_b in MESES[i+1:]:
            print(f"\n  --- Procesando {mes_a} → {mes_b} ---")
            
            archivo_salida = OUTPUT_DIR / f"reglas_{mes_a}_{mes_b}.parquet"
            
            # Verificar si ya existe
            if archivo_salida.exists():
                print(f"    ⏭️  Ya existe, saltando...")
                archivos_reglas.append(archivo_salida)
                continue
            
            # Cargar historial de B
            historial_b, serv_id_b = cargar_historial(mes_b)
            
            if historial_b is None:
                print(f"    ❌ No se pudo cargar {mes_b}")
                continue
            
            # Calcular reglas
            print(f"    🔗 Calculando...")
            
            all_serv_ids = {}
            all_serv_ids.update(serv_id_a)
            all_serv_ids.update(serv_id_b)
            id_serv = {v: k for k, v in all_serv_ids.items()}
            
            personas = set(historial_a.keys()) & set(historial_b.keys())
            print(f"      Personas comunes: {len(personas)}")
            
            conteo_a = defaultdict(lambda: defaultdict(int))
            trans = defaultdict(lambda: defaultdict(int))
            total_b = defaultdict(lambda: defaultdict(int))
            n_por_grupo = defaultdict(int)
            
            for p in personas:
                ga = historial_a[p]["grupo"]
                n_por_grupo[ga] += 1
                sa = historial_a[p]["servicios"]
                sb = historial_b[p]["servicios"]
                
                for a in sa:
                    conteo_a[ga][a] += 1
                    for b in sb:
                        if a != b:
                            trans[ga][(a, b)] += 1
                
                for b in sb:
                    total_b[ga][b] += 1
            
            filas = []
            for g, pares in trans.items():
                n_grupo = n_por_grupo[g]
                if n_grupo == 0:
                    continue
                    
                for (a, b), nab in pares.items():
                    if nab < MIN_SOPORTE:
                        continue
                    
                    conf = nab / conteo_a[g][a] if conteo_a[g][a] > 0 else 0
                    p_b = total_b[g][b] / n_grupo
                    lift = conf / p_b if p_b > 0 else 0
                    
                    if lift >= LIFT_MIN:
                        filas.append({
                            "grupo": g,
                            "antecedente_id": id_serv.get(a, f"ID_{a}"),
                            "consecuente_id": id_serv.get(b, f"ID_{b}"),
                            "soporte": nab,
                            "confianza": round(conf, 4),
                            "lift_temporal": round(lift, 3),
                            "mes_a": mes_a,
                            "mes_b": mes_b
                        })
            
            print(f"      → Generadas {len(filas)} reglas")
            
            # Guardar y liberar
            if filas:
                df = pl.DataFrame(filas)
                df.write_parquet(archivo_salida)
                archivos_reglas.append(archivo_salida)
                print(f"    ✓ Guardado: {archivo_salida}")
                del df
            
            # Liberar B (pero mantener A para el siguiente B)
            del historial_b, serv_id_b
            gc.collect()
        
        # Terminamos con este A, liberamos A
        print(f"\n  Liberando {mes_a} de memoria...")
        del historial_a, serv_id_a
        gc.collect()
    
    # --- PASO 3: Consolidar resultados ---
    print("\n" + "="*70)
    print("PASO 3: Consolidando resultados finales")
    print("="*70)
    
    if archivos_reglas:
        print(f"  Leyendo {len(archivos_reglas)} archivos de reglas...")
        dfs = []
        for archivo in archivos_reglas:
            if archivo.exists():
                df_temp = pl.read_parquet(archivo)
                dfs.append(df_temp)
                # Liberar después de leer? Mejor esperar a concatenar
        
        if dfs:
            df_final = pl.concat(dfs)
            
            # Traducir IDs a nombres
            print("  Traduciendo IDs a nombres...")
            df_final = df_final.join(dim, left_on="antecedente_id", right_on="client_id", how="left") \
                               .rename({"nombre": "antecedente"})
            df_final = df_final.join(dim, left_on="consecuente_id", right_on="client_id", how="left") \
                               .rename({"nombre": "consecuente"})
            
            df_final = df_final.with_columns([
                pl.col("antecedente").fill_null(pl.col("antecedente_id")),
                pl.col("consecuente").fill_null(pl.col("consecuente_id"))
            ])
            
            # Guardar resultado final
            df_final.write_parquet(OUTPUT_DIR / "reglas_temporales_completo.parquet")
            print(f"  ✓ Guardado: {OUTPUT_DIR / 'reglas_temporales_completo.parquet'}")
            
            # Mostrar top por grupo
            for g in ["A","B","C","D","E","F"]:
                out = df_final.filter(pl.col("grupo")==g).sort("lift_temporal", descending=True).head(10)
                print(f"\n=== GRUPO {g} - TOP 10 reglas ===")
                print(out.select(["antecedente","consecuente","soporte","confianza","lift_temporal","mes_a","mes_b"]))
            
            # Resumen
            resumen = df_final.group_by(["mes_a", "mes_b"]).agg(pl.len().alias("num_reglas"))
            print("\n=== Resumen de reglas por par de meses ===")
            print(resumen.sort(["mes_a", "mes_b"]))
        else:
            print("  ❌ No se encontraron datos para consolidar")
    else:
        print("  ⚠️ No se generaron reglas")
    
    print("\n" + "="*70)
    print("✅ PROCESAMIENTO COMPLETADO")
    print("="*70)

if __name__ == "__main__":
    main()
