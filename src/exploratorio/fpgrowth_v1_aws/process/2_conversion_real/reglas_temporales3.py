import io, os, zipfile, tempfile, pickle, time, gc
from collections import defaultdict
from pathlib import Path
import polars as pl
from tqdm import tqdm
import unicodedata
from itertools import combinations
import psutil

# VERSION MEJORADA: validacion checkpoints, progreso pares, ETA, RAM, autoregeneracion checkpoints corruptos
ZIP_PATHS = ['../../data/cu_user_client-2024.zip']
MESES = ['2024-01','2024-02','2024-03','2024-04','2024-05','2024-06','2024-07','2024-08','2024-09','2024-10','2024-11','2024-12']
DIM_PATH = '../../data/dim_integracion_cu.csv'
OUTPUT_DIR = Path('output_reglas_v3'); OUTPUT_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR = Path('checkpoints_historiales_v3'); CHECKPOINT_DIR.mkdir(exist_ok=True)
MIN_SOPORTE=200
LIFT_MIN=2.0
MAX_SERV=100

proc = psutil.Process()
def ram_gb(): return proc.memory_info().rss/1024**3

def map_rango(r):
    return 'A' if 22 <= r <= 23 else 'B' if 20 <= r < 22 else 'C' if 17 <= r < 20 else 'D' if 14 <= r < 17 else 'E' if 10 <= r < 14 else 'F'

def cargar_dim():
    dim = pl.read_csv(DIM_PATH, infer_schema_length=10000)
    return dim.with_columns((pl.col('nombre_app') + ' - ' + pl.col('institucion')).alias('nombre')).select(['client_id','nombre'])

def guardar_historial(mes,historial,serv_id):
    fp = CHECKPOINT_DIR / f'historial_{mes}.pkl'
    serial = {u:{'grupo':v['grupo'],'servicios':list(v['servicios'])} for u,v in historial.items()}
    with open(fp,'wb') as f:
        pickle.dump({'historial':serial,'serv_id':serv_id}, f, protocol=pickle.HIGHEST_PROTOCOL)

def cargar_historial(mes):
    fp = CHECKPOINT_DIR / f'historial_{mes}.pkl'
    if not fp.exists(): return None,None
    try:
        with open(fp,'rb') as f: data = pickle.load(f)
        hist = {u:{'grupo':v['grupo'],'servicios':set(v['servicios'])} for u,v in data['historial'].items()}
        return hist, data['serv_id']
    except Exception:
        try: fp.unlink()
        except: pass
        return None,None

def procesar_mes(mes):
    h,s = cargar_historial(mes)
    if h is not None:
        print(f'✓ {mes} ya disponible')
        return
    print(f'Procesando {mes} ...')
    historial, serv_id, next_id = {}, {}, 0
    for zp in ZIP_PATHS:
        with zipfile.ZipFile(zp) as z:
            inners = [n for n in z.namelist() if n.endswith('.zip')]
            for inner in tqdm(inners, desc=mes):
                with z.open(inner) as src:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp.write(src.read()); p=tmp.name
                try:
                    with zipfile.ZipFile(p) as iz:
                        for csv in [n for n in iz.namelist() if n.endswith('.csv')]:
                            with iz.open(csv) as f:
                                txt = io.TextIOWrapper(f, encoding='utf-8', errors='ignore')
                                next(txt,None)
                                for line in txt:
                                    try:
                                        fe,pe,ra,se = line.strip().split(';',3)
                                        if fe[:7] != mes: continue
                                        g = map_rango(float(ra))
                                        servs = [x.strip() for x in se.replace('"','').split(',') if x.strip()][:MAX_SERV]
                                        if not servs: continue
                                        ids=[]
                                        for x in servs:
                                            if x not in serv_id:
                                                serv_id[x]=next_id; next_id+=1
                                            ids.append(serv_id[x])
                                        if pe not in historial:
                                            historial[pe]={'grupo':g,'servicios':set()}
                                        historial[pe]['servicios'].update(ids)
                                    except: pass
                finally:
                    os.unlink(p)
    guardar_historial(mes,historial,serv_id)
    print(f'✓ {mes} guardado ({len(historial):,} usuarios) RAM {ram_gb():.2f} GB')
    del historial, serv_id; gc.collect()

def calcular_par(mes_a, mes_b):
    out = OUTPUT_DIR / f'reglas_{mes_a}_{mes_b}.parquet'
    if out.exists(): return True
    ha,sa = cargar_historial(mes_a); hb,sb = cargar_historial(mes_b)
    if ha is None or hb is None: return False
    personas = set(ha.keys()) & set(hb.keys())
    conteo_a=defaultdict(lambda:defaultdict(int)); trans=defaultdict(lambda:defaultdict(int)); total_b=defaultdict(lambda:defaultdict(int)); n=defaultdict(int)
    for p in personas:
        g=ha[p]['grupo']; sa_set=ha[p]['servicios']; sb_set=hb[p]['servicios']; n[g]+=1
        for a in sa_set:
            conteo_a[g][a]+=1
            for b in sb_set:
                if a!=b: trans[g][(a,b)]+=1
        for b in sb_set: total_b[g][b]+=1
    id_serv={v:k for d in (sa,sb) for k,v in d.items()}
    filas=[]
    for g,pares in trans.items():
        if n[g]==0: continue
        for (a,b),nab in pares.items():
            if nab < MIN_SOPORTE: continue
            conf = nab / conteo_a[g][a]
            pb = total_b[g][b] / n[g]
            lift = conf / pb if pb else 0
            if lift >= LIFT_MIN:
                filas.append({'grupo':g,'antecedente_id':id_serv.get(a,str(a)),'consecuente_id':id_serv.get(b,str(b)),'soporte':nab,'confianza':round(conf,4),'lift_temporal':round(lift,3),'mes_a':mes_a,'mes_b':mes_b})
    if filas: pl.DataFrame(filas).write_parquet(out)
    del ha,hb,sa,sb; gc.collect()
    return True

def main():
    inicio=time.time()
    print('Validando checkpoints...')
    for mes in MESES: procesar_mes(mes)
    pares=list(combinations(MESES,2))
    total=len(pares)
    for i,(a,b) in enumerate(pares, start=1):
        t0=time.time()
        ok=calcular_par(a,b)
        dt=time.time()-t0
        prom=(time.time()-inicio)/i
        eta=(total-i)*prom/60
        print(f'[{i}/{total}] {a}->{b} | {dt:.1f}s | RAM {ram_gb():.2f} GB | ETA {eta:.1f} min')
    archivos=list(OUTPUT_DIR.glob('reglas_*.parquet'))
    if archivos:
        dim=cargar_dim()
        df=pl.concat([pl.read_parquet(x) for x in archivos])
        df=df.join(dim,left_on='antecedente_id',right_on='client_id',how='left').rename({'nombre':'antecedente'})
        df=df.join(dim,left_on='consecuente_id',right_on='client_id',how='left').rename({'nombre':'consecuente'})
        df.write_parquet(OUTPUT_DIR/'reglas_temporales_completo.parquet')
        print('✓ Consolidado final listo')

if __name__ == '__main__':
    main()

