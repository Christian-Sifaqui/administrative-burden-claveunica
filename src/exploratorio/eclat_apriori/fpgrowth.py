# este script implementa el algoritmo FP-Growth para encontrar conjuntos frecuentes
# en un conjunto de datos de transacciones, utilizando un formato binario.
# El script también incluye la carga de un mapeo de IDs a nombres de aplicaciones e instituciones
# y la impresión de los resultados con nombres legibles.
# Requiere Python 3.6+ y las librerías estándar.
# Asegúrate de tener los archivos necesarios en el mismo directorio:
# - 'dim_integracion_cu.csv' (mapeo de client_id a nombre_app e institucion)
# - 'cu_user_client_id.csv' (archivo de transacciones con run_anonimizado y client_id_traza)
# Requiere las librerías estándar de Python:
# `csv`, `collections`, `defaultdict`, y `tqdm` para mostrar el progreso.

import pandas as pd
import csv
from tqdm import tqdm
from collections import defaultdict
import psutil
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.preprocessing import TransactionEncoder

# Mostrar uso actual de RAM
def print_memory_usage():
    mem = psutil.virtual_memory()
    used_gb = (mem.total - mem.available) / (1024 ** 3)
    print(f"RAM usada: {used_gb:.2f} GB de {mem.total / (1024**3):.2f} GB")

# Cargar mapeo de nombres de aplicaciones
def load_client_names_mapping(filename):
    mapping = {}
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row['nombre_app'].strip()
            inst = row['institucion'].strip()
            client_id = row['client_id'].strip()
            mapping[client_id] = f"{name} --> {inst}"
    return mapping

# Leer datos por bloques y armar transacciones
def load_transactions_chunked(filename, max_rows=None):
    transactions = []
    seen = defaultdict(set)
    chunk_size = 500_000
    total_rows = sum(1 for _ in open(filename)) - 1  # sin encabezado
    with tqdm(total=min(max_rows, total_rows) if max_rows else total_rows, desc="Leyendo archivo") as pbar:
        for chunk in pd.read_csv(filename, sep=';', chunksize=chunk_size):
            for _, row in chunk.iterrows():
                run = row['run_anonimizado']
                client_ids = str(row['client_id_traza']).split(',')
                seen[run].update(client_ids)
            pbar.update(chunk.shape[0])
            print_memory_usage()
            if max_rows and pbar.n >= max_rows:
                break
    transactions = list(seen.values())
    return transactions

# Aplicar FP-Growth
def run_fp_growth(transactions, min_support):
    print("Transformando transacciones a formato binario...")
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df = pd.DataFrame(te_ary, columns=te.columns_)
    print_memory_usage()
    print("Ejecutando FP-Growth...")
    freq_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True)
    print_memory_usage()
    return freq_itemsets

# Mostrar y guardar resultados
def guardar_resultados(freq_itemsets, mapping, output_file):
    freq_itemsets['translated_items'] = freq_itemsets['itemsets'].apply(
        lambda x: [mapping.get(i, f"{i} de Desconocido") for i in x]
    )
    freq_itemsets.sort_values(by='support', ascending=False).to_csv(output_file, index=False)
    print(f"Resultados guardados en {output_file}")

# Parámetros
min_support_fraction = 0.001  # ej. 0.001 = 0.1%
max_rows = 10_000_000  # limitar por ahora para evitar crash
archivo_transacciones = 'cu_user_client_id_2024-05-14.csv'
archivo_mapeo = 'dim_integracion_cu.csv'
salida_resultados = 'resultados_fp_growth.csv'

# Ejecutar pipeline
print("Cargando mapeo de nombres...")
id_name_mapping = load_client_names_mapping(archivo_mapeo)

print("Procesando archivo de transacciones...")
transactions = load_transactions_chunked(archivo_transacciones, max_rows=max_rows)

print("Total de transacciones cargadas:", len(transactions))

frequent_itemsets = run_fp_growth(transactions, min_support=min_support_fraction)

guardar_resultados(frequent_itemsets, id_name_mapping, salida_resultados)
