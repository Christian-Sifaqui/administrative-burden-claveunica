import csv
import os
import tempfile
import json
from collections import defaultdict
from tqdm import tqdm
import psutil

# ========================
# Configuración
# ========================
CHUNK_SIZE = 1_000_000  # filas por bloque
MIN_SUPPORT = 50
MAIN_FILE = 'cu_user_client_id_2024-05-14.csv'
MAPPING_FILE = 'dim_integracion_cu.csv'

# ========================
# Utilidades
# ========================

def report_memory_usage(tag=''):
    usage = psutil.virtual_memory()
    used_gb = (usage.total - usage.available) / (1024**3)
    print(f"[{tag}] Uso de memoria: {used_gb:.2f} GB de {usage.total / (1024**3):.2f} GB totales")

# Cargar diccionario de mapeo client_id → nombre
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

# Procesar archivo CSV en bloques y guardar transacciones en archivos temporales
def process_large_file_in_chunks(filename, chunk_size=CHUNK_SIZE):
    temp_files = []
    user_to_client_ids = defaultdict(set)
    counter = 0

    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in tqdm(reader, desc="Leyendo filas"):
            run = row['run_anonimizado']
            client_ids = row['client_id_traza'].split(',')
            user_to_client_ids[run].update(client_ids)
            counter += 1

            if counter % chunk_size == 0:
                temp_path = save_chunk(user_to_client_ids)
                temp_files.append(temp_path)
                user_to_client_ids.clear()
                print(f"Bloque de {chunk_size} filas procesado y guardado en {temp_path}")
                report_memory_usage(f"{counter} filas")

    if user_to_client_ids:
        temp_path = save_chunk(user_to_client_ids)
        temp_files.append(temp_path)
        print(f"Último bloque guardado en {temp_path}")
        report_memory_usage("Último bloque")

    return temp_files

def save_chunk(user_dict):
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json')
    json.dump([list(ids) for ids in user_dict.values()], temp_file)
    temp_file.close()
    return temp_file.name

def load_all_transactions(temp_files):
    all_transactions = []
    for path in temp_files:
        with open(path, 'r') as f:
            chunk = json.load(f)
            all_transactions.extend([set(t) for t in chunk])
    return all_transactions

# ========================
# Algoritmo ECLAT
# ========================

class Eclat:
    def __init__(self, min_support):
        self.min_support = min_support
        self.freq_sets = defaultdict(dict)

    def fit(self, transactions):
        item_tidset = defaultdict(set)
        for tid, transaction in enumerate(transactions):
            for item in transaction:
                item_tidset[frozenset([item])].add(tid)

        self.freq_sets[1] = {item: len(tids) for item, tids in item_tidset.items() if len(tids) >= self.min_support}
        k = 2
        current = {item: tids for item, tids in item_tidset.items() if len(tids) >= self.min_support}

        while current:
            print(f"\nGenerando candidatos de tamaño {k}...")
            next_candidates = {}
            items = list(current.items())
            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    itemset1, tids1 = items[i]
                    itemset2, tids2 = items[j]
                    union = itemset1 | itemset2
                    if len(union) == k:
                        tids_union = tids1 & tids2
                        if len(tids_union) >= self.min_support:
                            next_candidates[union] = tids_union
            self.freq_sets[k] = {fs: len(tids) for fs, tids in next_candidates.items()}
            current = next_candidates
            k += 1
            report_memory_usage(f"Nivel {k-1}")

    def get_frequent_itemsets(self):
        return self.freq_sets

# ========================
# Resultados
# ========================

def print_frequent_itemsets_with_names(freq_itemsets, id_name_mapping):
    print("\n==========================")
    print("Conjuntos frecuentes:")
    print("==========================")
    for k, itemsets in freq_itemsets.items():
        print(f"\nConjuntos de {k}-elementos:")
        for itemset, support in itemsets.items():
            named_items = [id_name_mapping.get(id, f"{id} de Desconocido") for id in itemset]
            print(f"{frozenset(named_items)}: {support}")

# ========================
# Ejecución principal
# ========================

def main():
    report_memory_usage("Inicio")
    id_name_mapping = load_client_names_mapping(MAPPING_FILE)

    temp_files = process_large_file_in_chunks(MAIN_FILE)
    transactions = load_all_transactions(temp_files)

    report_memory_usage("Antes de ECLAT")
    eclat = Eclat(MIN_SUPPORT)
    eclat.fit(transactions)

    frequent_itemsets = eclat.get_frequent_itemsets()
    print_frequent_itemsets_with_names(frequent_itemsets, id_name_mapping)

    # Limpieza opcional de archivos temporales
    for path in temp_files:
        os.remove(path)
        print(f"Archivo temporal eliminado: {path}")

    report_memory_usage("Fin")

if __name__ == "__main__":
    main()
