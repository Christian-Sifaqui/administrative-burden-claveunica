# este script implementa el algoritmo ECLAT para encontrar conjuntos frecuentes
# en un conjunto de datos de transacciones, utilizando un formato vertical.
# El script también incluye la carga de un mapeo de IDs a nombres de aplicaciones e instituciones
# y la impresión de los resultados con nombres legibles.
# Requiere Python 3.6+ y las librerías estándar.
# Asegúrate de tener los archivos necesarios en el mismo directorio:
# - 'dim_integracion_cu.csv' (mapeo de client_id a nombre_app e institucion)
# - 'cu_user_client_id.csv' (archivo de transacciones con run_anonimizado y client_id_traza)
# Requiere las librerías estándar de Python:
# `csv`, `collections`, `defaultdict`, y `tqdm` para mostrar el progreso.

import csv
from collections import defaultdict
from tqdm import tqdm

# ------------------------
# Cargar diccionario de mapeo client_id → nombre_app + institucion
# ------------------------
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

# ------------------------
# Procesar archivos de transacciones
# ------------------------
def process_large_transaction_files(filenames):
    user_to_client_ids = defaultdict(set)
    for fname in filenames:
        print(f"Procesando archivo: {fname}")
        with open(fname, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            count = 0
            for row in reader:
                run = row['run_anonimizado']
                client_ids = row['client_id_traza'].split(',')
                user_to_client_ids[run].update(client_id.strip() for client_id in client_ids)
                count += 1
                if count % 10000 == 0:
                    print(f"  Procesadas {count} filas...")
            print(f"  Total filas procesadas en {fname}: {count}")
    return list(user_to_client_ids.values())  # Lista de sets (transacciones)

# ------------------------
# Algoritmo ECLAT optimizado (formato vertical)
# ------------------------
class Eclat:
    def __init__(self, min_support):
        self.min_support = min_support
        self.freq_sets = defaultdict(dict)  # {k: {itemset: soporte}}

    def _build_vertical_format(self, transactions):
        vertical = defaultdict(set)
        for tid, transaction in enumerate(transactions):
            for item in transaction:
                vertical[item].add(tid)
        return vertical

    def fit(self, transactions):
        print("Transformando datos a formato vertical...")
        vertical_db = self._build_vertical_format(transactions)

        print("Buscando itemsets frecuentes (ECLAT)...")
        # Itemsets frecuentes de 1 elemento
        freq_itemsets = {
            frozenset([item]): tids
            for item, tids in vertical_db.items()
            if len(tids) >= self.min_support
        }

        # Guardar los itemsets de tamaño 1
        self.freq_sets[1] = {fs: len(tids) for fs, tids in freq_itemsets.items()}

        # Iniciar recursión con k = 2
        self._eclat_recursive(freq_itemsets, k=2)

    def _eclat_recursive(self, prefix_dict, k):
        items = list(prefix_dict.items())
        candidates = {}

        for i in range(len(items)):
            base_itemset, base_tids = items[i]
            for j in range(i + 1, len(items)):
                new_itemset = base_itemset.union(items[j][0])
                new_tids = base_tids & items[j][1]
                if len(new_tids) >= self.min_support:
                    candidates[new_itemset] = new_tids

        if candidates:
            print(f"Se generaron {len(candidates)} candidatos de tamaño {k}")
            self.freq_sets[k] = {fs: len(tids) for fs, tids in candidates.items()}
            self._eclat_recursive(candidates, k + 1)

    def get_frequent_itemsets(self):
        return self.freq_sets

# ------------------------
# Mostrar resultados traduciendo IDs
# ------------------------
def print_frequent_itemsets_with_names(freq_itemsets, id_name_mapping):
    print("\nConjuntos frecuentes:")
    for k in sorted(freq_itemsets):
        print(f"\nConjuntos de {k}-elementos:")
        for itemset, support in freq_itemsets[k].items():
            named_items = [
                id_name_mapping.get(item, f"{item} de Desconocido") for item in itemset
            ]
            print(f"{frozenset(named_items)}: {support}")

# ------------------------
# Parámetros y ejecución
# ------------------------
if __name__ == "__main__":
    min_support = 50  # ajusta según tu volumen

    # Archivos a procesar
    monthly_files = ['cu_user_client_id_2024-05-14.csv']
    mapping_file = 'dim_integracion_cu.csv'

    # Ejecutar pipeline
    id_name_mapping = load_client_names_mapping(mapping_file)
    transactions = process_large_transaction_files(monthly_files)

    print(f"\nTotal de transacciones cargadas: {len(transactions)}")

    eclat = Eclat(min_support)
    eclat.fit(transactions)

    frequent_itemsets = eclat.get_frequent_itemsets()
    print_frequent_itemsets_with_names(frequent_itemsets, id_name_mapping)
