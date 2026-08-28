import csv
from collections import defaultdict

# Cargar diccionario de mapeo
def load_client_names_mapping(filename):
    mapping = {}
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row['nombre_app'].strip()
            inst = row['institucion'].strip()
            client_id = row['client_id'].strip()
            mapping[client_id] = f"{name} de {inst}"
    return mapping

# Procesar todos los archivos mensuales grandes
def process_large_transaction_files(filenames):
    user_to_client_ids = defaultdict(set)

    for fname in filenames:
        print(f"Procesando archivo: {fname}")
        with open(fname, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                run = row['run_anonimizado']
                client_ids = row['client_id_traza'].split(',')
                user_to_client_ids[run].update(client_ids)

    return list(user_to_client_ids.values())  # transacciones = lista de sets

# Aplicar ECLAT (como ya lo tienes)
class Eclat:
    def __init__(self, min_support):
        self.min_support = min_support
        self.freq_sets = defaultdict(int)

    def _get_frequent_1_itemsets(self, transactions):
        itemsets = defaultdict(int)
        for transaction in transactions:
            for item in transaction:
                itemsets[item] += 1
        return {frozenset([item]): support for item, support in itemsets.items() if support >= self.min_support}

    def _join_sets(self, itemsets, length):
        return [i.union(j) for i in itemsets for j in itemsets if len(i.union(j)) == length]

    def _get_support(self, itemset, transactions):
        return sum(1 for transaction in transactions if itemset.issubset(transaction))

    def _get_frequent_itemsets(self, transactions):
        itemsets = self._get_frequent_1_itemsets(transactions)
        all_itemsets = {}
        k = 2
        while itemsets:
            all_itemsets[k - 1] = itemsets
            candidates = self._join_sets(itemsets.keys(), k)
            itemsets = {}
            for candidate in candidates:
                support = self._get_support(candidate, transactions)
                if support >= self.min_support:
                    itemsets[candidate] = support
            k += 1
        return all_itemsets

    def fit(self, transactions):
        self.freq_sets = self._get_frequent_itemsets(transactions)

    def get_frequent_itemsets(self):
        return self.freq_sets

# Mostrar resultados traduciendo IDs
def print_frequent_itemsets_with_names(freq_itemsets, id_name_mapping):
    print("Conjuntos frecuentes:")
    for k, itemsets in freq_itemsets.items():
        print(f"Conjuntos de {k}-elementos:")
        for itemset, support in itemsets.items():
            named_items = []
            for item in itemset:
                named_items.append(id_name_mapping.get(item, f"{item} de Desconocido"))
            print(f"{frozenset(named_items)}: {support}")

# Parámetros y ejecución
min_support = 50  # ajusta según tus necesidades

# Ruta a archivos mensuales
monthly_files = [
    '2024-01.csv',
    '2024-02.csv',
    '2024-03.csv',
    # ... hasta diciembre
]

# Diccionario de nombres (client_id → nombre_app + institucion)
mapping_file = 'client_id_mapping.csv'
id_name_mapping = load_client_names_mapping(mapping_file)

# Procesar transacciones anuales
transactions = process_large_transaction_files(monthly_files)

# Ejecutar ECLAT
eclat = Eclat(min_support)
eclat.fit(transactions)
frequent_itemsets = eclat.get_frequent_itemsets()

# Mostrar resultados
print_frequent_itemsets_with_names(frequent_itemsets, id_name_mapping)
