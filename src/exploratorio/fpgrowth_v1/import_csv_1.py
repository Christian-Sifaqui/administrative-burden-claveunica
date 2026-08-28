import csv
import os

class Eclat:
    def __init__(self, min_support):
        self.min_support = min_support
        self.freq_sets = {}

    def _get_frequent_1_itemsets(self, transactions):
        itemsets = {}
        for transaction in transactions:
            for item in transaction:
                itemsets[item] = itemsets.get(item, 0) + 1
        return {frozenset([item]): support for item, support in itemsets.items() if support >= self.min_support}

    def _join_sets(self, itemsets, length):
        return [i.union(j) for i in itemsets for j in itemsets if len(i.union(j)) == length]

    def _get_support(self, itemset, transactions):
        count = 0
        for transaction in transactions:
            if itemset.issubset(transaction):
                count += 1
        return count

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

# Función para cargar transacciones desde un archivo CSV
def load_transactions_from_csv(file_path):
    transactions = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            transactions.append(set(row))
    return transactions

# Ruta del archivo CSV en tu PC
csv_file_path = r'C:\Users\nacho\Downloads\copia_pruebas_conjuntos.csv'

# Ejemplo de uso
min_support = 3

transactions = load_transactions_from_csv(csv_file_path)

eclat = Eclat(min_support)
eclat.fit(transactions)
frequent_itemsets = eclat.get_frequent_itemsets()

# Guardar resultados en el escritorio
desktop_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
output_file_path = os.path.join(desktop_path, 'frequent_itemsets.csv')

with open(output_file_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for k, itemsets in frequent_itemsets.items():
        writer.writerow([f"Conjuntos de {k}-elementos:"])
        for itemset, support in itemsets.items():
            writer.writerow([f"{itemset}: {support}"])
