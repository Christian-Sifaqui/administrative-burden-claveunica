import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Archivos de pares y colores por rango etario
files = {
    '1_10': 'fpgrowth_1_10_pairs.csv',
    '10_14': 'fpgrowth_10_14_pairs.csv',
    '14_17': 'fpgrowth_14_17_pairs.csv',
    '17_19': 'fpgrowth_17_19_pairs.csv',
    '22_23': 'fpgrowth_22_23_pairs.csv'
}

colors = {
    '1_10': 'red',
    '10_14': 'orange',
    '14_17': 'green',
    '17_19': 'blue',
    '22_23': 'purple'
}

G = nx.Graph()

for rango, file in files.items():
    df = pd.read_csv(file, skiprows=1, names=['pair', 'frequency'])
    df[['site1', 'site2']] = df['pair'].str.split(':', n=1, expand=True)
    df['site2'] = df['site2'].str.strip()
    
    top20 = df.nlargest(20, 'frequency')
    max_freq = top20['frequency'].max()

    for _, row in top20.iterrows():
        weight_norm = row['frequency'] / max_freq  # Normalizar peso
        if G.has_edge(row['site1'], row['site2']):
            if 'weights' in G[row['site1']][row['site2']]:
                G[row['site1']][row['site2']]['weights'][rango] = weight_norm
            else:
                G[row['site1']][row['site2']]['weights'] = {rango: weight_norm}
        else:
            G.add_edge(row['site1'], row['site2'], weights={rango: weight_norm})

pos = nx.spring_layout(G, k=0.3, seed=42)
plt.figure(figsize=(16, 14))

nx.draw_networkx_nodes
