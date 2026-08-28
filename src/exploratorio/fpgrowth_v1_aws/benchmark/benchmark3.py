import multiprocessing as mp
import time

def trabajo(_):
    total = 0
    for i in range(100_000_000):
        total += i
    return total

if __name__ == "__main__":
    inicio = time.perf_counter()

    with mp.Pool() as pool:
        resultados = pool.map(trabajo, range(mp.cpu_count()))

    duracion = time.perf_counter() - inicio

    operaciones = mp.cpu_count() * 100_000_000

    print(f"CPU: {mp.cpu_count()}")
    print(f"Tiempo: {duracion:.2f}s")
    print(f"Operaciones/segundo: {operaciones/duracion:,.0f}")
