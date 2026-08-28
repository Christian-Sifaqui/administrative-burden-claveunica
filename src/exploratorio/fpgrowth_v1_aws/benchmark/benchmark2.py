from multiprocessing import Pool
import time

def trabajo(n):
    s = 0
    for i in range(50_000_000):
        s += i * i
    return s

if __name__ == "__main__":
    inicio = time.perf_counter()

    with Pool() as p:
        p.map(trabajo, range(p._processes))

    fin = time.perf_counter()

    print(f"Procesos: {p._processes}")
    print(f"Tiempo: {fin - inicio:.2f} segundos")
