#!/usr/bin/env python3

import time

N = 100_000_000

inicio = time.perf_counter()

x = 0
for i in range(N):
    x += i * i

duracion = time.perf_counter() - inicio

ops_por_segundo = N / duracion

print(f"Tiempo: {duracion:.3f} s")
print(f"Iteraciones por segundo: {ops_por_segundo:,.0f}")
