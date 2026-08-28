# Camino A — corrida completa en AWS

## Instalar
```
pip install polars tqdm psutil
```
Nada más — no hace falta `pm4py`, `lifelines`, `fim` ni el resto de dependencias de otras líneas del proyecto.

## Antes de correr
Ya está apuntado a `/home/ubuntu/codigos/python/data/` (donde van los 4 zips `cu_user_client-2026_07_02_00{1..4}.zip`, ~6,7GB en total — hay que copiarlos ahí primero).

`DIM_PATH` asume `dim_integracion_cu.csv` en esa misma carpeta — **confirmar**, si está en otro lado ajustar esa línea en `paso_full_precedencia.py`.

## Correr
```
python3 paso_full_precedencia.py
```
Es seguro dejarlo corriendo desatendido y cortar el proceso en cualquier momento — checkpointea el estado completo en `checkpoints_full/estado_acumulador.pkl` después de cada zip interno procesado. Si se interrumpe, correr el mismo comando de nuevo retoma donde quedó (no reprocesa lo ya hecho).

## Qué hace
Para cada par de servicios que aparecen en la misma persona-día, cuenta cuántas veces uno ocurre antes que el otro y con qué lag en minutos — versión a escala completa de `camino_a_precedencia/paso1_orden_coocurrencia.py` + `paso3_lags_minutos.py`. No necesita `pandas` (a diferencia del prototipo de sandbox): la lectura/escritura final usa `polars`, y la acumulación línea por línea es Python puro con diccionarios (mismo patrón que `computador_aws/process/2_conversion_real/reglas_temporales3.py`), así que la RAM se mantiene acotada — nunca se cargan los ~500M+ filas completas a memoria.

## Rendimiento esperado
Verificado localmente: ~234.000 filas/s por núcleo sobre un zip interno de referencia (1M filas → ~4,3s). Con ~560 zips internos en total (4 outer × ~140 c/u) el orden de magnitud esperado es **bajo una hora** en una máquina de este tamaño (4 vCPU), muy por debajo de los jobs de horas/días que tomó FP-Growth/LCM en su momento — este cálculo es mucho más liviano (solo diccionarios de contadores, no itemsets combinatorios).

## Al terminar
Salida en `output_full/pares_orden_lags_full.parquet` (y `.csv`) — una fila por par de servicios con soporte ≥ `MIN_SOPORTE` (50 por defecto, ajustable arriba del script), incluyendo dirección dominante, % de consistencia, clasificación consistente/mixto, y estadísticas de lag en minutos (media, desvest, percentiles aproximados).

Traer ese `.parquet`/`.csv` de vuelta y cruzarlo contra `computador_aws/fpgrowth/2024_2025/final_output_traducido_2024_2025.jsonl` y `computador_aws/lcm/final_output_lcm_closed_traducido_2024_2025.jsonl` (soporte transaccional sin orden) para armar la tabla final de la ALERTA A6 del paper — replicando el cruce manual que ya se hizo a mano con la muestra chica (ver `camino_a_precedencia/resumen_prototipo.md`).

## Validado
Corrida de prueba contra el mismo zip interno usado en el prototipo de sandbox (`cu_user_client-2026-07-02-001.zip` → `cu_user_client_375.zip`): reproduce el hallazgo principal — par PJUD↔Fiscalía, soporte 252.725, 93,1% dirección dominante — con una diferencia de 8 casos sobre 235 mil (ruido de parseo, no un bug).
