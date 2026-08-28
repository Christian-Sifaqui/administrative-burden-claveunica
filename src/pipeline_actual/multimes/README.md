# Camino A — fase multi-mes (AWS + DuckDB)

## Por qué esta fase existe

`camino_a_precedencia/aws/paso_full_precedencia.py` (fase anterior) solo mira orden
dentro de un mismo día (una fila = una persona-día). Sirve para pares que se usan en
ráfaga el mismo día, pero es ciego a patrones administrativos reales como
Registro Social de Hogares → (meses después) → FUAS, donde el lag es de semanas/meses,
no horas. Validado en muestra chica en `../validar_rsh_fuas.py`: 102.017 personas con
ambos servicios, 56,1% RSH-antes-FUAS vs 25,1% al revés, lag mediano 194 días.

Esta fase reconstruye, para cada persona (`run_anonimizado`, estable en el tiempo —
verificado), la **primera fecha** en que usa cada servicio a lo largo de TODO
2024-2025, y calcula dirección/lag en días para todos los pares con soporte suficiente
— no solo los ya conocidos por FP-Growth/LCM.

## Por qué DuckDB y no streaming+dict

El cálculo de "primera fecha por (run, servicio)" es un `GROUP BY` sobre ~700-800M
filas persona-día-servicio (todo 2024-2025, no una muestra). Un diccionario de Python
ya reventó memoria en la muestra chica (80M filas) con un simple contador. DuckDB
agrega out-of-core (usa disco vía `PRAGMA temp_directory` cuando no cabe en RAM) — la
máquina AWS tiene 4 vCPU / 15GB RAM + 15GB swap, no alcanza para tener todo en RAM de
Python puro a esta escala.

## Instalar

```
pip install duckdb pandas tqdm
```

## Antes de correr

Rutas ya confirmadas y configuradas (2026-07-13): scripts en
`~/codigos/pyhton/camino_a_precedencia/multimes`, datos en `~/codigos/python/data`
(la MISMA ruta que usó `paso_full_precedencia.py` — una confirmación anterior con
"pyhton" duplicado en la ruta de datos era incorrecta, ya corregida en los 3
scripts). Nombres de zip confirmados con guión, no guión bajo:
`cu_user_client-2026-07-02-001.zip` (`paso_full_precedencia.py` tiene el glob con
guión bajo — si esa corrida no matcheó nada al principio, es el mismo bug, ver su
propio historial de fix de glob). Si las rutas cambian, ajustar `ZIP_PATHS` (en
`1_ingesta.py`, `2a_diagnostico.py` y `2b_pares.py` — está duplicado en los tres
para el chequeo de completitud de la ingesta) y `DIM_PATH` (solo en `1_ingesta.py`).

## Correr — en este orden, cada paso es un archivo separado a propósito

### 1. Ingesta (la parte cara, horas de cómputo)
```
python3 1_ingesta.py
```
Aplana los zips a la tabla `eventos(run, fecha, servicio, rango_run)` en
`output_multimes/precedencia_multimes.duckdb` — deduplicada dentro del día (corrige
el bug de multiplicidad de la fase anterior). Checkpointea en la tabla `procesados`
del mismo archivo `.duckdb` (no en un pickle aparte) — cada inner zip se inserta en
una transacción, así que es seguro cortar y retomar sin reprocesar ni duplicar.

### 2a. Diagnóstico (barato, minutos — correr y REVISAR antes de 2b)
```
python3 2a_diagnostico.py
```
Imprime la distribución de volumen y de servicios-distintos por run, y una
**estimación** del tamaño del self-join de 2b antes de correrlo. Si la estimación
"con tope" luce demasiado grande para la RAM/disco disponibles, ajustar `PCTL_BOT`
(filtro de bots, más estricto) o `MAX_SERVICIOS_DISTINTOS_RUN` (tope duro adicional)
en **ambos** `2a_diagnostico.py` y `2b_pares.py` (deben coincidir), y volver a correr
2a antes de seguir.

### 2b. Self-join de pares (la parte pesada del análisis)
```
python3 2b_pares.py
```
Requiere que `2a_diagnostico.py` haya corrido al menos una vez (falla explícitamente
si falta la tabla `resumen_run`). El self-join de `primera_fecha` consigo misma se
materializa UNA vez como `pares_run` (fase D, ~900M filas) y se reutiliza si el script
se corre de nuevo (no se recalcula salvo que se borre la tabla).

**Diseño "por partes" (agregado 2026-07-14 tras dos caídas por OOM):** un único
`GROUP BY` sobre las ~900M filas de `pares_run` puede tener hasta ~800.000 grupos
distintos (1.266 servicios ⇒ hasta 1.266×1.265/2 pares posibles) — mantener el estado
de agregación (conteos + sketches de cuantil) de cientos de miles de grupos a la vez
ya alcanza varios GB, sin importar si se usa `MEDIAN` exacto o `approx_quantile`
(cambiar solo la función de agregación no alcanzó, la máquina murió igual). Por eso
las fases E/F/G procesan `pares_run` **en franjas** de
`hash(servicio_a || '#' || servicio_b) % N_SHARDS` (`N_SHARDS = 50` por defecto) —
**por par (arista), no por servicio individual (nodo)**. Se probó primero particionar
solo por `servicio_a` y se descartó: como cualquier plataforma tiene unos pocos
servicios "hub" (login, home, etc.) que aparecen como `servicio_a` en cientos de
pares distintos, todas esas filas caerían en la MISMA franja (`hash(hub)` es fijo,
sin importar cuántas franjas haya) — una franja se vuelve cuello de botella de tiempo
y memoria mientras las demás terminan en segundos (el problema clásico de particionar
grafos por nodo de alto grado). Al hashear el **par completo**, las aristas de un hub
se reparten entre muchas franjas distintas. Sigue siendo correcto: como el self-join
usa `a.servicio < b.servicio`, cada par tiene un `(servicio_a, servicio_b)` fijo —
todas sus filas comparten el mismo hash y caen juntas en la misma franja, así que las
franjas son disjuntas y no hace falta fusionar resultados parciales después, cada
`INSERT` por franja ya es el resultado final para esos pares. Cada franja corre en su
propia transacción (agregar + `COMMIT` + marcar en `shards_procesados`) antes de pasar
a la siguiente: si la máquina muere a mitad de camino, se pierde como máximo una
franja (unos minutos), no la corrida completa — volver a correr `2b_pares.py` retoma
desde la primera franja pendiente. Si con 50 franjas la máquina sigue muriendo por
memoria, subir `N_SHARDS` (ej. a 100) y borrar `shards_procesados` / `pares_multimes` /
`pares_por_trimestre` / `pares_por_cohorte` para reiniciar desde cero con franjas
más chicas (`pares_run`, el self-join en sí, NO hace falta borrarlo ni recalcularlo).

Produce:
- `pares_multimes.csv` — soporte, dirección, lag en días por par (TODOS los pares con
  soporte ≥ `MIN_SOPORTE_PAR`, no solo los de FP-Growth/LCM).
- `pares_por_trimestre.csv` — misma dirección desglosada por trimestre (estabilidad
  temporal, ALERTA A6).
- `pares_por_cohorte.csv` — misma dirección desglosada por grupo etario **A-F**
  (ALERTA A8). Grupos derivados de `rango_run` con la fórmula publicada en
  https://github.com/fvillena/rut-a-edad, ya usada en el proyecto (`Claude.md` sec. 8
  y `fpgrowth2/run_rango_tablaA/tablaA_grupo_etario.py`) — **no** el `rango_run` crudo.
  Fronteras solapadas a propósito (un run puede caer en 2 grupos adyacentes a la vez).
  Sigue siendo un proxy no validado externamente contra una fuente de verdad (censo,
  padrón, FONASA) — tratar como exploratorio, igual que en el resto del paper.

  Además de los 6 grupos A-F, se agregan dos columnas de **edad continua**
  (`edad_estimada_media`, `edad_estimada_mediana`) usando la misma fórmula de
  rut-a-edad de forma directa (`año_nacimiento ≈ RUN × 3,3363697569700348×10⁻⁶ + 1932,26`)
  sobre el punto medio del bucket de `rango_run` (que solo da el RUN truncado a millones,
  no el RUN exacto por persona — más granular que A-F, pero sigue siendo aproximado a
  nivel de ~1 millón de RUN, es decir ~3,3 años). La edad se calcula respecto al **año
  de `fecha_b`** (el año del segundo evento de cada par), no un año fijo — el dataset
  cubre 2024 y 2025, y la misma persona tiene distinta edad estimada según cuándo ocurrió
  el evento. Verificado: la fórmula aplicada a los límites de `rango_run` de cada grupo
  reproduce casi exacto las mismas fronteras A-F (ej. rango_run=14 → edad 45,0, el límite
  D/E de 45/46).

Como 2a y 2b son rápidos una vez que `eventos` está poblado, se pueden re-correr
las veces que haga falta para ajustar umbrales (`MIN_SOPORTE_PAR`, `PCTL_BOT`,
`MAX_SERVICIOS_DISTINTOS_RUN`) sin volver a leer los zips.

## Filtro de bots

A nivel de `run` completo (no por par): se excluyen los runs en el percentil más alto
de volumen total de eventos en 2024-2025 (`PCTL_BOT`, default 0.999 = 0,1% de mayor
volumen), más un tope duro de servicios-distintos-por-run (`MAX_SERVICIOS_DISTINTOS_RUN`,
default 100) como seguridad adicional para acotar el costo del self-join. En la
validación chica de RSH-FUAS, 0 de 102.017 runs superaron el umbral de 20
servicios-distintos-en-un-día — ese par en particular no tenía contaminación de bots;
otros pares (ej. los que involucran PJUD/SII, con usuarios de cientos de visitas/día)
sí la tenían, de ahí la necesidad del filtro general.

## Pendiente / no resuelto en este diseño

- Los pares descubiertos que NO están en las tablas FP-Growth/LCM ya publicadas en el
  paper son candidatos nuevos, no validados contra nada previo — revisar con criterio
  antes de reportarlos (podrían ser artefactos del filtro de bots siendo insuficiente).
- `MIN_SOPORTE_PAR = 50` es el mismo valor que usó la fase anterior por consistencia,
  pero no fue re-derivado para esta escala/ventana — ver ALERTA A2 (umbrales de soporte).
