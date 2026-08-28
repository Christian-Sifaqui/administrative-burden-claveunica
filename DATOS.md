# Política de datos de este repositorio

Este repositorio acompaña el artículo *"Registros nacionales de identidad
digital como capa observacional del administrative burden interinstitucional:
evidencia a escala nacional desde Chile"* (Christian Sifaqui, Secretaría de
Gobierno Digital de Chile). Contiene el código completo del pipeline y los
**resultados agregados** que produjo — nunca los eventos crudos de ClaveÚnica
a nivel de persona.

## Qué SÍ está en este repositorio

- Todo el código: el pipeline actual (`src/pipeline_actual/`) y las fases
  exploratorias descartadas antes de llegar a él — Apriori, Eclat, FP-Growth y
  LCM (`src/exploratorio/`).
- Resultados agregados por **par de servicio** o **par de institución**:
  soporte, dirección, lag mediano, lift, p-valor, q-valor. Cada fila describe
  una relación entre dos servicios/instituciones a nivel poblacional, no una
  persona.
- Catálogos institucionales (`resultados/catalogo/`, `resultados/pipeline_actual/cpat/`):
  metadata de aplicaciones e instituciones, y el catálogo CPAT, que es
  información pública de transparencia activa del Estado de Chile.
- Itemsets de las fases exploratorias (`resultados/exploratorio/`): conjuntos
  de servicios con su frecuencia poblacional, sin identificador de persona.

## Qué NO está, y no va a estar

El pipeline procesa ~545M eventos de 14,1M usuarios de ClaveÚnica
(2024-2025). Ninguno de esos eventos, ni ningún archivo intermedio a nivel de
persona, se publica aquí:

- Los zips de origen (`cu_user_client-*.zip`, varios GB cada uno).
- Cualquier tabla con una fila por `(persona, fecha, servicio)`, incluidos
  archivos que en el entorno de desarrollo se llamaban "muestra" o
  "diagnóstico" — son muestras de eventos individuales, no agregados.
- Bases DuckDB intermedias (`*.duckdb`) usadas para el `GROUP BY` de
  ~700-900M filas.

Esta exclusión es deliberada y no depende de que un archivo *parezca* chico o
agregado: cada archivo publicado en `resultados/` fue abierto y su esquema
verificado columna por columna antes de subirlo. `.gitignore` además bloquea
por nombre/extensión los patrones de archivo crudo conocidos, como defensa
adicional ante un futuro `git add` descuidado.

## Rutas hardcodeadas en el código

Varios scripts del pipeline actual (`src/pipeline_actual/`) tienen rutas
absolutas del entorno original (`/home/ubuntu/codigos/python/data/...`,
`/home/crist/eclat/...`). Se dejaron así a propósito — el código se publica
como registro fiel de lo que efectivamente corrió, no como paquete listo para
ejecutar. Quien quiera reproducir un paso debe ajustar esas constantes al
inicio de cada archivo (`ZIP_PATHS`, `DIM_PATH`, `MAPA_CSV`, y los `savefig`
de `src/figuras/`).

## Anonimización de los datos de origen

Los `run` (identificador nacional) que alimentan el pipeline llegan ya
pseudonimizados por el proveedor de los datos antes de este análisis; no hay
paso de anonimización en este código. Ese es exactamente el motivo por el que
ningún archivo a nivel de persona se publica: un identificador pseudonimizado
sigue siendo un identificador único y estable por persona, no un dato
anónimo.
