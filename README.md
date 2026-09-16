# Trayectorias interinstitucionales de servicios ciudadanos vía ClaveÚnica

Código y resultados agregados del estudio *"Registros nacionales de identidad
digital como capa observacional del administrative burden interinstitucional:
evidencia a escala nacional desde Chile"*. El artículo detecta y verifica, con evidencia
administrativa caso a caso, qué dependencias entre trámites del Estado de
Chile son reales y no solo estadísticamente aparentes, usando los logs de
autenticación de ClaveÚnica (2024-2025, 14,1M usuarios) como capa
observacional.

**Este repositorio no contiene datos a nivel de persona.** Ver [DATOS.md](DATOS.md)
para la política completa de qué se publica y por qué.

## Estructura

```
src/
  pipeline_actual/     Pipeline vigente, el que produjo los resultados del artículo
    aws/                Fase 1: orden y lag dentro del mismo día
    multimes/            Fase 2: reconstrucción multi-mes (DuckDB), red institucional,
                          modelos nulos, medidas de Burt, estabilidad de Louvain/Leiden,
                          control de FDR
  figuras/              Scripts que generan las figuras del artículo
  exploratorio/         Fases evaluadas y descartadas antes del pipeline actual
    eclat_apriori/        Primeras pruebas con Eclat/Apriori
    fpgrowth_v1/           Primera iteración de FP-Growth
    fpgrowth_v1_aws/        FP-Growth corrido en AWS, benchmarks
    fpgrowth_v2/            Segunda iteración: por sector, por cohorte etaria
    lcm/                    LCM (Linear time Closed itemset Miner) como alternativa

resultados/
  pipeline_actual/       Salidas agregadas del pipeline vigente (por par de
                          servicio/institución: soporte, dirección, lag, lift,
                          p-valor, q-valor — nunca por persona)
  exploratorio/           Salidas agregadas de las fases descartadas
  catalogo/                Catálogo de aplicaciones e instituciones ClaveÚnica

APENDICE_METODOLOGICO.md  El apéndice metodológico complementario que el artículo cita
                         explícitamente en el cuerpo, notas al pie y anexos (incluye
                         Tabla A.1, Tabla A.2, y las Secciones 3 y 22 citadas por número)
METODOLOGIA.md          Bitácora técnica: algoritmos evaluados, parámetros,
                         problemas encontrados y decisiones de diseño
DATOS.md                Qué datos se publican, cuáles no, y por qué
```

## Orden de lectura sugerido

1. `APENDICE_METODOLOGICO.md` — para verificar cualquier cifra o decisión
   metodológica que el artículo remite a "el apéndice metodológico
   complementario", sin necesidad de leer código.
2. `METODOLOGIA.md` — contexto general: por qué FP-Growth y no Apriori/Eclat,
   parámetros usados, problemas de escala.
3. `src/pipeline_actual/aws/README.md` y `src/pipeline_actual/multimes/README.md`
   — el pipeline vigente, paso a paso, con las decisiones de diseño documentadas
   inline (por qué DuckDB, por qué particionar por arista y no por nodo, etc.).
4. `resultados/pipeline_actual/` — para verificar una cifra del artículo sin
   volver a correr nada.

## Reproducibilidad

El código se publica como registro fiel de lo que efectivamente corrió, no
como paquete listo para ejecutar sin ajustes: varios scripts tienen rutas
absolutas del entorno de desarrollo original que hay que apuntar a una copia
propia de los datos de ClaveÚnica (no publicados, ver DATOS.md). Quien tenga
acceso a esos datos por otra vía puede reproducir el pipeline completo desde
`src/pipeline_actual/`; quien no, puede verificar cualquier cifra del
artículo directamente contra `resultados/pipeline_actual/`.

## Citación

Artículo en proceso de publicación. Esta sección se actualizará con la
referencia completa cuando esté disponible.

## Licencia

Código (`src/`): [MIT](LICENSE).
Resultados agregados (`resultados/`): [CC-BY-4.0](resultados/LICENSE).
