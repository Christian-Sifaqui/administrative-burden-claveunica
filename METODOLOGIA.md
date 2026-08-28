# Bitácora técnica del proyecto

## Análisis de patrones de uso de servicios públicos digitales mediante registros de ClaveÚnica

**Objetivo del documento**

Este documento resume las principales decisiones metodológicas, problemas encontrados, datos utilizados y lecciones aprendidas durante el desarrollo del estudio, con el propósito de facilitar el inicio de un nuevo ciclo de investigación utilizando nuevos datos (por ejemplo, 2025 o años posteriores), evitando repetir el proceso de diseño metodológico.

---

# 1. Objetivo general del estudio

El propósito del estudio fue identificar patrones de utilización conjunta de servicios públicos digitales mediante el análisis de registros anonimizados de autenticación de ClaveÚnica.

La idea central consiste en tratar cada sesión (o conjunto de accesos asociados a un usuario) como una "transacción", aplicando técnicas de **Market Basket Analysis (MBA)** para descubrir:

* servicios utilizados individualmente;
* pares frecuentes;
* tríadas frecuentes;
* relaciones entre sectores del Estado;
* diferencias por grupos etarios;
* comportamiento estacional;
* oportunidades de interoperabilidad y ventanillas únicas.

El objetivo final no es construir rankings de popularidad, sino comprender cómo los ciudadanos utilizan conjuntamente los servicios públicos.

---

# 2. Datos utilizados

Los datos provienen de registros anonimizados de ClaveÚnica.

Cada registro contiene, entre otros:

* RUN anonimizado
* identificador de sesión (cuando existe)
* servicio utilizado
* institución
* fecha
* timestamp
* rango RUN (proxy etario)

Los datos se distribuyen en:

* ZIP principal
* múltiples ZIP internos
* archivos CSV

Fue necesario desarrollar procesamiento específico para leer ZIP anidados sin descomprimir completamente el conjunto de datos.

---

# 3. Escala del problema

El estudio trabajó aproximadamente con:

* más de 26 millones de transacciones
* varios cientos de servicios
* cientos de instituciones
* millones de usuarios anonimizados

Los algoritmos clásicos resultaron insuficientes para este volumen.

---

# 4. Algoritmos evaluados

Se evaluaron distintas alternativas.

## Apriori

Ventajas

* ampliamente conocido
* fácil de interpretar

Problemas

* extremadamente lento
* alto consumo de memoria
* impracticable para millones de registros

Fue descartado.

---

## Eclat

Más rápido que Apriori.

Sin embargo:

* seguía siendo muy costoso
* no escalaba adecuadamente

También fue descartado.

---

## FP-Growth

Finalmente fue el algoritmo seleccionado.

Ventajas observadas

* muy superior en tiempo
* menor consumo de memoria
* permite millones de transacciones
* adecuado para minería de reglas de asociación

Fue el algoritmo utilizado durante todo el estudio.

---

# 5. Parámetros utilizados

Después de múltiples pruebas se definieron parámetros relativamente conservadores.

Valores utilizados:

* minSupport ≈ 0.0005
* itemsets hasta tamaño 5
* posteriormente el artículo concentra resultados en pares y tríadas

Se recomienda realizar análisis de sensibilidad variando:

* 0.0003
* 0.0005
* 0.001

para verificar estabilidad de los resultados.

---

# 6. Problemas encontrados

## Tiempo de ejecución

El principal problema fue el tiempo requerido.

Fue necesario:

* dividir archivos
* generar chunks
* procesar por bloques

---

## Memoria

Los algoritmos no podían cargar todas las transacciones simultáneamente.

Se optó por:

* JSONL intermedio
* procesamiento por bloques

---

## Dominancia de algunos servicios

Servicios como:

* PJUD
* Registro Civil
* SII

dominan completamente los rankings.

Esto dificulta descubrir relaciones menos frecuentes.

Se propuso realizar un análisis adicional excluyendo temporalmente los principales hubs.

---

## Definición de "sesión"

Fue una discusión importante.

Alternativas consideradas:

* usuario completo
* usuario-día
* sesión
* ventana temporal

Finalmente el artículo trabaja principalmente con sesiones/transacciones.

Debe mantenerse consistente en futuros estudios.

---

# 7. Clasificación institucional

Fue necesario construir una tabla auxiliar que relaciona:

institución →

* ministerio
* clasificación institucional
* sector

Ejemplos:

* Ministerio
* Servicio
* Municipalidad
* Empresa Pública
* Hospital
* Gobierno Regional
* Poder Judicial
* Organismo Autónomo

Esta tabla permitió construir el análisis sectorial.

Debe mantenerse actualizada.

---

# 8. Proxy etario mediante RUN

No existía edad.

Se utilizó el rango del RUN como aproximación.

Se empleó la fórmula publicada en:

https://github.com/fvillena/rut-a-edad

Se construyeron seis grupos etarios:

A

14–18

B

19–26

C

27–35

D

36–45

E

46–59

F

60+

El método es aproximado.

Debe explicitarse siempre esta limitación.

---

# 9. Análisis desarrollados

Durante el proyecto se decidió analizar distintos niveles.

## Servicios individuales

Objetivo

Conocer los servicios más utilizados.

---

## Pares

Métricas utilizadas

* soporte
* confianza
* lift

Los pares permiten identificar posibles integraciones entre servicios.

---

## Tríadas

Permiten identificar trayectorias más complejas.

Se concluyó que aportan mayor riqueza que los pares para detectar momentos de vida.

---

## Sectores

Los servicios fueron agrupados por sector institucional.

Se analizaron:

* frecuencia
* hubs
* relaciones entre sectores

---

## Rango etario

Se comparó:

* servicios
* pares
* tríadas

por grupo etario.

---

## Estacionalidad

Se construyeron series mensuales.

Se identificaron picos asociados a:

* declaración de renta
* FUAS
* subsidios
* procesos electorales
* reclutamiento
* beneficios sociales

---

# 10. Métricas utilizadas

Se decidió trabajar principalmente con:

Frecuencia

Soporte

Confianza

Lift

Hub Score

Usuarios únicos

Participación (share)

Tasas por 1.000 usuarios

No todas las métricas aparecen en todas las tablas.

---

# 11. Problemas de interpretación

Se discutieron varios riesgos.

## Co-ocurrencia no implica causalidad

Fue una de las principales limitaciones.

Los servicios pueden aparecer juntos sin que uno provoque al otro.

---

## Frecuencia no implica importancia

Servicios masivos dominan los rankings.

Lift resulta más útil para detectar afinidades.

---

## Share depende del número de servicios

Sectores con muchas aplicaciones pueden concentrar más accesos.

No debe confundirse con importancia institucional.

---

# 12. Gráficos considerados

Se propusieron diversos gráficos.

Entre ellos:

* heatmap de lift
* red de servicios
* red sectorial
* Sankey
* UpSet Plot
* series temporales
* hub score
* matrices por grupo etario

No todos fueron construidos.

Quedan como trabajo futuro.

---

# 13. Orientación del artículo

Durante la revisión quedó claro que el artículo debe ser interpretativo y no solamente descriptivo.

Los resultados deben responder preguntas como:

* ¿qué servicios estructuran el ecosistema?
* ¿qué sectores funcionan como hubs?
* ¿qué momentos de vida aparecen?
* ¿qué integraciones deberían priorizarse?

No basta con presentar tablas.

Cada resultado debe tener interpretación.

---

# 14. Enfoque conceptual

El artículo evolucionó desde un estudio de frecuencias hacia un estudio de:

* comportamiento ciudadano digital
* integración de servicios
* interoperabilidad
* ventanillas únicas
* diseño basado en evidencia

La discusión debe conectar los hallazgos con:

* Gobierno Digital
* Life Events
* Data Driven Government
* Interoperabilidad

---

# 15. Publicación objetivo

El trabajo fue orientado principalmente hacia:

Government Information Quarterly (Elsevier)

Como alternativa:

Digital Government: Research and Practice (ACM)

Esto implicó fortalecer:

* discusión
* limitaciones
* reproducibilidad
* ética
* trabajo futuro

---

# 16. Lecciones aprendidas

Las principales lecciones fueron:

* FP-Growth escala adecuadamente para este tipo de datos.
* El análisis sectorial resulta más interpretable que trabajar únicamente con instituciones.
* El lift entrega información mucho más útil que la frecuencia absoluta.
* Los grupos etarios muestran diferencias claras en el uso de servicios.
* La estacionalidad explica una parte importante de la demanda digital.
* Los resultados deben interpretarse desde la perspectiva de los "momentos de vida" y no únicamente como relaciones estadísticas.

---

# 17. Recomendaciones para un nuevo ciclo

Antes de iniciar un nuevo procesamiento conviene:

* actualizar el mapeo institución → sector;
* verificar servicios nuevos y servicios eliminados;
* mantener la misma definición de transacción;
* recalcular el proxy etario;
* repetir el análisis de sensibilidad de minSupport;
* reconstruir todos los indicadores con exactamente la misma metodología;
* comparar resultados entre años para detectar estabilidad o cambios estructurales.

---

# 18. Posibles extensiones

Quedaron identificadas varias líneas futuras:

* análisis secuencial (no sólo co-ocurrencia);
* minería predictiva;
* clustering de ciudadanos;
* grafos dinámicos;
* detección automática de momentos de vida;
* comparación internacional;
* modelos de recomendación de servicios públicos;
* evaluación de servicios proactivos basados en IA.

---

# 19. Aspectos que conviene documentar desde el inicio

En un nuevo ciclo es recomendable registrar desde el primer día:

* versión del dataset;
* período cubierto;
* cantidad de registros;
* cantidad de usuarios;
* cantidad de servicios;
* cantidad de instituciones;
* parámetros de FP-Growth;
* tiempo de ejecución;
* memoria utilizada;
* infraestructura computacional;
* versiones de Python y bibliotecas;
* decisiones metodológicas adoptadas;
* problemas encontrados y su solución.

Mantener esta bitácora reducirá significativamente el tiempo necesario para reproducir el estudio y facilitará la comparación longitudinal entre distintos períodos de datos.
