# Apéndice metodológico complementario

Este documento es el "apéndice metodológico complementario" que el artículo *Registros
nacionales de identidad digital como capa observacional del administrative burden
interinstitucional: evidencia a escala nacional desde Chile* (Christian Sifaqui) cita
repetidamente en el cuerpo del texto, en notas al pie y en los Anexos A–D. Contiene el
detalle computacional, los umbrales exactos, las verificaciones de robustez y las
decisiones metodológicas que el cuerpo del artículo resume o menciona sin desarrollar,
para que el estudio sea auditable y reproducible por un tercero con acceso a datos
equivalentes.

La numeración de las secciones es la que el artículo cita explícitamente en dos lugares
(nota al pie 4 → Sección 3; Anexo A → Sección 22) y se mantiene fija por esa razón. El
resto de las referencias del cuerpo del artículo al "apéndice metodológico complementario"
no citan número de sección; cada una se resuelve aquí por tema, con un puntero explícito
de vuelta a la sección o tabla del artículo que la origina.

Los scripts del pipeline, y los resultados agregados que respaldan cada cifra de este
documento, están publicados como material suplementario en este mismo repositorio —
`src/` para el código, `resultados/` para las salidas agregadas (ver Sección 31).

---

## 1. Criterios de deduplicación

El pipeline procesa dos formatos de origen distintos, según el tramo temporal:

- **Formato `client_id_traza`** (usado por el pipeline de FP-Growth/LCM que produce la
  validación cruzada de soporte de la Sección 4.5 del artículo): cada fila representa un
  (usuario anonimizado, día), y la lista de `client_id` viene ya deduplicada por servicio
  dentro del día en el dato de origen.
- **Formato `hora_client_id`** (usado por el pipeline secuencial principal, Sección 3.2
  del artículo): cada fila trae una lista de pares hora→`client_id` que puede incluir
  visitas repetidas al mismo servicio en el mismo día. La deduplicación se aplica
  explícitamente al construir el conjunto de servicios distintos usados por persona y día.
  Este paso corrige un problema de conteo detectado en una fase anterior del proyecto, que
  contaba cada combinación de visitas repetidas en el mismo día en vez de una vez por
  servicio distinto — un error de un orden de magnitud (~70×) sobre el soporte de pares
  frecuentes, encontrado y corregido antes de que ningún resultado de esa fase se
  publicara.

La deduplicación a nivel de persona usa una partición por hash del identificador
anonimizado, que garantiza que todas las filas de una misma persona —sin importar en qué
archivo de origen o fecha aparezcan— se procesen juntas antes de construir su conjunto de
servicios usados en el período completo. Esto se verificó comparando el soporte del
pathway ancla (Registro Social de Hogares→FUAS) calculado de forma independiente por el
pipeline secuencial (524.520) contra el de FP-Growth y LCM (525.125 en ambos): una
diferencia de 0,12% entre tres pipelines construidos sobre lógicas de partición distintas
(Sección 4.5 del artículo).

## 2. Reglas de normalización de nombres institucionales

El catálogo de aplicaciones e instituciones (`dim_integracion_cu.csv` / `cu_con_sectores.csv`
en `resultados/catalogo/`) usa el nombre de la institución como texto libre, no un código
único, y contiene inconsistencias verificadas directamente:

- El catálogo técnico completo trae 716 cadenas de texto distintas en el campo
  institución, de las cuales un subconjunto son variantes de mayúsculas o tildes de la
  misma institución (ej. "FONASA" / "Fonasa"), resueltas con normalización determinística
  (NFKD → quitar tildes → mayúsculas → colapsar espacios) y por al menos 23 siglas
  verificadas contra su nombre completo dentro del mismo catálogo (MDS, SAG, SRCEI, CNR,
  CONAF, DGA, IPS, JUNAEB, MINSAL, SENAMA, SERNAC, SERVEL, SUBTEL, CORFO, INAPI,
  SERNAPESCA, SUBPESCA, ISL, SENDA, MTT, TGR, MINSEGPRES, entre otras).
- Tras esa limpieza, el catálogo normalizado completo tiene 554 instituciones distintas —
  una cifra diferente tanto del catálogo técnico crudo (716) como del recuento de
  instituciones con tráfico real usado en el cuerpo del artículo (457, Sección 3.1) y del
  mapeo más granular usado para la red institucional (375, Sección 3.4). Las tres cifras
  son legítimamente distintas porque miden cosas distintas: 716 es el catálogo técnico sin
  normalizar; 554 es el mismo catálogo normalizado; 457 es el subconjunto con tráfico
  observado en 2024-2025 (sin normalizar nombres duplicados); 375 es el mapeo específico
  usado para la red institucional, construido sobre una fuente distinta
  (`cu_con_sectores.csv`) y ya normalizado. Ver Sección 18 para la cuantificación de la
  brecha entre 457 y 375.
- Del catálogo técnico completo, 60 de 1.734 `client_id` (3,5%) tienen un nombre de
  aplicación que corresponde inequívocamente a un ambiente de prueba o desarrollo (patrones
  `TEST`, `QA`, `DEV`, `SANDBOX`, `DEMO`, `PRUEBA`, `UAT`, entre otros verificados
  manualmente); de las 716 instituciones del catálogo, 12 tienen el 100% de sus `client_id`
  marcados así y por tanto no son instituciones reales (incluyen dos cuentas de prueba
  registradas bajo nombre de persona, un proveedor privado de software, y variantes de
  prueba de instituciones que sí tienen tráfico real bajo otra cadena de texto en el mismo
  catálogo).

## 3. Umbrales mínimos de soporte, por análisis

| Análisis | Umbral | Notas |
|---|---|---|
| FP-Growth (validación cruzada de soporte) | Soporte mínimo 1% de usuarios, confianza mínima 0,3 | |
| LCM (validación cruzada de soporte) | Soporte mínimo 0,1% de usuarios | |
| Pipeline secuencial principal (soporte de pares) | Soporte mínimo 50 personas | Umbral heredado de una fase anterior del proyecto por consistencia; sometido a análisis de sensibilidad formal en la Sección 4.9 del artículo, que muestra que el conteo de candidatos es prácticamente insensible a este umbral específico entre 25 y 200 personas. |
| Pipeline secuencial (desglose trimestral/cohorte) | Soporte mínimo 20 personas | Umbral interno, nunca publicado celda por celda; ver Anexo A del artículo sobre el piso real de divulgación (342 personas, Sección 22). |
| Filtro de automatizaciones (pipeline secuencial) | Percentil 99,9 superior de volumen total de eventos en 2024-2025, más cualquier usuario con más de 100 servicios distintos usados | Dos señales de comportamiento no atribuible a una persona individual (cuentas de integración, automatizaciones institucionales), no un juicio caso a caso. Afecta al 0,10% de los usuarios del período (nota al pie 4 del artículo). |

## 4. Lógica de partición temporal

- **FP-Growth y LCM:** sin partición temporal — un solo análisis sobre el período completo
  2024-2025, transacciones a nivel de persona (conjunto de todos los servicios distintos
  usados alguna vez en el período, sin fecha).
- **Comparación 2024 vs. 2025:** los conteos agregados (eventos, usuarios, servicios,
  instituciones) se calculan por año por separado como verificación de robustez (Sección
  3.5 del artículo).
- **Pipeline secuencial principal:** partición trimestral (8 trimestres, 2024-T1 a
  2025-T4), usada para la tabla de estabilidad temporal y para descartar pares
  contaminados por relanzamientos de plataforma (ver Sección 11).
- **Sin ventana de co-uso:** la minería de asociación (Sección 3.4/4.5 del artículo) no usa
  ninguna ventana temporal explícita (1h, 24h, 7 días, etc.) — es una canasta de todo el
  período 2024-2025 por persona. Esta es la razón por la que esa vía se trata como
  validación secundaria y no como método primario: maximiza poder estadístico a costa de
  mezclar ráfagas administrativas puntuales con dependencias genuinamente secuenciales.

## 5. Cobertura poblacional y proxy etario

La cobertura del ecosistema (84,4% de la población de 14 años o más, con gradiente etario
marcado — 47,0% en el tramo 14-18 años frente a 95-99% en tramos adultos) se documenta en
la Sección 3.1 del artículo. El proxy etario basado en rango de RUN, sus tres fuentes de
error estructurales y su validación externa contra proyecciones del INE se documentan
íntegramente en el Anexo B del artículo y se extienden en las Secciones 8, 9 y 30 de este
apéndice.

## 6. Infraestructura y entorno de cómputo

- Máquina de cómputo: 4 vCPU, 15 GB RAM + 15 GB swap, ~189 GB de disco libre.
- DuckDB para agregación out-of-core sobre ~545 millones de filas evento-persona-servicio,
  necesario porque una agregación en memoria pura de Python no escala a este volumen en la
  RAM disponible (verificado empíricamente: un contador simple ya alcanzó varios GB de RAM
  con una muestra de 80 millones de filas).
- Minería de itemsets frecuentes: implementación en C de Christian Borgelt (bindings de
  Python) para FP-Growth y LCM.
- Análisis de redes: `networkx` y `python-louvain`, en un entorno virtual dedicado, sin
  modificar el entorno del sistema.

## 7. Prueba de línea base hipergeométrica (lift vs. azar)

Todas las cifras de "lift vs. azar" del artículo usan el mismo test: bajo un modelo nulo de
independencia estadística —el uso de cada servicio o institución por cada persona tratado
como eventos independientes a sus tasas marginales observadas—, el conteo de co-ocurrencia
esperado y su distribución de muestreo exacta siguen una distribución hipergeométrica. Es
el equivalente exacto de forma cerrada de un test de permutación que reasigna
aleatoriamente qué usuarios "tienen" cada servicio mientras preserva el conteo total de
cada uno, sin necesidad de remuestreo.

    Lift = co-uso observado / [(marginal_A × marginal_B) / N]

El test se aplica en cola superior (interesa detectar enriquecimiento, no empobrecimiento).
Esa elección tiene una consecuencia declarada en la Sección 27 de este apéndice
(bimodalidad de p-valores).

## 8. Validación externa de las cohortes A–F contra proyecciones del INE

La validación de las cohortes etarias del Anexo B contra proyecciones de población
oficiales usa tres precisiones metodológicas no evidentes desde el cuerpo del artículo:

- **Proyección de población, no conteo censal.** La fuente correcta es la proyección de
  población del INE (base Censo 2024, desagregada por edad simple), no el conteo directo
  del Censo, que subestima la población real por diseño (el propio INE estima una omisión
  censal de aproximadamente 7,0% para el Censo 2024). Usar el conteo censal como
  denominador habría inflado artificialmente los porcentajes de representación de
  ClaveÚnica en todas las cohortes.
- **Edad simple, sin interpolación.** La tabla de proyección usada viene por edad simple
  (año a año), lo que permite sumar exactamente la población dentro de cada límite de
  cohorte (A: 14-18, B: 19-26, etc.) sin ningún supuesto de distribución uniforme dentro de
  un tramo quinquenal.
- **Denominador de población de 14 años o más, no población total.** Los menores de 14 años
  están estructuralmente ausentes de ClaveÚnica y de las cohortes A-F; usar la población
  total como denominador infla artificialmente la subrepresentación aparente de las
  cohortes jóvenes. El denominador correcto —16.693.840, población de 14+ según la
  proyección INE base Censo 2024— es el mismo valor usado en el análisis de cobertura por
  edad de la Sección 3.1 del artículo.

Con esas tres correcciones, la cohorte A aparece subrepresentada en ClaveÚnica (4,64%
observado frente a 7,73% esperado, consistente con la cobertura de solo 47,0% en ese
tramo), y las cohortes B-F muestran sobrerrepresentación creciente con la edad (+3,3 a
+6,7 puntos porcentuales) — cifras reportadas en el Anexo B del artículo.

## 9. Cuantificación del sesgo migrante en el proxy etario

El proxy etario basado en RUN subestima la magnitud del sesgo introducido por la población
migrante si no se declara explícitamente: dado que a las personas extranjeras se les asigna
RUN de forma correlativa según fecha de trámite (no según edad biológica), quien regulariza
su situación migratoria en un año dado recibe un RUN cercano al de un recién nacido chileno
de ese mismo año, y la fórmula del proxy los clasifica en una cohorte joven que no les
corresponde.

El límite superior de rango de RUN que las cohortes A-F de este estudio efectivamente usan
(RUN ≤ 23 millones, heredado del umbral etario 14+ que ClaveÚnica ya aplica, no fijado como
control deliberado contra este sesgo) excluye por diseño el grueso de la ola migratoria más
reciente de Chile: bajo la fórmula del proxy, RUN=23 millones corresponde aproximadamente
al año 2010, y la ola migratoria de mayor magnitud (desde 2017 en adelante) recibió RUN muy
por encima de ese umbral, quedando fuera de toda cohorte A-F en vez de mal clasificada
dentro de A o B. Lo que ese límite no resuelve es la fracción de población extranjera que
regularizó su situación antes de 2010, dentro del rango de RUN que las cohortes A y B (las
dos más jóvenes) sí usan; los logs anonimizados no tienen campo de nacionalidad, por lo que
esa fracción no se puede aislar ni cuantificar con los datos disponibles.

Para dimensionar la magnitud del fenómeno a escala nacional (aunque no se pueda aislar
dentro de las cohortes A/B específicamente): 1.918.583 personas extranjeras residentes
estimadas al 31-12-2023 (INE & SERMIG, 2024), concentradas en
edad adulta activa (30-39 años), equivalentes a 9,6% de la población nacional bajo el mismo
denominador de proyección INE usado en el resto de este estudio.

**Nota de actualización.** Este análisis usa el límite de RUN=23M bajo la fórmula de proxy
etario del Anexo B, que es una fórmula pública no oficial (Villena, 2019). La respuesta
oficial del Servicio de Registro Civil e Identificación a la solicitud de transparencia
citada en el Anexo B (Sección 30 de este apéndice) confirma un mecanismo relacionado pero
distinto: la numeración de RUN fue local por oficina —no correlativa a nivel nacional— hasta
el año 2000, unificándose recién a partir del RUN 21 millones. La fecha exacta en que la
correspondencia RUN↔año de trámite se vuelve confiable a nivel nacional es, por tanto, más
tardía y más incierta de lo que el límite de 23M por sí solo sugiere; ambas fuentes de error
(fragmentación pre-2000 y correlatividad por fecha de trámite en vez de edad biológica)
apuntan en la misma dirección y no se cuantifican de forma independiente en este estudio.

## 10. Construcción de la red institucional completa

La red de co-uso institucional se construye reutilizando la fecha de primer uso por
(persona, servicio) ya materializada por el pipeline secuencial, sin releer datos crudos.
Se agrega un mapeo `client_id → institución` normalizado con el criterio de la Sección 2 de
este apéndice: 464 cadenas crudas de institución colapsan a 375 instituciones distintas.

Se calcula la primera fecha de uso por (persona, institución) y se hace un self-join de esa
tabla consigo misma para obtener soporte, dirección y simultaneidad para todos los pares
con soporte mayor a cero — no una selección manual de pares. Resultado: 43.005 pares
institución-institución con soporte mayor a cero.

Sobre ese universo, filtrado a soporte≥50 (mismo umbral de la Sección 3), quedan 19.220
pares sobre 330 instituciones conectadas. Se construyen tres representaciones: (a) no
dirigida, peso=soporte ("red bruta"); (b) no dirigida, solo pares con lift≥1,2, peso=lift
("red de enriquecimiento"); (c) dirigida, peso=fracción de dirección dominante, para flujo
neto por institución. Sobre ambas redes no dirigidas se calculan grado, centralidad de
intermediación, densidad y modularidad (algoritmo de comunidades de Louvain). La
transformación de peso a distancia usada para la centralidad de intermediación se documenta
en la Sección 19 de este apéndice.

## 11. Verificaciones adicionales de robustez

**11.1. Exclusión de los tres hubs de mayor volumen.** Los tres hubs por volumen de eventos
(Poder Judicial, Servicio de Impuestos Internos, Registro Civil) se excluyeron de la red de
19.220 pares (soporte≥50), dejando 18.255. El top-25 por lift no cambia en absoluto —ninguno
de los tres hubs aparece en él— y ninguno de los 12 pathways de la Tabla 1 del artículo toca
a los tres hubs excluidos.

**11.2. Comparación entre normalizaciones (evento, usuario-día, usuario único).**
Correlación de Spearman entre el ranking de servicios por volumen bajo cada normalización:
ρ=0,87 entre eventos y usuario único (n=328, 12/15 de coincidencia en el top-15); ρ=0,9996
entre eventos y usuario-día (15/15 de coincidencia en el top-15, razón eventos/usuario-día
de 1,00-1,10 para las 15 instituciones de mayor volumen) — es decir, casi toda la brecha
entre eventos y usuario único proviene de visitas en días distintos a lo largo de
2024-2025, no de reincidencia el mismo día.

**11.3. Descubrimiento y corrección de un artefacto de relanzamiento de plataforma
(Ventanilla Única Social).** Al recalcular el test hipergeométrico por ventana mensual
(Sección 13 de este apéndice) para el pathway ancla (Registro Social de Hogares→FUAS), se
detectó que el marginal mensual de Registro Social de Hogares cae de un rango estable de
700 mil a 1,7 millones de usuarios (enero 2024 a mayo 2025) a un rango de 250 a 1.000
usuarios mensuales (julio a diciembre 2025) — una caída de más de 1.000×, casi exactamente
un mes después del lanzamiento de la plataforma Ventanilla Única Social (19 de mayo de
2025).

Se verificó, no se asumió: en los meses de colapso del marginal de Registro Social de
Hogares, entre 38% y 54% de ese marginal reducido corresponde a personas que también
usaron Ventanilla Única Social ese mismo mes (lift 5,6×-10,0×, p hasta 10⁻²⁵² en algunos
meses); y Ventanilla Única Social→FUAS muestra, desde mayo de 2025 en adelante, un
enriquecimiento mensual sostenido (lift 3,9×-5,9×, mediana 5,35×) de magnitud casi idéntica
a la que Registro Social de Hogares→FUAS mostraba antes de la migración (mediana 5,77×), con
un pico de 81.228 co-usuarios en octubre de 2025 (mes de postulación FUAS). Esto confirma
que el concepto del pathway se mantuvo estable durante todo 2024-2025; lo que cambió a
mitad de año fue el identificador de plataforma que lo registra en los logs, no el
comportamiento ciudadano. Este hallazgo motiva el filtro de estabilidad trimestral que
excluye pares con el patrón "soporte cero antes de una fecha y masivo después" (Sección 3.5
del artículo), y es la razón por la que Ventanilla Única Social y Sucursal Virtual FONASA
se excluyen explícitamente del análisis de pares nombrados a mano.

## 12. Nota de alcance sobre privacidad y gobernanza

El marco conceptual de privacidad (regla de frecuencia mínima de *statistical disclosure
control*, piso real de divulgación verificado en 342 personas, distinción respecto de
k-anonymity y l-diversity) y la base legal del estudio se documentan íntegramente en los
Anexos A y D del artículo, no se duplican aquí. El umbral de soporte mínimo que sostiene esa
discusión es el mismo de la Sección 3 de este apéndice.

## 13. Test hipergeométrico por ventana mensual

El test hipergeométrico principal (Sección 7) usa la tasa marginal promedio de cada
institución sobre los 24 meses completos como baseline de independencia. Si dos
instituciones comparten un pico estacional por razones administrativas no relacionadas
entre sí (por ejemplo, Operación Renta en abril), su co-ocurrencia agregada podría verse
artificialmente enriquecida sin que exista una dependencia real entre ambas.

**Diseño.** En vez de excluir los meses de pico, se recalcula el baseline de independencia
hipergeométrico dentro de cada mes, usando la marginal de cada grupo en ese mes específico
como denominador. Si dos servicios solo comparten un pico de calendario, sus marginales de
ese mes ya son altas, el conteo esperado bajo independencia ya es alto, y el lift de ese mes
debería acercarse a 1. Un par con dependencia administrativa genuina debería mostrar lift
sostenido a lo largo de varios meses, no solo concentrado en el mes de pico compartido.

**Alcance:** cuatro pares evaluados a nivel mensual —Servicio de Impuestos Internos↔AFC,
Servicio de Impuestos Internos↔Comisión para el Mercado Financiero, Poder Judicial↔Ministerio
Público (par ya reclasificado como efecto de hub, usado como control negativo), y Registro
Social de Hogares↔FUAS a nivel de servicio individual (el pathway ancla, usado como control
positivo).

**Resultado.** Para los cuatro pares, el lift mensual se mantiene por encima de 1 en los 24
meses sin excepción (mínimo 1,3×-7,2× según el par), y sistemáticamente por encima —no por
debajo— del lift agregado ya reportado en el cuerpo del artículo: para Poder Judicial↔Ministerio
Público el agregado es 2,39× mientras que el mensual oscila entre 7,2× y 9,9×. Para
Servicio de Impuestos Internos↔AFC sí hay una caída real en abril (2,22×-2,32× frente a
4×-6× el resto del año) —el efecto de Operación Renta es real y detectable— pero el lift de
abril nunca se acerca a 1. El test agregado del artículo no está inflado por estacionalidad
compartida; si acaso es conservador respecto del detalle mensual. Este análisis fue el que
condujo al hallazgo de la migración Registro Social de Hogares→Ventanilla Única Social
documentado en la Sección 11.3.

## 14. Extensión sistemática de la clasificación de pathway

La Sección 4.6 del artículo parte de 87.465 pares de servicios con soporte≥50 (Sección 3.2
del artículo). Sobre ese universo se evalúan los primeros cuatro criterios computables del
estándar de cinco criterios (soporte, significancia estadística, estabilidad temporal,
dirección≥70%), excluyendo pares entre dos servicios de la misma institución. El resultado:
5.113 pares de servicios que superan los cuatro criterios computables y quedan pendientes
del quinto (fuente administrativa verificada).

Estos 5.113 pares se agregan en 3.136 pares institución-institución distintos y se
clasifican íntegramente —no solo el subconjunto de mayor soporte— en cinco categorías
mecánicas, sin búsqueda de fuente administrativa por par (esa búsqueda exhaustiva excede el
alcance razonable de este estudio; ver Sección 15 para el subconjunto que sí se investigó
con fuente oficial real):

| Categoría | Pares | Soporte total |
|---|---:|---:|
| Clúster ACEPTA ya confirmado | 126 | 7,6 millones |
| Dominado por un pathway del Registro Social de Hogares ya confirmado | 1 | 1,2 millones |
| Artefacto de lanzamiento de Ventanilla Única Social | 90 | 12,9 millones |
| Efecto de tasa base (lift máximo <1,5× entre instituciones grandes) | 570 | 19,5 millones |
| Candidato estadístico sin verificación individual | 2.347 | 42,0 millones |

La categoría de "efecto de tasa base" no es una particularidad de este dataset: refleja la
dependencia conocida del lift respecto del tamaño de los marginales, una limitación formal
de esa medida y no un artefacto de este pipeline (Tan, Kumar & Srivastava, 2004; Sección 2.2
del artículo).

**Tabla A.1** (citada en la Sección 4.6 del artículo) es la clasificación completa de los
3.136 pares institución-institución con su categoría asignada, publicada íntegra como
`resultados/pipeline_actual/multimes/candidatos_institucion_clasificados.csv` en este
repositorio (columnas: institución de origen, institución de destino, soporte total, número
de pares de servicio agregados, lift máximo, categoría).

## 15. Validación en dirección inversa: cruce contra el catálogo CPAT

El Catálogo de Procedimientos Administrativos y otras Tramitaciones (CPAT) documenta, para
cada procedimiento administrativo, el medio utilizado para obtener cada dato o documento
que un organismo exige de otro: interoperabilidad electrónica automática, intercambio
manual entre organismos, o "no hay interoperabilidad, se solicita a sus usuarios(as)" — este
último caso es el único que debería dejar rastro conductual en ClaveÚnica, porque es la
ciudadanía quien debe obtener el documento de un organismo y presentarlo en otro.

De 2.761 registros de ese tipo en el catálogo CPAT nivel central, cruzados contra su nómina
oficial de instituciones responsables, se identifican 651 dependencias interinstitucionales
documentadas oficialmente, correspondientes a 2.021 procedimientos individuales. Cruzadas
contra la red institucional de co-uso (Sección 10), 469 tienen algún correlato observable en
los logs (soporte mayor a cero); 168 no tienen marginal resoluble (institución no mapeada en
los logs, mismo fenómeno de brecha de catálogo documentado en la Sección 18); 14 tienen
ambas instituciones en los logs pero soporte cero.

Entre los pares con soporte≥500, tres muestran alta dirección esperada y buen soporte:
ChileCompra→Dirección de Contabilidad y Finanzas (92% de dirección esperada),
Dirección General del Territorio Marítimo→Servicio Nacional de Pesca y Acuicultura (69%), y
Gendarmería de Chile→DIPRECA (68%). El segundo caso es la convergencia más fuerte del
estudio: había sido identificado de forma completamente independiente por la búsqueda
estadística ascendente (Sección 14) a nivel de servicio individual (lift 63,16×) — dos
métodos independientes, uno ascendente desde el patrón estadístico y otro descendente desde
un registro oficial, convergiendo en el mismo hallazgo sin haberse buscado mutuamente.

También se observa el patrón opuesto: varias dependencias documentadas entre instituciones
de previsión de las fuerzas armadas y de orden muestran lift alto pero dirección observada
débil o invertida (9%-32%) respecto de lo esperado. Esto no invalida la dependencia
documentada —sigue siendo una obligación legal real—; ilustra que el co-uso agregado a
nivel institución puede no aislar la secuencia de un procedimiento específico cuando dos
instituciones sostienen múltiples relaciones administrativas simultáneas (Sección 5.3 del
artículo).

Los tres pares limpios se promovieron a "Pathway" en la Tabla 1 del artículo tras calcular
su estabilidad trimestral (Sección 22 de este apéndice).

## 16. Triaje del resto de los candidatos con mayor soporte

Dentro de la categoría "candidato estadístico sin verificación individual" (2.347 pares,
Sección 14), se investigaron con fuente oficial real los de mayor soporte agregado que no
calzaban ya en un patrón explicado. Ninguno se sostuvo como pathway: Seguro de Cesantía
(AFC)→FONASA (2,1 millones de personas, lift 3,46×) resultó ser una opción disponible al
quedar cesante, no una obligación; Crédito con Aval del Estado→FONASA (367.557, lift 10,49×)
y Dirección Nacional del Servicio Civil→FONASA (357.572, lift 8,10×) no tienen fuente que
los vincule más allá de la obligación general de afiliación a salud que aplica a cualquier
trabajador; Servicio de Impuestos Internos→Carabineros de Chile (514.479, lift 4,26×) tiene
una fuente parcial real (empresas de seguridad privada requieren certificado del Servicio de
Impuestos Internos para acreditarse ante Carabineros) pero esa fuente no explica el
componente de mayor soporte del par agregado.

Este resultado negativo —que ni siquiera los candidatos de mayor volumen y mayor lift dentro
de un conjunto ya pre-filtrado estadísticamente resisten la verificación administrativa— es
evidencia a favor del quinto criterio del estándar de pathway.

## 17. Verificación de candidatos con el Registro Social de Hogares como puerta de entrada

Dentro del subconjunto de mayor soporte de los 5.113 candidatos (soporte≥1.000: 3.351
pares), el Registro Social de Hogares precede con dirección y lift consistentes al subsidio
habitacional del Ministerio de Vivienda y Urbanismo, al Subsidio Familiar Automático, y a
programas de emprendimiento vulnerable de Sercotec. Cada uno se verificó contra fuente
oficial antes de reportarse como hallazgo: el Ministerio de Vivienda y Urbanismo y
ChileAtiende confirman que la inscripción en el Registro Social de Hogares es requisito base
para los subsidios habitacionales correspondientes; la plataforma del Subsidio Familiar
Automático confirma que este se asigna de forma automática según la calificación
socioeconómica del Registro Social de Hogares; y las bases de convocatoria de Sercotec
confirman que el programa exige explícitamente un tramo de calificación del Registro Social
de Hogares.

## 18. Catálogo institucional: entradas de prueba y servicios sin institución resoluble

Dos problemas del catálogo institucional son la misma limitación vista desde dos lados.

**(a) Entradas de prueba presentes en la red institucional.** El catálogo trae 33
`client_id` con sector "Test" declarado explícitamente. Cuatro de esas entradas de prueba
están presentes como nodos en la red institucional de 330 nodos, con tráfico real aunque
pequeño: un nodo genérico "Institución" (7.475 usuarios, 0,053% de N), un nodo genérico
"Municipios" (1.219 usuarios, 0,009%), un nodo de aplicación de prueba "Simple" (380
usuarios, 0,003%), y un nodo residual con 3 usuarios. Su efecto sobre los resultados
publicados es despreciable por volumen, pero son ruido que no debería estar en una red que
se describe como institucional.

**(b) Los 6.307 pares de servicios (7,2% de 87.465) sin institución resoluble.** No son una
cola larga de servicios menores: provienen de solo 61 `client_id` distintos, y tres de ellos
concentran el 89% del soporte faltante. El mayor de ellos aparece en 717 pares con
11.426.910 de soporte acumulado, y su marginal propio es de 761.267 usuarios distintos —
5,41% de todo el universo de usuarios— sin siquiera nombre registrado en el catálogo. Los
tres primeros suman 1.402.748 usuarios, casi el 10% del universo. En conjunto, los pares no
resolubles representan 24.220.404 de soporte, 2,69% del soporte total del universo de
tests; su distribución de soporte es sistemáticamente menor que la de los pares resueltos
(mediana 207 frente a 318; percentil 95 de 9.350 frente a 25.143).

Esta omisión no es aleatoria: el catálogo institucional es una curaduría manual, no un
registro exhaustivo generado por el sistema. Los servicios que faltan no faltan por ser
pequeños —el mayor de todos equivale al 5,4% del universo de usuarios— sino por no haber
pasado por ese proceso de curaduría. Es la misma brecha documentada en la Sección 3.4 del
artículo entre los dos mapeos institucionales (457 instituciones con tráfico real frente a
375 en el catálogo curado, de las cuales 330 quedan conectadas tras el umbral de
soporte≥50). El universo canónico citado en el cuerpo del artículo es el completo (87.465
pares); la variante conservadora que excluye estos 6.307 pares (81.158) se reporta solo en
nota al pie, y el control de multiplicidad de comparaciones (Sección 26) confirma que la
conclusión sustantiva no depende de esta elección.

## 19. Transformación de peso a distancia en la betweenness

Los porcentajes de intermediación publicados en la Sección 4.3 del artículo dependen
enteramente de esta decisión, no documentada anteriormente en el cuerpo del texto.

`networkx.betweenness_centrality` interpreta el argumento de peso como distancia (costo de
recorrer la arista), no como fuerza del vínculo. Como el peso de la red de enriquecimiento
es el lift —donde un valor más alto significa un vínculo más fuerte—, pasarlo directamente
habría hecho que los caminos más cortos prefirieran las aristas de menor lift, exactamente
lo contrario de lo buscado. La transformación usada convierte el peso a distancia mediante
**1/log(1+lift)**; la compresión logarítmica atenúa el efecto de los valores extremos de
lift (que van de 1,2 a aproximadamente 5.275, un rango de más de 4.000×) sobre la longitud
de los caminos más cortos.

Se verificó la sensibilidad de la conclusión a esta elección recalculando la betweenness con
tres transformaciones alternativas sobre la misma red:

- **1/log(1+lift)** (la usada): reproduce los porcentajes publicados (Subsecretaría General
  de la Presidencia 11,82% frente al 11,8% publicado; Comisión para el Mercado Financiero
  3,217% frente a 3,2%, y así para las cinco instituciones de mayor intermediación).
- **1/lift** (recíproco simple): conserva la conclusión cualitativa —ninguno de los cinco
  puentes coincide con los seis nodos de mayor volumen (0/15 de solapamiento en el top-15)—
  aunque desplaza los porcentajes exactos y altera el orden interno del ranking.
- **Lift crudo como distancia** (la interpretación incorrecta que se buscaba descartar):
  habría colapsado el hallazgo central, con 14 de 15 nodos de solapamiento entre hubs y
  puentes. Es decir, si ese error hubiera estado presente en el cálculo, la afirmación "hub
  ≠ puente" del artículo no se sostendría.

El valor de este contraste no es solo confirmar la ausencia de error: muestra que la
conclusión es sensible a una decisión de implementación que no es obvia, y por eso se
declara explícitamente.

## 20. Modelos nulos de modularidad: diseño, corrección de un error y resultado

Los valores de modularidad de la red institucional se contrastan contra tres modelos nulos
de 1.000 réplicas cada uno: reconexión que preserva la secuencia de grados
(`double_edge_swap`), modelo de configuración (binario por diseño), y permutación de los
pesos observados sobre la topología fija.

**Corrección de un error en el nulo de reconexión.** La función `networkx.double_edge_swap`
no traslada el atributo de peso a las aristas nuevas que crea al reconectar la topología; el
algoritmo de comunidades de Louvain y el cálculo de constraint de Burt resuelven un peso
faltante asignándole 1 sin advertencia alguna. Como la fracción de aristas afectada depende
de cuántos intentos de reconexión logran completarse, el efecto no era un binarizado limpio:
en la corrida original alcanzaba al 25,3% de las aristas en la red bruta y al 88,7% en la
red de enriquecimiento, dejando el resto con su peso real — una mezcla azarosa entre peso
real y peso unitario, no un modelo nulo bien definido.

La corrección redistribuye el multiset de pesos observado sobre la topología ya reconectada,
de modo que el nulo preserve secuencia de grados exacta y distribución de pesos exacta,
rompiendo deliberadamente solo la asociación entre ambas —la opción más informativa para la
pregunta sustantiva del artículo: ¿la estructura de la red depende de qué pesos están en
qué aristas? El efecto de la corrección no fue neutro: el z-score de la red bruta contra
este nulo pasó de +2,96 (con el error) a −77,8 (corregido), invirtiendo el signo de la
conclusión. Con el error, la lectura era "la red bruta es indistinguible de su nulo";
corregida, la lectura es "la red bruta tiene modularidad muy por debajo de lo que produce
cualquier reasignación azarosa de sus propios pesos" — un hallazgo sustantivo distinto, y
el que efectivamente se reporta en el artículo.

**Resultado, con la corrección aplicada:**

- **Red bruta** (Q observado = 0,0163 en la corrida puntual citada originalmente; ver
  Sección 24 sobre la media de 100 corridas): el nulo de reconexión corregido da media
  0,5397 (z=−77,8), prácticamente idéntico al nulo de permutación de pesos (media 0,5395,
  z=−79,2), que nunca tuvo este error y por tanto valida cruzadamente la corrección. Frente
  al modelo de configuración, z=−25,9. La red real queda muy por debajo de lo que produce
  cualquier reasignación azarosa de sus propios pesos sobre el mismo grado.
- **Red de enriquecimiento** (Q observado = 0,5483): el nulo de reconexión corregido da
  media 0,4875 (z=11,9, p empírico=0,000999, el máximo alcanzable con 1.000 réplicas), de
  nuevo casi idéntico a permutación de pesos (media 0,4885, z=11,4). Frente al modelo de
  configuración, z=234,3. La red real queda por encima de la reasignación azarosa.

Que los dos nulos metodológicamente independientes (reconexión corregida y permutación de
pesos) converjan tan de cerca en ambas redes es, en sí mismo, una verificación adicional de
que el resultado no es un artefacto de un modelo nulo particular.

**Por qué la red bruta tiene modularidad tan baja.** Las aristas incidentes a los seis
nodos de mayor volumen (Sección 4.2 del artículo) son solo el 9,9% de las 19.220 aristas de
la red bruta (1.911), pero concentran el 53,8% del peso total —un peso medio por arista 10,5
veces mayor que el resto de la red—, una configuración de estrella incompatible con una
partición modular. Esto es coherente con, y cuantifica en la misma dirección que, el
coeficiente de Gini de 0,956 ya reportado en la Sección 4.2 del artículo (ver también Sección
24 de este apéndice).

**Sensibilidad al umbral de lift** (1,1 a 3,0): la modularidad de la red de enriquecimiento
sube monótonamente de 0,5453 a 0,6239, sin quiebres — la modularidad alta no depende de la
posición exacta del umbral 1,2 usado en el artículo.

## 21. Medidas de agujeros estructurales de Burt

Se calculó constraint, effective size, efficiency y hierarchy por nodo, en ambas redes
(bruta y de enriquecimiento), en versión binarizada y ponderada. El constraint se verificó
desde cero contra la implementación de referencia de NetworkX, coincidiendo exactamente
(tolerancia 1e-9) en ambas redes y ambas versiones.

**Nota de implementación:** el effective size calculado con la fórmula original de Burt
(1992) diverge del que calcula `networkx.effective_size()` en 327 de 375 nodos en la versión
ponderada. Esto es esperado, no un error: NetworkX usa la simplificación de Borgatti para
redes no dirigidas, que se aparta de la formulación original de Burt precisamente cuando la
red egocéntrica es densa o ponderada —el caso de esta red—. El artículo reporta la
formulación original de Burt, la que corresponde a la cita, no la de NetworkX.

**Convergencia entre marcos, el resultado que sostiene la cita a Burt en la Sección 4.3 del
artículo.** Correlación de Spearman entre betweenness y constraint, calculada sobre los 330
nodos conectados de cada red, con intervalo de confianza al 95% por bootstrap (2.000
remuestreos):

- **Versión binaria** (topología pura, sin pesos): rho=−0,954 (red bruta) y rho=−0,970 (red
  de enriquecimiento), ambas con p<10⁻¹⁷³ — convergencia robusta y muy fuerte entre la
  centralidad de intermediación de Freeman y el constraint de Burt, el signo esperado por la
  teoría. Con effective size, la convergencia es incluso más fuerte: rho=+0,975 (bruta) y
  +0,990 (enriquecimiento).
- **Versión ponderada** (distancia=1/log(1+peso), la misma transformación de la Sección 19):
  la correlación es real pero más débil y sensible a la transformación de peso elegida —con
  el recíproco simple del peso, el signo en la red de enriquecimiento sale positivo, opuesto
  al esperado; con 1/log(1+peso) sale el signo correcto pero débil (rho=−0,140, p=0,011 en
  la red de enriquecimiento; no significativo en la red bruta).

Esta asimetría es un hallazgo de robustez, no una debilidad: la convergencia entre ambos
marcos es topológica, no depende de los pesos —consistente con la Sección 19, donde la
transformación de peso incorrecta habría colapsado por completo el hallazgo "hub ≠ puente"—.
Que la convergencia sobreviva en la versión binaria, la menos sensible a decisiones de
transformación, la hace más robusta, no menos.

**Lo que los datos de Burt no sostienen, reportado explícitamente.** El z-score de
constraint contra el nulo de reconexión (grado y distribución de pesos exactos preservados)
no es significativo para ninguno de los cinco puentes ni los seis hubs, en ninguna de las
dos redes (rango z=[−0,92, −0,004] en la red bruta; z=[−0,68, −0,58] en la de
enriquecimiento). Effective size, efficiency y hierarchy tampoco separan a los puentes de
los hubs: ambos grupos muestran valores igualmente altos de effective size/efficiency y
valores igualmente cercanos a cero de hierarchy, un patrón mecánico de tener grado extremo
(percentil 84-99,5 en ambas redes) más que una señal específica de posición de puente.

**Conclusión.** Los datos sostienen que constraint y betweenness convergen como marcos de
medición en esta red —evidencia de convergencia, no solo de analogía teórica—, lo suficiente
para que la referencia a Burt (1992) en la Sección 4.3 del artículo pase de interpretación a
medida verificada. Los datos no sostienen que los cinco puentes ocupen agujeros
estructurales de forma excepcional más allá de lo que su grado y la distribución de pesos de
la red ya predicen, ni los distinguen de los hubs de tráfico en ningún indicador de Burt más
allá de la betweenness misma. Aun si el constraint bajo fuera excepcional, tampoco sería
evidencia de la ventaja competitiva que la teoría de Burt postula: esa teoría asume actores
que eligen estratégicamente sus propios vínculos compitiendo por ventaja, mientras que las
aristas de esta red son conducta ciudadana agregada, no vínculos elegidos por las
instituciones. El lenguaje que usa el artículo ("puentes estructurales") es correcto y
deliberadamente no se refuerza hacia un lenguaje de "ventaja".

## 22. Estabilidad trimestral de los pares CPAT confirmados

Único criterio que le faltaba a los tres pares del cruce CPAT-logs (Sección 15) para
promoverse a "Pathway" en la Tabla 1 del artículo: estabilidad trimestral, calculada sobre
8 trimestres (2024-T1 a 2025-T4).

Los tres pares están presentes en los 8 trimestres, sin el patrón de "soporte cero antes de
una fecha y masivo después" que excluyó a Ventanilla Única Social del análisis de pares
nombrados a mano (Sección 11.3), pero con un patrón que requirió lectura cuidadosa:

- **ChileCompra→Dirección de Contabilidad y Finanzas**: limpio sin reservas. Soporte de
  **342 a 835 personas por trimestre** (suma exacta 4.329, coincide con el agregado
  publicado en la Tabla 1 del artículo). El trimestre de menor soporte, 342 personas
  (2024-T1), es el grupo más pequeño reportado en cualquier parte del artículo o de este
  apéndice, y sostiene el piso real de divulgación declarado en el Anexo A del artículo.
  Dirección dominante en el mismo sentido en los 8 trimestres, sin excepción: 69,5%
  (2024-T1) → 86,0% → 91,2% → 91,3% → 95,2% → 96,1% → 96,4% → 98,9% (2025-T4).
  Fortalecimiento monótono, nunca se revierte.
- **Dirección General del Territorio Marítimo→Servicio Nacional de Pesca y Acuicultura**:
  soporte de 858 a 2.168 personas por trimestre (suma 9.833). Dirección: 37,9% (2024-T1,
  revertida — ese trimestre la dirección dominante fue la opuesta a la esperada) → 58,1% →
  68,9% → 69,1% → 77,0% → 84,9% → 89,5% → 92,7% (2025-T4). Desde 2024-T2 en adelante,
  consistente y con fortalecimiento monótono.
- **Gendarmería de Chile→DIPRECA**: soporte con caída monótona de 7,3× a lo largo del
  período (5.893 en 2024-T1 a 810 en 2025-T4; suma total 15.560). Dirección: 39,5%
  (2024-T1, revertida) → 69,2% → 76,8% → 88,0% → 91,1% → 95,4% → 96,7% → 99,3% (2025-T4).
  Mismo patrón que el par anterior: revertida solo en el primer trimestre, luego
  consistente y creciente.

**Interpretación.** Los dos pares con menor volumen institucional muestran dirección
revertida específicamente en 2024-T1 —el primer trimestre de todo el período de estudio— y
en ningún otro. Este patrón se distingue del de "posible lanzamiento" que excluyó a
Ventanilla Única Social (Sección 11.3): ahí el patrón era soporte cero antes de una fecha y
masivo después; aquí el soporte está presente y es del mismo orden de magnitud en los 8
trimestres, lo único que cambia es la dirección. La hipótesis más plausible —explícitamente
no verificada con fuente oficial, se reporta como hipótesis y no como hecho— es que 2024-T1
recoja un lote de casos históricos registrado por integraciones institucionales recién
habilitadas a *logging*, sin preservar el orden cronológico real de los trámites
subyacentes; ambos pares corresponden a instituciones de menor tráfico que ChileCompra,
consistente con integraciones más recientes o más pequeñas siendo más susceptibles a este
efecto de arranque.

## 23. Coeficiente de Gini: fórmula y verificación

El coeficiente de Gini de 0,956 reportado en la Sección 4.2 del artículo se calcula con la
fórmula estándar basada en la diferencia media absoluta, sobre el recuento de eventos de
cada uno de los 1.266 servicios del período completo (544.735.871 eventos, 2024-2025),
ordenados de menor a mayor volumen:

    G = (2 · Σ i·x_i) / (n · Σ x_i) − (n+1)/n

- **G = 0,9559** sin corrección muestral — reproduce el 0,956 publicado.
- G = 0,9566 con la corrección n/(n−1), que no se aplica en el artículo.

Los dos puntos de la curva de Lorenz citados en el mismo párrafo del artículo se verifican
contra la misma fuente: el 50% de los servicios de menor volumen acumula 0,05% de los
eventos, y el 1% superior (poco más de una decena de servicios) acumula 60,2%. Puntos
adicionales, no reportados en el cuerpo del artículo: el 90% inferior acumula 4,65% de los
eventos, y el 99% inferior, 39,85%.

## 24. Umbral de consistencia direccional ≥70%

A diferencia de los umbrales de soporte y lift —fijados ex ante y sometidos a un análisis de
sensibilidad formal (Sección 28 de este apéndice)—, el umbral de dirección ≥70% (criterio 4
del estándar de pathway, Sección 2.2 del artículo) no tiene fuente en la literatura ni
análisis de sensibilidad propio. Se declara en el cuerpo del artículo como lo que es: una
convención de este estudio, fijada ex ante para exigir una mayoría clara sin llegar a
requerir unidireccionalidad casi total.

## 25. Control de multiplicidad de comparaciones

El pipeline secuencial realiza un test hipergeométrico exacto por cada uno de los 87.465
pares de servicios con soporte≥50, seleccionando candidatos a pathway a partir de ese
universo completo — no de un subconjunto ya cribado. Esta sección documenta el control de
tasa de falso descubrimiento que responde a la objeción evidente de que, con decenas de
miles de tests, una fracción de los "hallazgos" podría ser ruido acumulado por comparaciones
múltiples.

**Correcciones aplicadas, sobre el universo completo de 87.465 tests:**

| Criterio | Sobreviven | % de 87.465 |
|---|---:|---:|
| p<0,05 sin corregir | 75.396 | 86,2% |
| Benjamini-Hochberg q=0,05 | 75.252 | 86,0% |
| Benjamini-Hochberg q=0,01 | 73.786 | 84,4% |
| Benjamini-Hochberg q=0,001 | 71.893 | 82,2% |
| Benjamini-Yekutieli q=0,05 (no asume independencia) | 73.018 | 83,5% |
| Bonferroni (referencia extrema) | 66.704 | 76,3% |

**Resultado sustantivo:** de los pares que superan los cuatro criterios computables del
artículo, ninguno se pierde al aplicar cualquiera de las tres correcciones —la caída es de
0,00% con Benjamini-Hochberg q=0,05, q=0,01 y Benjamini-Yekutieli q=0,05 por igual—. Los seis
pathways de la Tabla 1 del artículo que pertenecen a este universo (Registro Social de
Hogares→FUAS, →MINVU, →Subsidio Familiar Automático, →Sercotec, AFC→Bolsa Nacional de
Empleo, Milicenciamedica.cl→SUSESO) sobreviven los tres ajustes, todos con p=0 exacto (por
subyacer al límite de representación de punto flotante) y posiciones de ranking entre 14 y
440 de 87.465. El control de multiplicidad no cambia ninguna conclusión sustantiva del
artículo, pero se documenta en vez de asumirse.

**Universo conservador** (81.158 pares con institución resoluble en ambos lados, Sección
18): el mismo procedimiento aplicado a este universo reproduce exactamente los 5.113
candidatos publicados en la Sección 4.6 del artículo, que sobreviven íntegros a las tres
correcciones (caída de 0,00%), con rangos entre 14 y 428 de 81.158. La proporción de
supervivientes es prácticamente idéntica a la del universo completo en todos los criterios,
lo que confirma que la elección entre uno y otro no es sustantiva sino de encuadre.

**Sensibilidad de Benjamini-Hochberg q=0,05 al umbral de soporte** (universo completo):

| Umbral de soporte | Tests | Sobreviven BH q=0,05 |
|---:|---:|---:|
| ≥50 | 87.465 | 75.252 |
| ≥100 | 68.221 | 59.836 |
| ≥200 | 52.249 | 46.577 |
| ≥500 | 35.565 | 32.273 |
| ≥1.000 | 25.959 | 23.825 |

## 26. Bimodalidad de los p-valores y por qué no se reporta el estimador π₀ de Storey

El test hipergeométrico del pipeline se aplica en cola superior — interesa detectar
enriquecimiento, no empobrecimiento (Sección 7). La consecuencia directa es que la
distribución de p-valores resultante es bimodal: los pares enriquecidos se acumulan cerca de
0 (17,13% del universo completo cae en p=0 exacto por subyacer al límite de representación
de punto flotante) y los pares empobrecidos —que son mayoría, dado que la mayor parte de las
combinaciones de servicios coocurren menos de lo que sus marginales predicen— se acumulan
cerca de 1.

Por esta razón no se reporta el estimador π₀ de Storey y Tibshirani (2003), aunque se
calculó (0,9022 en el universo completo; 0,9050 en el universo conservador): ese estimador
supone que los p-valores bajo la hipótesis nula se distribuyen uniformemente en [0,1], y
estima π₀ a partir de la densidad observada cerca de p=1. Bajo un test de una cola aplicado
a un universo donde el empobrecimiento es el caso mayoritario, esa densidad cerca de 1 no
representa "nulos verdaderos" en el sentido que el método supone, sino una categoría
sustantivamente distinta (pares con coocurrencia por debajo de lo esperado). El supuesto no
se cumple y el estimador no es interpretable en este contexto. Las correcciones que sí se
reportan (Benjamini-Hochberg, Benjamini-Yekutieli, Bonferroni) no dependen de ese supuesto.

El histograma de p-valores que muestra la bimodalidad directamente está publicado como
`resultados/pipeline_actual/multimes/fdr_completo/fig_histograma_pvalores.png` (y su
equivalente para el universo conservador) en este repositorio.

## 27. La modularidad reportada es una media de 100 corridas

El algoritmo de Louvain no es determinista: depende de la semilla aleatoria y del orden de
recorrido de los nodos. Los valores de modularidad de la Sección 4.3 del artículo
corresponden a medias sobre 100 corridas variando ambas cosas, con su dispersión declarada:

- Red de enriquecimiento: Q = 0,5482 ± 0,0006 (prácticamente invariante entre corridas).
- Red bruta: Q = 0,0226 ± 0,0034, rango observado [0,0142, 0,0248].
- Red de control (mismo número de aristas que la de enriquecimiento, seleccionadas por
  soporte en vez de por lift): Q = 0,0217 ± 0,0037.

El valor Q=0,0163 usado para la red bruta en una versión anterior de este análisis (Sección
20) correspondía a una corrida única que resultó estar en el extremo bajo de esta
distribución, no en su centro; se declara explícitamente en vez de sustituirse en silencio.
La conclusión cualitativa no cambia: incluso el máximo observado en 100 corridas (0,0248) es
modularidad trivial frente al 0,548 de la red de enriquecimiento.

Sobre 100 corridas con semillas distintas, ninguna partición de ninguna de las dos redes
presenta comunidades internamente desconectadas — verificación de una limitación conocida
del algoritmo de Louvain, que motivó el desarrollo posterior de Leiden (Traag et al., 2019).
La partición de la red de enriquecimiento resulta estable (índice de Rand ajustado de 0,91
entre corridas; 14 de 330 nodos cambian de comunidad en más del 20% de las corridas, casi
todos periféricos), y Leiden reproduce esa partición con acuerdo alto (índice de Rand
ajustado de 0,95).

## 28. Sensibilidad de los umbrales de soporte y lift (Tabla 4 del artículo)

Los umbrales de soporte≥50 y lift≥1,2 se fijaron ex ante, antes de examinar los resultados.
Para verificar directamente que las conclusiones no dependen de esa elección específica, se
recalculó, sobre los mismos 87.465 pares de servicios, cuántos pares satisfacen los cuatro
criterios computables (soporte, lift, dirección≥70%, estabilidad completa) bajo una grilla
de umbrales alternativos:

| Soporte↓ / Lift→ | ≥1,1 | ≥1,2 | ≥1,5 | ≥2,0 | ≥3,0 |
|---|---:|---:|---:|---:|---:|
| ≥25 | 6.230 | 5.902 | 4.625 | 2.890 | 1.385 |
| ≥50 (usado) | 6.230 | 5.902 | 4.625 | 2.890 | 1.385 |
| ≥100 | 6.230 | 5.902 | 4.625 | 2.890 | 1.385 |
| ≥200 | 6.229 | 5.901 | 4.624 | 2.889 | 1.385 |
| ≥500 | 5.286 | 4.994 | 3.876 | 2.337 | 1.098 |
| ≥1.000 | 4.038 | 3.804 | 2.898 | 1.681 | 746 |

El umbral de soporte es prácticamente irrelevante entre 50 y 200 (el conteo de candidatos
varía en menos de 0,1% en ese tramo) y solo cae de forma apreciable a partir de soporte≥500.
El umbral de lift, en cambio, mueve el conteo de forma continua y sustancial (de 6.230 a
1.385 candidatos entre lift≥1,1 y lift≥3,0): es la dimensión que realmente determina cuántos
candidatos necesitan verificación con fuente administrativa, no el soporte.

De los 12 pathways confirmados en la Tabla 1 del artículo, ninguno está cerca de los
umbrales usados de forma uniforme: el más expuesto en soporte es
ChileCompra→Dirección de Contabilidad y Finanzas (4.329, 86 veces el umbral de 50), que
caería con un umbral de soporte≥5.000; el más expuesto en lift es el extremo inferior del
rango de ACEPTA (1,30×, en uno de sus cuatro destinos), que caería con un umbral de
lift≥1,5. Los tres pathways confirmados vía CPAT y el par confirmado por la fase exploratoria
previa al pipeline sistemático no pertenecen a este universo de 87.465 pares —se validaron
por métodos distintos (Secciones 15 y 22)— y sus valores de lift ya publicados se leen
directamente contra las columnas de esta tabla sin necesidad de recalcularlos.

## 29. Análisis extendido de asociación sin ventana temporal (Tabla A.2)

La minería de asociación sin ventana temporal (FP-Growth) por sí sola sobreestima
sistemáticamente cuántos pares son pathways genuinos, precisamente porque no distingue una
ráfaga administrativa puntual de una dependencia secuencial real (Sección 4.5 del artículo).
Para cuantificar esa sobreestimación, se extendió el mismo baseline hipergeométrico a los 60
pares de mayor soporte bruto en el output completo de FP-Growth.

**Hallazgo principal.** Un clúster de servicios vinculados al empleo formal —Dirección del
Trabajo (Portal MiDT), AFC, Servicio Nacional de Capacitación y Empleo (SENCE), Comisión
para el Mercado Financiero y FONASA— muestra lift consistentemente elevado entre sí
(1,52×-1,90×) pese a tener marginales individuales grandes (21%-38% cada uno), sugiriendo un
clúster genuino de servicios de empleo y previsión que se usan juntos más de lo que su
tamaño individual explicaría por separado. Al someter cada uno de los seis pares de ese
clúster al estándar de cinco criterios, cuatro resultan estadísticamente indistinguibles de
simétricos y los dos restantes muestran una dirección real aunque moderada, sin que haya
sido posible identificar una norma específica que la explique. Ninguno de los seis satisface
el quinto criterio (fuente administrativa verificada); esta es la razón por la que el
artículo trata la asociación sin ventana como validación secundaria, no como método
primario.

Un segundo hallazgo del mismo análisis: Registro Civil (con 69,8% de marginal) es, por
lejos, el hub individual más grande de todo el ecosistema —mayor que FONASA, el Ministerio
de Desarrollo Social o el Servicio de Impuestos Internos (43%-50% cada uno)—, y prácticamente
todos sus pares de mayor soporte bruto muestran lift cercano a 1 o incluso por debajo de 1:
el par Subsidio Eléctrico↔Registro Civil tiene lift 0,84×, la primera y única asociación
negativa —co-ocurrencia menor a la esperada por puro azar— encontrada en todo este estudio.

**Estado de los datos de esta sección.** El script que calculó originalmente el top-60
completo (lectura de una sola pasada del archivo de salida de FP-Growth, construcción de un
diccionario de soportes marginales y un min-heap de los 60 pares de mayor soporte bruto) fue
un análisis exploratorio de una sola sesión y no quedó guardado como archivo versionado; solo
su resultado quedó documentado en el cuerpo del artículo y en esta sección. Quien necesite
reproducir exactamente el top-60 y sus lifts puede reconstruir la lógica descrita arriba
sobre el archivo de salida completo de FP-Growth publicado en
`resultados/exploratorio/lcm/` y `computador_aws/fpgrowth/` de este repositorio (2.304.676
itemsets totales, de los cuales 9.137 son pares de exactamente dos servicios; N=14.089.176,
el universo propio de ese pipeline, distinto de los 14.081.161 usados en el resto del
artículo por diferencias de filtrado de bots entre pipelines — no deben mezclarse ambos
valores de N al reproducir estas cifras). La tabla completa de 60 filas no se reconstruyó
retroactivamente para este documento porque hacerlo exigiría recrear una selección top-k
sobre el archivo de origen sin el script exacto que se usó originalmente, con riesgo de
introducir pequeñas diferencias de criterio de desempate; se prefirió declarar el estado de
los datos con honestidad en vez de presentar una reconstrucción no verificada como si fuera
la original.

## 30. Cohortes etarias aproximadas por proxy de RUN: detalle completo

El Anexo B del artículo reporta este análisis como exploratorio y secundario. Esta sección
documenta el detalle completo de su verificación.

**Fórmula del proxy** (Villena, 2019): AñoNacimiento ≈ (RUN × 3,336×10⁻⁶) + 1932,26, usada
para generar seis cohortes (A: 14-18, B: 19-26, C: 27-35, D: 36-45, E: 46-59, F: 60+).

**Fecha oficial de asignación de RUN al nacimiento, verificada a solicitud del autor.** El
Servicio de Registro Civil e Identificación confirmó, en respuesta a una solicitud de acceso
a información pública (Carta STSI N° 2124, 24 de agosto de 2026), que la asignación de RUN
en el mismo acto de inscripción de nacimiento rige para las personas inscritas a partir del 1
de enero de 1982, como parte de un proceso de mecanización que sustituyó el RUN de 13
dígitos por el de 9 dígitos entonces vigente. Con anterioridad a esa fecha, el número se
otorgaba cuando la persona —menor o mayor de edad— solicitaba por primera vez su cédula de
identidad, mediante un procedimiento de clasificación dactiloscópica (Clave Chilena de 14
valores) sobre una ficha y tarjeta índice remitidas al nivel central. La distancia entre
nacimiento y asignación de RUN antes de 1982 es, por tanto, variable y puede alcanzar
décadas.

**Fragmentación de la numeración antes de 2000, un segundo mecanismo de error no limitado a
un extremo de la distribución.** El mismo Servicio confirmó que, con anterioridad al año
2000, los números de cédula se asignaban de forma local por oficina —cada oficina con su
propia numeración—, y que la asignación se unificó en una secuencia nacional única recién a
partir del RUN 21 millones. Bajo la fórmula del proxy, ese umbral corresponde a un año de
nacimiento en torno a 2002: en el período analizado por este estudio, únicamente la cohorte
A y la fracción más joven de la cohorte B quedan dentro del tramo de numeración unificada a
nivel nacional; para las cohortes restantes, el proxy opera sobre una serie que no fue
nacionalmente correlativa, con una dispersión que los datos disponibles no permiten
cuantificar. El Servicio señaló además que, desde el año 2000, existen dos rangos de
numeración (en torno a 14 y 20 millones) reservados para trámites de asignación familiar,
que bajo la fórmula del proxy se datan en 1979 y 1999 respectivamente, con independencia de
la edad real de quien recibió el número.

**Sesgo migrante, cuantificado en la Sección 9 de este apéndice.** Los números entregados
por primera vez para cualquier trámite —incluidas las primeras filiaciones de personas
extranjeras— provienen de la misma numeración que los nacimientos, de modo que un residente
extranjero que regulariza su situación en un año dado recibe un RUN cercano al de un recién
nacido chileno de ese mismo año.

**Ninguna cohorte queda libre de error estructural**: las más jóvenes por la contaminación
migratoria, las restantes por la numeración local previa a la unificación de 2000. Esta es
la razón por la que el análisis se reporta como exploratorio y no sostiene ninguna conclusión
del cuerpo del artículo — no por falta de esfuerzo de validación, sino porque la validación
misma revela una estructura de error de tres capas que ningún ajuste retrospectivo puede
corregir sin datos de edad independientes, no disponibles para este estudio.

**Validación contra proyección poblacional y patrón de servicios por cohorte**: ver Secciones
8 y 9 de este apéndice para el detalle completo de la validación externa y la cuantificación
del sesgo migrante, respectivamente.

## 31. Scripts del pipeline y material suplementario

El código completo del pipeline vigente y de las fases exploratorias descartadas antes de
llegar a él (Apriori/Eclat, FP-Growth, LCM en sus distintas iteraciones), junto con los
resultados agregados que cada uno produjo, están publicados en este mismo repositorio:

- `src/pipeline_actual/` — el pipeline que produjo los resultados de este artículo.
- `src/figuras/` — los scripts que generan las figuras del artículo.
- `src/exploratorio/` — las fases evaluadas y descartadas.
- `resultados/` — salidas agregadas por par de servicio o institución (soporte, dirección,
  lag, lift, p-valor, q-valor), nunca por persona. Ver `DATOS.md` en la raíz del
  repositorio para la política completa de qué se publica y por qué.

Ningún archivo a nivel de persona se publica en ningún caso, incluidos los que en el entorno
de desarrollo se llamaban "muestra" o "diagnóstico" — son muestras de eventos individuales,
no agregados, y quedan fuera de este repositorio por diseño.

## Referencias citadas en este apéndice y no incluidas en la bibliografía del artículo

Storey, J. D., & Tibshirani, R. (2003). Statistical significance for genomewide studies.
*Proceedings of the National Academy of Sciences, 100*(16), 9440–9445.
https://doi.org/10.1073/pnas.1530509100

Todas las demás fuentes citadas en este documento (Villena, 2019; Tan, Kumar & Srivastava,
2004; Traag et al., 2019; INE & SERMIG, 2024; Servicio de Registro Civil e Identificación,
2026) están en la Bibliografía del artículo.
