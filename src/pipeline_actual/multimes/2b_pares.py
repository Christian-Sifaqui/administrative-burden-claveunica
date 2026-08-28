"""
Camino A · fase multi-mes — Paso 2b: self-join de pares (AWS + DuckDB)
==========================================================================
Correr SOLO despues de revisar la salida de 2a_diagnostico.py y confirmar que
la estimacion del self-join es manejable con los recursos de la maquina. Si
se ajustaron PCTL_BOT / MAX_SERVICIOS_DISTINTOS_RUN en 2a, replicar el mismo
valor aca (deben coincidir).

Historial: la corrida del 2026-07-14 murio dos veces por OOM (sin traceback
de Python, el proceso simplemente aparecio "Killed"). Diagnostico: un unico
GROUP BY sobre las ~900M filas de pares_run puede llegar a tener hasta
~800.000 grupos distintos (1.266 servicios => hasta 1.266*1.265/2 pares
posibles); aunque cada grupo individual sea chico, mantener un estado de
agregacion (conteos + sketches de cuantil) para cientos de miles de grupos
SIMULTANEAMENTE ya alcanza varios GB, independiente de si se usa MEDIAN
exacto o approx_quantile. La solucion no es cambiar la funcion de agregacion,
sino nunca calcular todos los grupos a la vez.

Diseno nuevo: fases E/F/G ahora procesan pares_run EN FRANJAS (`por partes`),
particionando por hash(servicio_a || '#' || servicio_b) % N_SHARDS -- es
decir, por PAR (arista), no por servicio individual (nodo). Particionar por
servicio_a solo se probo y se descarto: como muchas plataformas tienen unos
pocos servicios "hub" (login, home, etc.) que aparecen como servicio_a en
cientos de pares distintos, TODAS esas filas caerian en la MISMA franja
(hash(hub) es fijo) sin importar cuantas franjas haya -- una franja se vuelve
cuello de botella mientras las demas terminan en segundos (particionamiento
de grafos por nodo de alto grado). Al hashear el PAR completo, los cientos
de aristas de un hub se reparten entre MUCHAS franjas distintas. Sigue siendo
correcto: como el self-join usa "a.servicio < b.servicio", cada par tiene un
(servicio_a, servicio_b) fijo -- todas sus filas comparten el mismo hash, caen
juntas en la misma franja -- las franjas son disjuntas, no se necesita
fusionar resultados parciales despues. Cada franja se agrega y se INSERTA
directo en las tablas finales, se hace commit, y se marca en `shards_procesados`
antes de pasar a la siguiente -- si la maquina muere a mitad de camino, se
pierde como maximo una franja (unos minutos de trabajo), no la corrida
completa. Volver a correr el script retoma desde la primera franja pendiente.

Fases:
  B. Filtro de bots (percentil de volumen total) + tope duro de servicios
     distintos por run (seguridad adicional para acotar el self-join).
  C. primera_fecha: MIN(fecha) por (run, servicio), solo runs no filtrados.
  D. Self-join de primera_fecha consigo misma (por run) -> tabla base
     `pares_run` (un registro por persona x par de servicios x lag). Se
     materializa UNA sola vez -- se reusa si ya existe de una corrida previa.
  R. run_grupo: cohorte etaria A-F + edad continua por run (atributo estable
     de la persona, calculado UNA vez, no por franja -- es chico, ~14-20M filas).
  E/F/G POR FRANJAS -- para cada franja de PAR de servicios (0..N_SHARDS-1):
     E. Agregado total -- soporte, direccion, lag en dias por par.
     F. Estabilidad trimestral por fecha_b (ALERTA A6).
     G. Cohorte etaria A-F + edad continua (ALERTA A8), join contra run_grupo.
     Formula de cohorte: https://github.com/fvillena/rut-a-edad, ya usada en
     el proyecto (Claude.md sec. 8 y fpgrowth2/run_rango_tablaA/tablaA_grupo_etario.py):
       F: rango_run 1-10  (60+)   | E: 10-14 (46-59) | D: 14-17 (36-45)
       C: rango_run 17-19 (27-35) | B: 20-22 (19-26) | A: 22-23 (14-18)
     Fronteras SOLAPADAS a proposito: un run con rango_run=14 cuenta en D y
     en E a la vez, no se fuerza a elegir. Proxy no validado externamente --
     tratar como exploratorio (ALERTA A8).
  H. Export a csv.

Requiere: pip install duckdb
"""

import zipfile
from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

# Mismas rutas que 1_ingesta.py -- para verificar que la ingesta termino antes de
# lanzar el self-join (la parte cara e irreversible en tiempo de este pipeline).
ZIP_PATHS = sorted(Path("/home/ubuntu/codigos/python/data").glob("cu_user_client-2026-*.zip"))

THREADS = 4
MEMORY_LIMIT = "10GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"

MIN_SOPORTE_PAR = 50               # minimo de runs con ambos servicios para reportar un par
MIN_SOPORTE_DESGLOSE = 20           # minimo para las tablas de trimestre/cohorte (mas fino que
                                     # MIN_SOPORTE_PAR a proposito -- cada sub-grupo tiene menos
                                     # soporte que el total -- pero NO fue revisado con el usuario,
                                     # ajustar si 20 no es un umbral razonable para estas tablas.
PCTL_BOT = 0.999                    # debe coincidir con 2a_diagnostico.py
MAX_SERVICIOS_DISTINTOS_RUN = 100   # debe coincidir con 2a_diagnostico.py

# Numero de franjas para procesar pares_run "por partes" (fases E/F/G), particionando
# por hash del PAR completo (servicio_a || servicio_b), no por servicio individual --
# asi los pares de un servicio "hub" (login, home, etc.) se reparten entre franjas en
# vez de concentrarse todos en una sola. Subir este numero (ej. a 100) si con 50 la
# maquina sigue muriendo por memoria.
N_SHARDS = 50

# Grupos etarios A-F desde rango_run -- formula de https://github.com/fvillena/rut-a-edad,
# ya usada en el proyecto (Claude.md sec. 8). Fronteras inclusive-solapadas.
GRUPOS_ETARIOS = [
    ("F", 1, 10, "60+"),
    ("E", 10, 14, "46-59"),
    ("D", 14, 17, "36-45"),
    ("C", 17, 19, "27-35"),
    ("B", 20, 22, "19-26"),
    ("A", 22, 23, "14-18"),
]

RANGO_RUN_MIN = min(lo for _, lo, _, _ in GRUPOS_ETARIOS)
RANGO_RUN_MAX = max(hi for _, _, hi, _ in GRUPOS_ETARIOS)


def _expandir_grupos_por_entero():
    """Expande GRUPOS_ETARIOS (rangos con fronteras solapadas) a una fila por
    cada entero de rango_run que cae en algun grupo -- universo cerrado y
    chico (RANGO_RUN_MIN..RANGO_RUN_MAX), permite reemplazar el JOIN por
    BETWEEN (evalua el rango fila a fila) por un JOIN de igualdad (hash join
    exacto, mucho mas rapido en DuckDB). Los enteros de frontera (ej. 14,
    dentro de D y de E a la vez) generan DOS filas -- se preserva el
    solapamiento intencional, no se pierde ninguna."""
    filas = []
    for v in range(RANGO_RUN_MIN, RANGO_RUN_MAX + 1):
        for grupo, lo, hi, edad in GRUPOS_ETARIOS:
            if lo <= v <= hi:
                filas.append((v, grupo, edad))
    return filas

# Formula continua de https://github.com/fvillena/rut-a-edad: anio_nacimiento = RUN * COEF + INTERCEPT.
# Verificada: aplicada a los limites de rango_run (RUN en millones, truncado) de GRUPOS_ETARIOS
# arriba con anio de referencia 2024, reproduce casi exacto las mismas fronteras A-F (ej.
# rango_run=14 -> edad 45.0, justo el limite D/E de 45/46). rango_run solo da el RUN truncado a
# millones, no el RUN exacto por persona -- se usa el punto medio del bucket (v+0.5)*1e6 como
# aproximacion de RUN real, asi que esto da MAS granularidad que los 6 grupos A-F pero sigue
# siendo aproximado a nivel de bucket de ~1 millon de RUN (~3.3 anios), no una edad individual
# exacta. La edad se calcula respecto al ANIO DE fecha_b (no un anio fijo): el dataset cubre
# 2024 Y 2025, y una persona nacida en un rango_run dado tiene una edad distinta segun si el
# evento ocurrio en 2024 o en 2025.
FORMULA_EDAD_COEF = 3.3363697569700348e-6
FORMULA_EDAD_INTERCEPT = 1932.26


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    r = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'resumen_run'").fetchone()
    if r[0] == 0:
        raise SystemExit("Falta la tabla 'resumen_run' -- correr 2a_diagnostico.py primero.")

    # Bloqueo duro (no solo advertencia, a diferencia de 2a_diagnostico.py): el self-join
    # es la parte cara e irreversible en tiempo de este pipeline, no vale la pena correrlo
    # sobre una ingesta a medio terminar solo para tener que repetirlo despues.
    if ZIP_PATHS:
        n_esperados = 0
        for outer_path in ZIP_PATHS:
            with zipfile.ZipFile(outer_path) as outer_zip:
                n_esperados += sum(1 for n in outer_zip.namelist() if n.lower().endswith(".zip"))
        n_procesados = con.execute("SELECT COUNT(*) FROM procesados").fetchone()[0]
        if n_procesados < n_esperados:
            raise SystemExit(
                f"Ingesta incompleta: {n_procesados:,} de {n_esperados:,} inner zips procesados. "
                f"Retomar 1_ingesta.py antes de correr el self-join."
            )
    return con


def fase_b_c_filtrado_y_primera_fecha(con):
    print("=" * 70)
    print("FASE B/C -- filtro de bots + primera_fecha por (run, servicio)")
    print("=" * 70)

    umbral_volumen = con.execute(f"""
        SELECT quantile_cont(n_eventos_totales, {PCTL_BOT}) FROM resumen_run
    """).fetchone()[0]
    print(f"Umbral de volumen (percentil {PCTL_BOT}): {umbral_volumen:,.0f} eventos en 2024-2025")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE runs_validos AS
        SELECT run FROM resumen_run
        WHERE n_eventos_totales < {umbral_volumen}
          AND n_servicios_distintos <= {MAX_SERVICIOS_DISTINTOS_RUN}
    """)
    n_validos = con.execute("SELECT COUNT(*) FROM runs_validos").fetchone()[0]
    n_total = con.execute("SELECT COUNT(*) FROM resumen_run").fetchone()[0]
    print(f"Runs validos tras filtro: {n_validos:,} de {n_total:,} ({n_validos/n_total*100:.2f}%)")

    con.execute("""
        CREATE OR REPLACE TABLE primera_fecha AS
        SELECT e.run, e.servicio, MIN(e.fecha) AS fecha
        FROM eventos e
        JOIN runs_validos r ON e.run = r.run
        GROUP BY e.run, e.servicio
    """)
    n_pf = con.execute("SELECT COUNT(*) FROM primera_fecha").fetchone()[0]
    print(f"Filas en primera_fecha (run, servicio) tras filtro: {n_pf:,}")


def fase_d_pares_run(con):
    """Self-join de primera_fecha consigo misma -- UNA vez, materializado.
    Las fases E/F/G (por franjas) leen de aca en vez de repetir el join.

    Si `pares_run` ya existe (ej. una corrida anterior murio en una fase
    posterior por OOM), se reusa en vez de recalcular -- el self-join de
    ~900M filas es la parte mas cara del pipeline en tiempo, no vale la pena
    repetirlo si los umbrales de filtrado de bots no cambiaron."""
    print("\n" + "=" * 70)
    print("FASE D -- self-join base: pares_run (run x par de servicios x lag)")
    print("=" * 70)

    existe = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'pares_run'"
    ).fetchone()[0]
    if existe:
        n = con.execute("SELECT COUNT(*) FROM pares_run").fetchone()[0]
        print(f"pares_run ya existe (de una corrida anterior) -- reusando, {n:,} filas. "
              f"Si cambiaste PCTL_BOT/MAX_SERVICIOS_DISTINTOS_RUN, borra la tabla antes de correr.")
        return

    con.execute("""
        CREATE OR REPLACE TABLE pares_run AS
        SELECT
            a.servicio AS servicio_a,
            b.servicio AS servicio_b,
            a.run,
            b.fecha AS fecha_b,
            date_diff('day', a.fecha, b.fecha) AS lag_dias  -- positivo: a antes que b
        FROM primera_fecha a
        JOIN primera_fecha b
          ON a.run = b.run AND a.servicio < b.servicio
    """)
    n = con.execute("SELECT COUNT(*) FROM pares_run").fetchone()[0]
    print(f"Filas en pares_run: {n:,}")


def fase_run_grupo(con):
    """Cohorte etaria A-F + anio de nacimiento estimado por run -- UNA vez
    (atributo estable de la persona, no depende de la franja ni del par).
    Tabla chica: ~14-20M filas (un run puede caer en 2 grupos solapados)."""
    print("\n" + "=" * 70)
    print("FASE R -- run_grupo: cohorte etaria por run (ALERTA A8)")
    print("=" * 70)

    # bounds: una fila por (entero de rango_run, grupo) -- ver _expandir_grupos_por_entero().
    # Permite un JOIN de igualdad en vez de BETWEEN (universo cerrado y chico: 1-23).
    valores_bounds = ", ".join(
        f"({v}, '{grupo}', '{edad}')" for v, grupo, edad in _expandir_grupos_por_entero()
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE run_grupo AS
        WITH rango_int AS (
            -- FLOOR(CAST AS DOUBLE) en vez de SPLIT_PART+CAST de string: mismo resultado
            -- para rango_run >= 0 (parte entera antes del punto decimal), sin parsear texto.
            SELECT run, FLOOR(TRY_CAST(ANY_VALUE(rango_run) AS DOUBLE))::INTEGER AS v
            FROM eventos
            GROUP BY run
        ),
        bounds(v, grupo, edad_aprox) AS (VALUES {valores_bounds})
        -- fronteras solapadas: un run puede caer en 1 o 2 grupos a la vez (ver bounds arriba).
        -- anio_nacimiento_estimado: formula continua de rut-a-edad sobre el punto medio del
        -- bucket de rango_run (mas granular que el grupo A-F, pero sigue siendo aproximado).
        SELECT
            r.run, b.grupo, b.edad_aprox,
            (r.v + 0.5) * 1000000 * {FORMULA_EDAD_COEF} + {FORMULA_EDAD_INTERCEPT} AS anio_nacimiento_estimado
        FROM rango_int r
        JOIN bounds b ON r.v = b.v
    """)
    n = con.execute("SELECT COUNT(*) FROM run_grupo").fetchone()[0]
    n_runs_sin_grupo = con.execute(f"""
        WITH rango_int AS (
            SELECT run, FLOOR(TRY_CAST(ANY_VALUE(rango_run) AS DOUBLE))::INTEGER AS v
            FROM eventos GROUP BY run
        )
        SELECT COUNT(*) FROM rango_int WHERE v IS NULL OR v < {RANGO_RUN_MIN} OR v > {RANGO_RUN_MAX}
    """).fetchone()[0]
    print(f"Filas en run_grupo: {n:,}")
    print(f"Runs con rango_run fuera de {RANGO_RUN_MIN}-{RANGO_RUN_MAX} o no parseable "
          f"(excluidos de toda cohorte): {n_runs_sin_grupo:,}")


def inicializar_salidas_si_hace_falta(con):
    """Si esta es la primera vez que se corre este script (no hay tabla de
    checkpoint de franjas), crea vacias las 3 tablas finales y el checkpoint.
    Si se esta retomando una corrida cortada, NO toca las tablas existentes
    -- serian los resultados de las franjas ya procesadas."""
    existe_checkpoint = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'shards_procesados'"
    ).fetchone()[0]

    if not existe_checkpoint:
        con.execute("CREATE TABLE shards_procesados (shard INTEGER PRIMARY KEY)")
        con.execute("""
            CREATE OR REPLACE TABLE pares_multimes (
                servicio_a VARCHAR, servicio_b VARCHAR,
                soporte BIGINT, a_antes_b BIGINT, b_antes_a BIGINT, mismo_dia BIGINT,
                frac_direccion_dominante DOUBLE,
                lag_dias_mediana DOUBLE, lag_dias_p25 DOUBLE, lag_dias_p75 DOUBLE
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE pares_por_trimestre (
                servicio_a VARCHAR, servicio_b VARCHAR, trimestre_b DATE,
                soporte BIGINT, frac_direccion_dominante DOUBLE
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE pares_por_cohorte (
                servicio_a VARCHAR, servicio_b VARCHAR,
                grupo_etario VARCHAR, edad_aprox VARCHAR,
                soporte BIGINT, frac_direccion_dominante DOUBLE,
                edad_estimada_media DOUBLE, edad_estimada_mediana DOUBLE
            )
        """)
        print("Iniciando desde cero: tablas de salida vacias creadas.")
    else:
        n_hechas = con.execute("SELECT COUNT(*) FROM shards_procesados").fetchone()[0]
        print(f"Retomando corrida anterior: {n_hechas} de {N_SHARDS} franjas ya procesadas.")


def fase_e_f_g_por_franjas(con):
    """Procesa pares_run EN FRANJAS de hash(servicio_a || servicio_b) % N_SHARDS
    -- por PAR, no por servicio individual (evita que un servicio "hub" que
    aparezca como servicio_a en muchos pares concentre todas esas filas en una
    sola franja). Cada franja es disjunta (un par tiene un (servicio_a,
    servicio_b) fijo, cae siempre en la misma franja) -- no hace falta
    fusionar resultados parciales despues, cada INSERT es ya el resultado
    final para esos pares. Cada franja se hace en una transaccion propia: si
    el proceso muere a mitad de camino, se pierde como maximo una franja, no
    la corrida completa."""
    print("\n" + "=" * 70)
    print("FASE E/F/G -- agregados por franjas de PAR de servicios (ALERTAS A6/A8)")
    print("=" * 70)

    inicializar_salidas_si_hace_falta(con)
    hechas = {r[0] for r in con.execute("SELECT shard FROM shards_procesados").fetchall()}

    for k in range(N_SHARDS):
        if k in hechas:
            continue

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE pares_shard AS
                SELECT servicio_a, servicio_b, run, fecha_b, lag_dias
                FROM pares_run
                WHERE hash(servicio_a || '#' || servicio_b) % {N_SHARDS} = {k}
            """)
            n_shard = con.execute("SELECT COUNT(*) FROM pares_shard").fetchone()[0]

            con.execute(f"""
                INSERT INTO pares_multimes
                SELECT
                    servicio_a, servicio_b,
                    COUNT(*) AS soporte,
                    SUM(CASE WHEN lag_dias > 0 THEN 1 ELSE 0 END) AS a_antes_b,
                    SUM(CASE WHEN lag_dias < 0 THEN 1 ELSE 0 END) AS b_antes_a,
                    SUM(CASE WHEN lag_dias = 0 THEN 1 ELSE 0 END) AS mismo_dia,
                    GREATEST(
                        SUM(CASE WHEN lag_dias > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN lag_dias < 0 THEN 1 ELSE 0 END)
                    )::DOUBLE / COUNT(*) AS frac_direccion_dominante,
                    approx_quantile(ABS(lag_dias), 0.5) FILTER (WHERE lag_dias != 0) AS lag_dias_mediana,
                    approx_quantile(ABS(lag_dias), 0.25) FILTER (WHERE lag_dias != 0) AS lag_dias_p25,
                    approx_quantile(ABS(lag_dias), 0.75) FILTER (WHERE lag_dias != 0) AS lag_dias_p75
                FROM pares_shard
                GROUP BY servicio_a, servicio_b
                HAVING COUNT(*) >= {MIN_SOPORTE_PAR}
            """)

            con.execute(f"""
                INSERT INTO pares_por_trimestre
                SELECT
                    servicio_a, servicio_b,
                    date_trunc('quarter', fecha_b) AS trimestre_b,
                    COUNT(*) AS soporte,
                    GREATEST(
                        SUM(CASE WHEN lag_dias > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN lag_dias < 0 THEN 1 ELSE 0 END)
                    )::DOUBLE / COUNT(*) AS frac_direccion_dominante
                FROM pares_shard
                GROUP BY servicio_a, servicio_b, trimestre_b
                HAVING COUNT(*) >= {MIN_SOPORTE_DESGLOSE}
            """)

            con.execute(f"""
                INSERT INTO pares_por_cohorte
                SELECT
                    p.servicio_a, p.servicio_b, rg.grupo AS grupo_etario, rg.edad_aprox,
                    COUNT(*) AS soporte,
                    GREATEST(
                        SUM(CASE WHEN p.lag_dias > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN p.lag_dias < 0 THEN 1 ELSE 0 END)
                    )::DOUBLE / COUNT(*) AS frac_direccion_dominante,
                    ROUND(AVG(YEAR(p.fecha_b) - rg.anio_nacimiento_estimado), 1) AS edad_estimada_media,
                    ROUND(approx_quantile(YEAR(p.fecha_b) - rg.anio_nacimiento_estimado, 0.5), 1) AS edad_estimada_mediana
                FROM pares_shard p
                JOIN run_grupo rg ON p.run = rg.run
                GROUP BY p.servicio_a, p.servicio_b, rg.grupo, rg.edad_aprox
                HAVING COUNT(*) >= {MIN_SOPORTE_DESGLOSE}
            """)

            con.execute("DROP TABLE pares_shard")
            con.execute("INSERT INTO shards_procesados VALUES (?)", [k])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"Franja {k + 1}/{N_SHARDS} lista y guardada ({n_shard:,} filas de pares_run procesadas).")

    n_pares = con.execute("SELECT COUNT(*) FROM pares_multimes").fetchone()[0]
    n_trim = con.execute("SELECT COUNT(*) FROM pares_por_trimestre").fetchone()[0]
    n_coh = con.execute("SELECT COUNT(*) FROM pares_por_cohorte").fetchone()[0]
    print(f"\nTotales tras {N_SHARDS} franjas -- pares_multimes: {n_pares:,} | "
          f"pares_por_trimestre: {n_trim:,} | pares_por_cohorte: {n_coh:,}")


def fase_h_export(con):
    print("\n" + "=" * 70)
    print("FASE H -- export")
    print("=" * 70)

    con.execute(f"""
        COPY (
            SELECT
                p.*,
                da.nombre AS nombre_a,
                db.nombre AS nombre_b
            FROM pares_multimes p
            LEFT JOIN dim da ON p.servicio_a = da.servicio
            LEFT JOIN dim db ON p.servicio_b = db.servicio
            ORDER BY soporte DESC
        ) TO '{OUTPUT_DIR}/pares_multimes.csv' (HEADER, DELIMITER ',')
    """)
    con.execute(f"COPY pares_por_trimestre TO '{OUTPUT_DIR}/pares_por_trimestre.csv' (HEADER, DELIMITER ',')")
    con.execute(f"COPY pares_por_cohorte TO '{OUTPUT_DIR}/pares_por_cohorte.csv' (HEADER, DELIMITER ',')")
    print(f"Exportado a {OUTPUT_DIR}/pares_multimes.csv, pares_por_trimestre.csv, pares_por_cohorte.csv")


def main():
    con = conectar()
    fase_b_c_filtrado_y_primera_fecha(con)
    fase_d_pares_run(con)
    fase_run_grupo(con)
    fase_e_f_g_por_franjas(con)
    fase_h_export(con)
    con.close()


if __name__ == "__main__":
    main()
