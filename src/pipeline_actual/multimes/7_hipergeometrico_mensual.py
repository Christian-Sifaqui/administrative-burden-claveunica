"""
Camino A · fase multi-mes -- Paso 7: test hipergeometrico por ventana mensual
(item "nice to have" de la revision editorial externa, textos/revision.md,
Seccion 10: "Correr un test hipergeometrico controlado por ventanas de tiempo
mensuales para eliminar el sesgo de la estacionalidad regulatoria, ej. el
efecto Operacion Renta").

El test hipergeometrico ya reportado en el paper (Tabla A4.1, Seccion 6.4) se
calcula sobre TODO el periodo 2024-2025 de una sola vez: usa la tasa marginal
promedio de cada institucion a lo largo de dos anios como el denominador del
baseline de independencia. Eso asume tasas marginales estables. El problema
que senala la revision: si dos instituciones comparten un pico estacional NO
relacionado entre si (ej. SII en abril por Operacion Renta, cualquier otro
tramite con pico propio en abril por una razon distinta), su co-ocurrencia
POOLED sobre 24 meses puede aparecer inflada -- ambas suben juntas en abril
por calendario compartido, no por una dependencia administrativa real.

La forma correcta de controlar esto no es "quitar abril", es recalcular el
propio baseline de independencia DENTRO de cada mes: para cada mes M, ?el
co-uso observado en M excede lo que predicen las tasas marginales de ESE
MISMO mes M? Si dos servicios simplemente comparten un pico de abril por
calendario, sus marginales de abril ya son altas, el baseline esperado de
abril ya es alto, y el lift de abril deberia acercarse a 1 (sin
enriquecimiento real) aunque el conteo absoluto de co-uso en abril sea alto.
Un pathway administrativamente real, en cambio, deberia mostrar lift >1
sostenido mes a mes, no solo en los meses de pico compartido.

Este script SOLO calcula los conteos (marginal mensual, co-uso mensual, y
los mismos agregados sobre el periodo completo para comparacion directa) --
no calcula lift ni p-valor aca, eso se hace localmente con scipy a partir
de los 4 CSV de salida (mismo patron que 3_red_institucional.py /
4_analisis_red_local.py: SQL pesado en AWS, metricas derivadas en local).

Alcance deliberadamente acotado a 8 grupos (5 instituciones + 3 servicios
individuales), elegidos para cubrir exactamente los casos que la revision
y el cuerpo del paper ya nombran:
  - Servicio de Impuestos Internos (SII) -- el ejemplo explicito de la
    revision (Operacion Renta, abril).
  - Sociedad Administradora de Fondos de Cesantia (AFC) y Comision para el
    Mercado Financiero (CMF) -- los pares SII<->AFC y SII<->CMF ya
    reportados en la Tabla A4.1/7.1 del paper.
  - Poder Judicial y Ministerio Publico -- el par ya reclasificado como
    "No es un pathway" en la Tabla A5.1 (Seccion 6.5); sirve de
    comparacion (deberia seguir sin lift sostenido mes a mes).
  - Registro Social de Hogares y FUAS-Mineduc, a nivel de SERVICIO
    individual (no de institucion -- evita mezclarlos con el resto de
    Ministerio de Desarrollo Social y Familia / Subsecretaria de
    Educacion, que son instituciones grandes y heterogeneas) -- el
    pathway ancla del estudio (RSH->FUAS); sirve de validacion en el
    sentido contrario: deberia seguir mostrando lift sostenido, no
    limitado a octubre.
  - Ventanilla Unica Social (VUS), agregado 2026-07-16 como chequeo extra
    tras una primera corrida: el marginal mensual de RSH colapsa >1000x
    desde junio 2025 (de ~700k-1,66M/mes a un par de cientos), coincidente
    con el lanzamiento oficial de VUS (19-mayo-2025) y su absorcion
    documentada de Registro Social de Hogares (Seccion 6.2 del paper,
    fuente externa Ministerio de Hacienda 2025). Se agrega VUS para
    verificar directamente si su marginal sube cuando el de RSH cae, y si
    VUS<->FUAS empieza a mostrar la misma dependencia direccional que
    RSH->FUAS -- es decir, si el "concepto" del pathway (actualizar el
    registro social antes de postular a FUAS) se mantiene aunque el
    client_id que lo registra haya cambiado a mitad de periodo, en vez de
    asumir la continuidad sin verificarla.

No se agregan mas grupos a proposito: un self-join mensual sobre TODAS las
instituciones (como el de 3_red_institucional.py, pero x24 meses) seria
mucho mas caro y esta fuera del alcance de un chequeo opcional. Este script
evalua C(8,2)=28 pares x 24 meses = 672 filas por tabla mensual.

Reusa `eventos` (run, servicio, fecha) y `primera_fecha` (para el universo
de runs ya filtrado de bots, Seccion B/C de 2b_pares.py) -- no relee zips,
no recalcula el filtro de bots.

NOTA (post-mortem 2026-07-16): la primera version de este script encadenaba
el filtro a 28 client_id, el filtro de runs validos y el DISTINCT en una
sola sentencia CREATE TABLE con 3 joins -- fallo con OutOfMemoryException
(reportado como "9,3GiB/9,3GiB used"). Esa cifra NO era el techo fisico de
la maquina -- la maquina tiene 15GB totales con ~14GB libres en el momento
del fallo (confirmado con `free -h` despues) -- era el propio
`memory_limit='10GB'` del script, que DuckDB interpreta en GB decimales
(10 * 10^9 bytes = 9,31 GiB binarios): el limite que goteo era el que este
mismo script le habia pedido, no una restriccion real de hardware. Aun asi,
la reestructuracion en dos pasos de abajo es una mejora real independiente
de ese malentendido (evita un plan de consulta ineficiente sobre 544,7M
filas), asi que se dejo. `MEMORY_LIMIT` se subio a 12GB (no se bajo) y
`THREADS` se devolvio a 4, aprovechando el margen real disponible.
`construir_presencia_mes()` ahora hace el filtro+dedup en dos pasos
materializados por separado: primero filtra `eventos` a los 28 client_id
(sin DISTINCT, sin filtro de runs -- reduce el volumen de 544,7M filas a
una fraccion chica), y solo DESPUES, sobre esa tabla ya chica, aplica el
DISTINCT y el filtro de runs validos. Mismo principio que ya aplicaba
2b_pares.py (materializar en fases en vez de encadenar operaciones caras)
pero que esta version inicial no siguio.

Requiere: pip install duckdb
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

THREADS = 4
MEMORY_LIMIT = "12GB"
TEMP_DIR = OUTPUT_DIR / "duckdb_tmp"

# client_id -> grupo, extraido a mano de cu_con_sectores.csv (ver docstring
# arriba para el criterio de seleccion de cada grupo).
GRUPOS: dict[str, str] = {}

_SII = [
    "ee00832abfba40b38335ce48a12981b5",  # Autenticacion Nube Privada
    "ef7f52e241e04cc69c0fad85a88599bc",  # Autenticación SII
    "52449c34bb1e47fe81802bf322dae8ee",  # bcm2.sii.cl
    "ac8fac68f21540539f02e8fd2a44f651",  # CapacitaSII
    "c5f58b408cee4b0ebf0cec74532175e1",  # Mandatarios Digitales
    "2549b51828ce40af920a932dd2102217",  # undefinedSII
    "51e1587ac2364f19bb4a9344841af223",  # www.sii.cl
]
_AFC = [
    "78b147a9c73f4d6d8f6d1e0f0b082556",  # Afiliación voluntaria en AFC
    "006d384713634e34915490c02379b0a4",  # Autenticador INE (asi mapeado en el catalogo)
    "b169192c478149d7a0c8baced4513d2c",  # Retiro saldo Cuenta Individual
    "4e4dd0f1b76d499588a3c32d04aa7390",  # Traspaso Fondos TCP
    "e8dde9d26979498786c6ba5a82f70ad3",  # undefinedAFC
]
_CMF = [
    "495c261c0f484bd08e232f7358452c4d",  # undefinedCMF
]
_PJUD = [
    "8ccf6c97b6eb4670bafec186fbe64f85",  # Buscador de Jurisprudencia
    "cc28da1df13d4147b5420ccc231a458e",  # Canal de denuncias
    "ae4caa1cb2154cac89440f3fe18251cb",  # Ingreso de causas y escritos
    "d602a0071f3f4db8b37a87cffd89bf23",  # PJUD
]
_MP = [
    "165139dad00243e695f7f3a695563ae2",  # Agendamiento EIVG
    "01b56ab5b9be43e19fa06300994787ec",  # Bitácora Web
    "399e7fc266894562850261cfbc06b992",  # Denuncia en Linea
    "56da1c80cfea4a5b83326bfef271fc3e",  # Escritorio de Aplicaciones
    "d0bb6506628a4f37bbee7e2aa4097b92",  # Fiscalía Atiende
    "26b4ed3b679045889fdc5539a4c2d4fc",  # Incautación de Vehículos
    "347178b052294153af290d6a84589868",  # Mi Fiscalía en Línea
    "fb74fb1e98d447cc875898434e8135bc",  # SigesPass
    "f15bd4421f604ffbae3d627bef601546",  # Transferencia de Archivos
]
_RSH = [
    "672f76fa24044d36b28fd11b50d7a4ec",  # Registro Social de Hogares (servicio individual)
]
_FUAS = [
    "0a750e36725e495ba32bd43a5f170724",  # FUAS-Mineduc (servicio individual)
]
_VUS = [
    "c82f84bf3f2b451393445a0cb6883665",  # Ventanilla Única Social (servicio individual)
    # Chequeo extra 2026-07-16: confirmar si el marginal de VUS sube cuando el de RSH
    # cae en 2025-T2/T3 (Seccion 6.2/should-fix #2 del apendice), y si VUS<->FUAS
    # empieza a mostrar la misma dependencia direccional que RSH->FUAS -- evidencia de
    # que el "concepto" del pathway se mantiene aunque el client_id que lo registra
    # haya cambiado a mitad de periodo, no una perdida real de comportamiento.
]

for cid in _SII:
    GRUPOS[cid] = "Servicio de Impuestos Internos"
for cid in _AFC:
    GRUPOS[cid] = "AFC"
for cid in _CMF:
    GRUPOS[cid] = "Comisión para el Mercado Financiero"
for cid in _PJUD:
    GRUPOS[cid] = "Poder Judicial"
for cid in _MP:
    GRUPOS[cid] = "Ministerio Público"
for cid in _RSH:
    GRUPOS[cid] = "Registro Social de Hogares (servicio)"
for cid in _VUS:
    GRUPOS[cid] = "Ventanilla Única Social (servicio)"
for cid in _FUAS:
    GRUPOS[cid] = "FUAS-Mineduc (servicio)"


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    for tabla in ("eventos", "primera_fecha"):
        r = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [tabla]
        ).fetchone()
        if r[0] == 0:
            raise SystemExit(f"Falta la tabla '{tabla}' -- correr 1_ingesta.py / 2b_pares.py primero.")
    return con


def construir_mapa_grupo(con):
    print("=" * 70)
    print("PASO 7 -- test hipergeometrico por ventana mensual (nice-to-have)")
    print("=" * 70)
    con.execute("CREATE OR REPLACE TABLE mapa_grupo (client_id VARCHAR, grupo VARCHAR)")
    con.executemany("INSERT INTO mapa_grupo VALUES (?, ?)", list(GRUPOS.items()))
    print(f"Grupos definidos: {len(set(GRUPOS.values()))} "
          f"({len(GRUPOS)} client_id en total)")
    for g in sorted(set(GRUPOS.values())):
        n_ids = sum(1 for v in GRUPOS.values() if v == g)
        print(f"  {g}: {n_ids} client_id")


def construir_presencia_mes(con):
    print("\nPaso 1/2 -- eventos_grupo: filtrar `eventos` (544,7M filas) a solo los "
          f"{len(GRUPOS)} client_id de interes, SIN DISTINCT todavia y SIN el filtro de "
          "runs validos todavia (materializado aparte para no encadenar 3 joins/DISTINCT "
          "en una sola sentencia -- eso fue lo que produjo el OOM: el optimizador no "
          "redujo el tamano antes de intentar deduplicar).")
    con.execute("""
        CREATE OR REPLACE TABLE eventos_grupo AS
        SELECT e.run, g.grupo, date_trunc('month', e.fecha) AS mes
        FROM eventos e
        JOIN mapa_grupo g ON e.servicio = g.client_id
    """)
    n_crudo = con.execute("SELECT COUNT(*) FROM eventos_grupo").fetchone()[0]
    print(f"Filas en eventos_grupo (sin deduplicar, sin filtro de runs validos aun): {n_crudo:,} "
          "-- ya deberia ser una fraccion pequena de 544,7M")

    print("\nPaso 2/2 -- presencia_mes_grupo: deduplicar (run, grupo, mes) y aplicar el "
          "filtro de runs validos (semi-join contra primera_fecha) sobre la tabla ya chica...")
    con.execute("""
        CREATE OR REPLACE TABLE presencia_mes_grupo AS
        SELECT DISTINCT run, grupo, mes
        FROM eventos_grupo
        WHERE run IN (SELECT DISTINCT run FROM primera_fecha)
    """)
    con.execute("DROP TABLE eventos_grupo")
    n = con.execute("SELECT COUNT(*) FROM presencia_mes_grupo").fetchone()[0]
    n_meses = con.execute("SELECT COUNT(DISTINCT mes) FROM presencia_mes_grupo").fetchone()[0]
    print(f"Filas (run, grupo, mes): {n:,} | meses distintos: {n_meses}")

    # Version colapsada sobre meses (run, grupo) -- para los agregados de periodo
    # completo que sirven de referencia directa contra las cifras ya reportadas
    # en el paper (Tabla A4.1/A5.1, mismo N=14.081.161).
    con.execute("""
        CREATE OR REPLACE TABLE presencia_total_grupo AS
        SELECT DISTINCT run, grupo FROM presencia_mes_grupo
    """)


def exportar_marginales(con):
    n_total = con.execute("SELECT COUNT(DISTINCT run) FROM primera_fecha").fetchone()[0]
    print(f"\nN total (mismo universo que Tablas A4.1/A5.1/A6.1): {n_total:,}")

    con.execute(f"""
        COPY (
            SELECT grupo, mes, COUNT(DISTINCT run) AS marginal, {n_total} AS n_total
            FROM presencia_mes_grupo
            GROUP BY grupo, mes
            ORDER BY grupo, mes
        ) TO '{OUTPUT_DIR}/hipergeometrico_mensual_marginales.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Guardado {OUTPUT_DIR}/hipergeometrico_mensual_marginales.csv")

    con.execute(f"""
        COPY (
            SELECT grupo, COUNT(DISTINCT run) AS marginal, {n_total} AS n_total
            FROM presencia_total_grupo
            GROUP BY grupo
            ORDER BY marginal DESC
        ) TO '{OUTPUT_DIR}/hipergeometrico_total_marginales.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Guardado {OUTPUT_DIR}/hipergeometrico_total_marginales.csv "
          "(mismos 7 grupos, agregado sobre todo 2024-2025 -- referencia directa)")


def exportar_pares(con):
    print("\nCo-uso por par de grupos, por mes (self-join chico: 7 grupos, "
          "no todas las instituciones)...")
    con.execute(f"""
        COPY (
            SELECT a.grupo AS grupo_a, b.grupo AS grupo_b, a.mes AS mes, COUNT(*) AS co_uso
            FROM presencia_mes_grupo a
            JOIN presencia_mes_grupo b ON a.run = b.run AND a.mes = b.mes AND a.grupo < b.grupo
            GROUP BY a.grupo, b.grupo, a.mes
            ORDER BY a.grupo, b.grupo, a.mes
        ) TO '{OUTPUT_DIR}/hipergeometrico_mensual_pares.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Guardado {OUTPUT_DIR}/hipergeometrico_mensual_pares.csv")

    con.execute(f"""
        COPY (
            SELECT a.grupo AS grupo_a, b.grupo AS grupo_b, COUNT(*) AS co_uso
            FROM presencia_total_grupo a
            JOIN presencia_total_grupo b ON a.run = b.run AND a.grupo < b.grupo
            GROUP BY a.grupo, b.grupo
            ORDER BY co_uso DESC
        ) TO '{OUTPUT_DIR}/hipergeometrico_total_pares.csv' (HEADER, DELIMITER ',')
    """)
    print(f"Guardado {OUTPUT_DIR}/hipergeometrico_total_pares.csv (referencia directa)")


def main():
    con = conectar()
    construir_mapa_grupo(con)
    construir_presencia_mes(con)
    exportar_marginales(con)
    exportar_pares(con)
    con.close()
    print("\nListo. Bajar los 4 CSV de output_multimes/ (hipergeometrico_mensual_marginales.csv, "
          "hipergeometrico_mensual_pares.csv, hipergeometrico_total_marginales.csv, "
          "hipergeometrico_total_pares.csv). El lift y el p-valor hipergeometrico por mes "
          "y el agregado de referencia se calculan localmente con scipy a partir de estos "
          "4 CSV (no requieren mas consultas a AWS).")


if __name__ == "__main__":
    main()
