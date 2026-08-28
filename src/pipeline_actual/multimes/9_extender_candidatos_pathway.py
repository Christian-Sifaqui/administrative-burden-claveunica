"""
Camino A · fase multi-mes -- Paso 9 (LOCAL, no toca AWS): extender la
clasificacion de pathway mas alla de los 17 pares evaluados a mano en el
cuerpo del paper (Seccion 7.1/6.5).

Motivacion (2026-07-17): Christian senalo que el estudio evalua contra los
cinco criterios de pathway (soporte, significancia, estabilidad trimestral,
consistencia direccional, justificacion administrativa) solo 17 pares, pese
a que la red de enriquecimiento (Seccion 6.3) ya identifica 16.052 pares
institucion-institucion con lift>=1,2. "Encontrar pocos pathways" podria ser
un artefacto del embudo de evaluacion, no evidencia de que los pathways
genuinos sean raros. Este script calcula los primeros 4 de los 5 criterios
(soporte, significancia, estabilidad, direccion) para TODOS los 87.465 pares
de SERVICIOS ya clasificados por el pipeline multi-mes -- no solo los 17 --
para identificar candidatos nuevos a evaluar manualmente contra el quinto
criterio (justificacion administrativa), que es el unico que requiere
investigacion humana caso por caso.

No requiere ninguna consulta nueva a AWS: toda la informacion ya existe
localmente en dos CSV que ya se habian generado en sesiones anteriores:

  - `pares_multimes_clasificado.csv` (87.465 filas): soporte, direccion
    (a_antes_b/b_antes_a/mismo_dia), fraccion de direccion dominante, lag en
    dias (mediana/p25/p75), nombres legibles, y clase_estabilidad ya
    calculada por trimestre (estable_completo / posible_lanzamiento /
    posible_censura_o_retiro / erratico / sin_desglose_trimestral) -- este
    ultimo campo es exactamente el filtro de estabilidad trimestral que la
    Seccion 5.5 del paper ya aplica para excluir artefactos tipo VUS/
    Sucursal Virtual FONASA, ya resuelto para los 87.465 pares, no solo
    para los 20 de la Tabla A6.1.
  - `marginal_usuario_dia.csv` (1.234 filas): soporte usuario-dia y, en su
    cuarta columna (`n_usuarios_distintos_control`), el marginal de usuario
    UNICO por servicio -- verificado cruzando 3 servicios conocidos (SII,
    RSH, FUAS) contra los marginales ya usados en otras partes del estudio,
    coincide exactamente. Este archivo se genero originalmente para la
    comparacion de normalizaciones de ALERTA B1 (5_normalizacion_usuario_dia.py),
    no para este proposito, pero contiene justo el dato que faltaba (soporte
    de un servicio, no de un par) para calcular lift sin volver a AWS.

Calcula, para cada uno de los 87.465 pares:
  lift = soporte / ((marginal_a * marginal_b) / n_total)
  p_valor = P(X >= soporte) bajo Hipergeometrica(n_total, marginal_a, marginal_b)

Filtra a candidatos "fuertes": soporte>=50 (ya deberia cumplirse en todo el
archivo por diseno del pipeline, se verifica igual), clase_estabilidad ==
'estable_completo' (excluye lanzamientos/censura/erraticos, mismo criterio
ya aplicado en el resto del estudio), lift>=1.2 (mismo umbral de la red de
enriquecimiento, Seccion 6.3), y direccion dominante >=70% (el mismo nivel
que ya tiene el pathway ancla RSH->FUAS, 73,9%).

Excluye pares donde ambos servicios ya pertenecen a instituciones que
componen alguno de los 17 pares institucionales ya evaluados en el cuerpo
del paper (lista PARES_YA_EVALUADOS abajo, transcrita de la Seccion 7.1) --
el objetivo es encontrar candidatos genuinamente NUEVOS, no redescubrir los
mismos 17 a nivel de servicio individual.

Salida: `output_multimes/candidatos_pathway_nuevos.csv`, ordenado por lift
descendente -- para investigar manualmente la justificacion administrativa
de los primeros N (el unico criterio que no se puede calcular).

Requiere: pip install scipy
"""

import csv
import unicodedata
from pathlib import Path

from scipy.stats import hypergeom

OUTPUT_DIR = Path("output_multimes")
N_TOTAL = 14_081_161  # mismo universo que el resto del estudio (primera_fecha)

UMBRAL_SOPORTE = 50
UMBRAL_LIFT = 1.2
UMBRAL_DIRECCION = 0.70

# Instituciones que componen alguno de los 17 pares ya evaluados en la
# Seccion 7.1 del paper (transcrito de ahi, no recalculado). Un candidato
# nuevo se descarta si AMBOS lados del par pertenecen a instituciones de
# esta lista Y el par en si ya aparece en esa seccion -- ver DESCARTAR_SI
# mas abajo para la lista exacta de pares (no solo instituciones sueltas,
# porque varias instituciones de esta lista SI tienen pares nuevos validos
# con otras instituciones fuera de sus 17 pares ya evaluados).
PARES_YA_EVALUADOS = {
    frozenset(["SII", "AFC"]),
    frozenset(["REGISTRO CIVIL", "FONASA"]),
    frozenset(["RSH", "FUAS"]),
    frozenset(["PODER JUDICIAL", "MINISTERIO PUBLICO"]),
    frozenset(["PORTAL MIDT", "FONASA"]),
    frozenset(["SENCE", "FONASA"]),
    frozenset(["CMF", "AFC"]),
    frozenset(["CMF", "SII"]),
    frozenset(["SENCE", "AFC"]),
    frozenset(["PORTAL MIDT", "AFC"]),
    frozenset(["CHILECOMPRA", "SUBSECRETARIA DE ECONOMIA Y EMPRESAS DE MENOR TAMANO"]),
    frozenset(["MDS", "FOSIS"]),
    frozenset(["INE", "CARABINEROS"]),
    frozenset(["CHILECOMPRA", "CARABINEROS"]),
    frozenset(["INE", "SUBSECRETARIA DE EDUCACION"]),
    frozenset(["INE", "SERVICIO ELECTORAL"]),
    frozenset(["FONASA", "AFP UNO"]),
}

# Mapeo aproximado nombre_a/nombre_b (tal como aparecen en
# pares_multimes_clasificado.csv) -> etiqueta institucional normalizada,
# para poder aplicar PARES_YA_EVALUADOS. Basado en substrings, no en el
# catalogo completo -- suficiente para el filtro de exclusion, no pretende
# ser un mapeo institucional completo (eso ya existe en cu_con_sectores.csv
# para otros propositos).
_ETIQUETAS = [
    ("SERVICIO DE IMPUESTOS INTERNOS", "SII"),
    (" SII", "SII"),
    ("AFC", "AFC"),
    ("SERCEI", "REGISTRO CIVIL"),
    ("SRCEI", "REGISTRO CIVIL"),
    ("REGISTRO CIVIL", "REGISTRO CIVIL"),
    ("FONASA", "FONASA"),
    ("REGISTRO SOCIAL DE HOGARES", "RSH"),
    (" RSH", "RSH"),
    ("FUAS", "FUAS"),
    ("PODER JUDICIAL", "PODER JUDICIAL"),
    ("PJUD", "PODER JUDICIAL"),
    ("MINISTERIO PUBLICO", "MINISTERIO PUBLICO"),
    ("FISCALIA", "MINISTERIO PUBLICO"),
    ("MIDT", "PORTAL MIDT"),
    ("DIRECCION DEL TRABAJO", "PORTAL MIDT"),
    ("SENCE", "SENCE"),
    ("MERCADO FINANCIERO", "CMF"),
    ("CHILECOMPRA", "CHILECOMPRA"),
    ("COMPRAS Y CONTRATACION PUBLICA", "CHILECOMPRA"),
    ("ECONOMIA Y EMPRESAS DE MENOR TAMANO", "SUBSECRETARIA DE ECONOMIA Y EMPRESAS DE MENOR TAMANO"),
    ("DESARROLLO SOCIAL", "MDS"),
    ("FOSIS", "FOSIS"),
    ("INSTITUTO NACIONAL DE ESTADISTICAS", "INE"),
    ("CARABINEROS", "CARABINEROS"),
    ("SUBSECRETARIA DE EDUCACION", "SUBSECRETARIA DE EDUCACION"),
    ("SERVICIO ELECTORAL", "SERVICIO ELECTORAL"),
    ("AFP UNO", "AFP UNO"),
]


def normalizar(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.upper()


def etiqueta(nombre: str) -> str | None:
    n = normalizar(nombre)
    for substr, et in _ETIQUETAS:
        if substr in n:
            return et
    return None


def cargar_marginales():
    m = {}
    with open(OUTPUT_DIR / "marginal_usuario_dia.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m[row["servicio"]] = (row["nombre"], int(row["n_usuarios_distintos_control"]))
    return m


def cargar_institucion_por_servicio():
    """client_id -> institucion, desde el catalogo cu_con_sectores.csv (mismo
    archivo usado en 3_red_institucional.py). No todos los servicio_a/b de
    pares_multimes_clasificado.csv aparecen aca -- algunos client_id tienen
    trafico pero no estan en este catalogo curado (mismo fenomeno ya
    documentado en el apendice metodologico para la brecha 554 vs 457
    instituciones); esos pares se descartan mas abajo por falta de
    institucion resoluble, no se adivinan."""
    m = {}
    with open("cu_con_sectores.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cid = row["client_id"].strip()
            inst = row["institucion"].strip()
            if cid and inst:
                m[cid] = inst
    return m


def ya_evaluado(nombre_a: str, nombre_b: str) -> bool:
    ea, eb = etiqueta(nombre_a), etiqueta(nombre_b)
    if ea is None or eb is None or ea == eb:
        return False
    return frozenset([ea, eb]) in PARES_YA_EVALUADOS


def main():
    marginales = cargar_marginales()
    instituciones = cargar_institucion_por_servicio()
    print(f"Marginales de servicio cargados: {len(marginales):,}")
    print(f"Servicios con institucion resoluble (cu_con_sectores.csv): {len(instituciones):,}")

    candidatos = []
    n_procesados = 0
    n_sin_marginal = 0
    n_bajo_soporte = 0
    n_inestable = 0
    n_bajo_lift = 0
    n_baja_direccion = 0
    n_ya_evaluado = 0
    n_sin_institucion = 0
    n_misma_institucion = 0

    with open(OUTPUT_DIR / "pares_multimes_clasificado.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n_procesados += 1
            soporte = int(row["soporte"])
            if soporte < UMBRAL_SOPORTE:
                n_bajo_soporte += 1
                continue
            if row["clase_estabilidad"] != "estable_completo":
                n_inestable += 1
                continue
            frac = float(row["frac_direccion_dominante"])
            if frac < UMBRAL_DIRECCION:
                n_baja_direccion += 1
                continue

            ma_info = marginales.get(row["servicio_a"])
            mb_info = marginales.get(row["servicio_b"])
            if ma_info is None or mb_info is None:
                n_sin_marginal += 1
                continue
            _, ma = ma_info
            _, mb = mb_info

            inst_a = instituciones.get(row["servicio_a"])
            inst_b = instituciones.get(row["servicio_b"])
            if inst_a is None or inst_b is None or not row["nombre_a"].strip() or not row["nombre_b"].strip():
                n_sin_institucion += 1
                continue
            if inst_a == inst_b:
                n_misma_institucion += 1
                continue

            esperado = (ma * mb) / N_TOTAL
            lift = soporte / esperado if esperado > 0 else float("nan")
            if not (lift == lift) or lift < UMBRAL_LIFT:
                n_bajo_lift += 1
                continue

            if ya_evaluado(row["nombre_a"], row["nombre_b"]):
                n_ya_evaluado += 1
                continue

            p = hypergeom.sf(soporte - 1, N_TOTAL, ma, mb)
            direccion = "a_antes_b" if int(row["a_antes_b"]) >= int(row["b_antes_a"]) else "b_antes_a"

            candidatos.append({
                "nombre_a": row["nombre_a"],
                "nombre_b": row["nombre_b"],
                "institucion_a": inst_a,
                "institucion_b": inst_b,
                "soporte": soporte,
                "marginal_a": ma,
                "marginal_b": mb,
                "lift": lift,
                "p_valor": p,
                "frac_direccion_dominante": frac,
                "direccion_dominante": direccion,
                "lag_dias_mediana": row["lag_dias_mediana"],
                "lag_dias_p25": row["lag_dias_p25"],
                "lag_dias_p75": row["lag_dias_p75"],
                "servicio_a": row["servicio_a"],
                "servicio_b": row["servicio_b"],
            })

    print(f"\nPares procesados: {n_procesados:,}")
    print(f"  descartados por soporte<{UMBRAL_SOPORTE}: {n_bajo_soporte:,}")
    print(f"  descartados por clase_estabilidad != estable_completo: {n_inestable:,}")
    print(f"  descartados por direccion dominante<{UMBRAL_DIRECCION:.0%}: {n_baja_direccion:,}")
    print(f"  descartados por falta de marginal de servicio: {n_sin_marginal:,}")
    print(f"  descartados por falta de institucion resoluble o nombre vacio: {n_sin_institucion:,}")
    print(f"  descartados por ser la MISMA institucion (no interinstitucional): {n_misma_institucion:,}")
    print(f"  descartados por lift<{UMBRAL_LIFT}: {n_bajo_lift:,}")
    print(f"  descartados por ya evaluado en la Seccion 7.1: {n_ya_evaluado:,}")
    print(f"  CANDIDATOS NUEVOS INTERINSTITUCIONALES: {len(candidatos):,}")

    candidatos.sort(key=lambda c: -c["lift"])

    out_path = OUTPUT_DIR / "candidatos_pathway_nuevos.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        cols = ["nombre_a", "nombre_b", "institucion_a", "institucion_b", "soporte",
                "marginal_a", "marginal_b", "lift",
                "p_valor", "frac_direccion_dominante", "direccion_dominante",
                "lag_dias_mediana", "lag_dias_p25", "lag_dias_p75", "servicio_a", "servicio_b"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for c in candidatos:
            w.writerow(c)
    print(f"\nGuardado {out_path}")

    print("\n" + "=" * 70)
    print(f"TOP 60 candidatos nuevos por lift (de {len(candidatos):,} totales)")
    print("=" * 70)
    for c in candidatos[:60]:
        print(f"{c['lift']:7.2f}x  soporte={c['soporte']:>8,}  dir={c['frac_direccion_dominante']:.0%}  "
              f"lag_mediana={c['lag_dias_mediana']:>6}d  [{c['institucion_a']}] {c['nombre_a']} -> "
              f"[{c['institucion_b']}] {c['nombre_b']} ({c['direccion_dominante']})")


if __name__ == "__main__":
    main()
