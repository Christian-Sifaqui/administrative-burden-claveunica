"""
Camino A · fase multi-mes -- Paso 10 (LOCAL, no toca AWS): cruza el catalogo
oficial CPAT (dependencias interinstitucionales documentadas por ley) contra
los pares institucion-institucion ya calculados desde los logs de ClaveUnica
(`red_institucional_pares.csv`, soporte y direccion, 43.005 pares con
soporte>0, generados por 3_red_institucional.py).

Idea de Christian (sesion 2026-07-17): en vez de partir de un patron
estadistico y despues buscar si existe una norma que lo justifique -- el
metodo ya usado en el resto del paper para las Tablas A5.1/A5.2 -- invertir
el proceso: partir de la lista OFICIAL de procedimientos administrativos que
exigen a la ciudadania un documento emitido por OTRO organismo publico, y
verificar cuales de esas dependencias ya documentadas por el propio Estado
tienen un correlato observable (soporte, direccion) en los logs reales de
ClaveUnica.

Fuente: Catalogo de Procedimientos Administrativos y otras Tramitaciones
(CPAT), publicado por la Secretaria de Gobierno Digital en el portal de
Datos Abiertos de Chile (julio 2025) -- la herramienta con la que cada
organismo cumple la fase de preparacion de la Ley 21.180 (la misma ley que
fundamenta todo este estudio). Dos recursos del dataset:
  - "CPAT nivel central - Datos y/o documentos requeridos por otros
    organismos publicos": una fila por (codigo de registro, documento
    requerido). La columna "Medio utilizado" distingue tres casos:
    interoperabilidad electronica automatica, intercambio manual entre
    organismos, o "No hay interoperabilidad, se solicita a sus usuarios(as)"
    -- este ultimo es el unico que deberia dejar rastro conductual en
    ClaveUnica: el ciudadano debe ir a buscar el documento a otro organismo.
  - "CPAT nivel central - nomina oficial": codigo de registro -> institucion
    dueña del procedimiento (entre otras 50+ columnas administrativas).

Ambos se descargan automaticamente si no existen localmente (dataset publico,
sin autenticacion). Requiere solo `requests` y la libreria estandar.

Salida: para cada par (institucion proveedora del documento, institucion
dueña del procedimiento) documentado en CPAT, se busca el par correspondiente
en red_institucional_pares.csv (matching por nombre normalizado, mismo
criterio NFKD del resto del proyecto) y se reporta soporte, lift, y si la
direccion observada coincide con la esperada (proveedora ANTES que dueña --
se obtiene el documento primero, luego se usa en el otro tramite).
"""

import csv
import unicodedata
import urllib.request
from pathlib import Path

MULTIMES_DIR = Path(__file__).parent
OUTPUT_DIR = MULTIMES_DIR / "output_multimes"
CPAT_DIR = MULTIMES_DIR / "cpat_data"

CPAT_CENTRAL_URL = (
    "https://datos.gob.cl/dataset/9d3788ef-d695-47c6-8d74-f2aa54a04521/resource/"
    "a6d2862a-b509-43b5-8aaf-80710cbd17f0/download/"
    "cpat-nivel-central-datos-y_o-documentos-requeridos-por-otros-organismos-publicos-julio-2025.csv"
)
CPAT_NOMINA_URL = (
    "https://datos.gob.cl/dataset/9d3788ef-d695-47c6-8d74-f2aa54a04521/resource/"
    "32e7ee42-3cf4-4dc8-9d03-723a54ee7bc0/download/"
    "cpat-nivel-central-nomina-oficial-julio-2025.csv"
)

N_TOTAL = 14_081_161  # mismo universo que el resto del estudio (primera_fecha)

MEDIO_SOLICITA_A_USUARIOS = "No hay interoperabilidad, se solicita a sus usuarios(as)"


def normalizar(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.upper().strip().split())


def asegurar_descarga(path: Path, url: str):
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Descargando {path.name} desde datos.gob.cl...")
    urllib.request.urlretrieve(url, path)
    print(f"  {path.stat().st_size:,} bytes guardados en {path}")


def cargar_nomina(path: Path) -> dict:
    m = {}
    with open(path, encoding="utf-8-sig") as f:
        r = csv.reader(f, delimiter=";")
        next(r)
        for row in r:
            m[row[2].strip()] = row[1].strip()
    return m


def cargar_dependencias_cpat(path: Path, nomina: dict) -> dict:
    """(institucion_proveedora, institucion_dueña) -> set de codigos de
    registro (para contar cuantos PROCEDIMIENTOS DISTINTOS documentan la
    misma dependencia, no solo cuantas filas)."""
    dep: dict = {}
    n_sin_dueña = 0
    with open(path, encoding="utf-8-sig") as f:
        r = csv.reader(f, delimiter=";")
        next(r)
        for row in r:
            if row[2].strip() != MEDIO_SOLICITA_A_USUARIOS:
                continue
            proveedora = row[3].strip()
            codigo = row[0].strip()
            dueña = nomina.get(codigo)
            if not dueña:
                n_sin_dueña += 1
                continue
            if normalizar(proveedora) == normalizar(dueña):
                continue  # mismo organismo pidiendose un documento a si mismo -- no interinstitucional
            dep.setdefault((proveedora, dueña), set()).add(codigo)
    print(f"Filas CPAT sin institucion dueña resoluble (codigo no en nomina): {n_sin_dueña}")
    return dep


def cargar_pares_logs() -> dict:
    pares = {}
    with open(OUTPUT_DIR / "red_institucional_pares.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            a, b = row["inst_a"], row["inst_b"]
            key = frozenset([normalizar(a), normalizar(b)])
            pares[key] = {
                "inst_a": a, "inst_b": b,
                "soporte": int(row["soporte"]),
                "a_antes_b": int(row["a_antes_b"]),
                "b_antes_a": int(row["b_antes_a"]),
                "mismo_dia": int(row["mismo_dia"]),
            }
    return pares


def cargar_marginales_logs() -> dict:
    m = {}
    with open(OUTPUT_DIR / "red_institucional_marginales.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m[normalizar(row["institucion"])] = int(row["marginal"])
    return m


def main():
    cpat_central = CPAT_DIR / "cpat_nivel_central_datos_requeridos.csv"
    cpat_nomina = CPAT_DIR / "cpat_nivel_central_nomina_oficial.csv"
    asegurar_descarga(cpat_central, CPAT_CENTRAL_URL)
    asegurar_descarga(cpat_nomina, CPAT_NOMINA_URL)

    nomina = cargar_nomina(cpat_nomina)
    dependencias = cargar_dependencias_cpat(cpat_central, nomina)
    print(f"Dependencias interinstitucionales unicas documentadas en CPAT "
          f"(proveedora != dueña, '{MEDIO_SOLICITA_A_USUARIOS}'): {len(dependencias):,}")
    print(f"  (suma de {sum(len(v) for v in dependencias.values()):,} procedimientos individuales)")

    pares_logs = cargar_pares_logs()
    marginales = cargar_marginales_logs()
    print(f"Pares institucion-institucion con soporte>0 en los logs: {len(pares_logs):,}")

    encontrados = []
    n_sin_marginal = 0
    n_sin_coocurrencia = 0

    for (proveedora, dueña), codigos in dependencias.items():
        np_, nd = normalizar(proveedora), normalizar(dueña)
        if np_ not in marginales or nd not in marginales:
            n_sin_marginal += 1
            continue
        par = pares_logs.get(frozenset([np_, nd]))
        if par is None:
            n_sin_coocurrencia += 1
            continue

        ma, mb = marginales[np_], marginales[nd]
        esperado = (ma * mb) / N_TOTAL
        lift = par["soporte"] / esperado if esperado > 0 else float("nan")

        # direccion esperada: proveedora (documento) ANTES que dueña (procedimiento que lo exige)
        if normalizar(par["inst_a"]) == np_:
            proveedora_antes, dueña_antes = par["a_antes_b"], par["b_antes_a"]
        else:
            proveedora_antes, dueña_antes = par["b_antes_a"], par["a_antes_b"]
        total_direccional = proveedora_antes + dueña_antes
        frac_esperada = proveedora_antes / total_direccional if total_direccional > 0 else float("nan")

        encontrados.append({
            "proveedora": proveedora, "dueña": dueña, "n_procedimientos_cpat": len(codigos),
            "soporte": par["soporte"], "marginal_proveedora": ma, "marginal_dueña": mb,
            "lift": lift, "frac_direccion_esperada": frac_esperada,
            "proveedora_antes": proveedora_antes, "dueña_antes": dueña_antes,
            "mismo_dia": par["mismo_dia"],
        })

    encontrados.sort(key=lambda x: -x["soporte"])

    print(f"\nDependencias CPAT CON correlato en los logs (soporte>0): {len(encontrados):,}")
    print(f"Dependencias CPAT sin marginal resoluble en los logs: {n_sin_marginal:,}")
    print(f"Dependencias CPAT con ambas instituciones en los logs pero SIN co-ocurrencia: {n_sin_coocurrencia:,}")

    out_path = OUTPUT_DIR / "cpat_vs_logs.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        cols = ["proveedora", "dueña", "n_procedimientos_cpat", "soporte", "marginal_proveedora",
                "marginal_dueña", "lift", "frac_direccion_esperada", "proveedora_antes",
                "dueña_antes", "mismo_dia"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in encontrados:
            w.writerow(e)
    print(f"Guardado {out_path}")

    print("\n" + "=" * 70)
    print("TOP 40 dependencias CPAT con mayor soporte observado en los logs")
    print("=" * 70)
    for e in encontrados[:40]:
        print(f"soporte={e['soporte']:>9,}  lift={e['lift']:6.2f}x  "
              f"dir_esperada={e['frac_direccion_esperada']:.0%} ({e['n_procedimientos_cpat']} proc. CPAT)  "
              f"{e['proveedora']} -> {e['dueña']}")


if __name__ == "__main__":
    main()
