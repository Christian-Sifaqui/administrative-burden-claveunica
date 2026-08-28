import io
import zipfile
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import polars as pl
from tqdm import tqdm


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ZIP_PATHS = [
    "../data/cu_user_client-2024.zip",
    "../data/cu_user_client-2025.zip",
]

COL_FECHA = "fecha"
COL_PERSONA = "run_anonimizado"
COL_SERVICIO = "client_id_traza"
COL_RANGO = "rango_rol"


# ─────────────────────────────────────────────
# LECTURA DESDE ZIP
# ─────────────────────────────────────────────

def iter_rows_from_zip(zip_path, pbar_zips):
    with zipfile.ZipFile(zip_path, "r") as top_zip:
        inner_zips = [n for n in top_zip.namelist() if n.endswith(".zip")]
        pbar_zips.reset(total=len(inner_zips))
        pbar_zips.set_description(f"{Path(zip_path).name}")

        for inner_name in inner_zips:
            pbar_zips.set_postfix_str(inner_name)
            inner_bytes = top_zip.read(inner_name)

            with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as inner_zip:
                for csv_name in inner_zip.namelist():
                    if not csv_name.endswith(".csv"):
                        continue

                    with inner_zip.open(csv_name) as f:
                        reader = csv.DictReader(
                            io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                        )
                        yield from reader

            pbar_zips.update(1)


def iter_rows_from_all_zips(zip_paths):
    with (
        tqdm(total=0, desc="ZIPs internos",  position=0, unit="zip") as pbar_zips,
        tqdm(total=0, desc="Filas leídas",   position=1, unit="fila", unit_scale=True) as pbar_rows,
    ):
        for zip_path in zip_paths:
            path = Path(zip_path)
            if not path.exists():
                tqdm.write(f"⚠️  Archivo no encontrado, se omite: {zip_path}")
                continue

            for row in iter_rows_from_zip(zip_path, pbar_zips):
                pbar_rows.update(1)
                yield row


# ─────────────────────────────────────────────
# ETAPA 1: PRIMER Y ÚLTIMO USO POR PERSONA/SERVICIO
# ─────────────────────────────────────────────

def construir_uso_persona_servicio():
    uso = defaultdict(dict)
    filas_error = 0

    for row in iter_rows_from_all_zips(ZIP_PATHS):
        try:
            fecha = datetime.fromisoformat(row[COL_FECHA])
        except Exception:
            filas_error += 1
            continue

        persona = row[COL_PERSONA]
        servicios_raw = row[COL_SERVICIO]

        if not persona or not servicios_raw:
            filas_error += 1
            continue

        servicios = [s.strip() for s in servicios_raw.replace('"', '').split(",") if s.strip()]

        for svc in servicios:
            if svc not in uso[persona]:
                uso[persona][svc] = [fecha, fecha]
            else:
                uso[persona][svc][0] = min(uso[persona][svc][0], fecha)
                uso[persona][svc][1] = max(uso[persona][svc][1], fecha)

    tqdm.write(f"Filas con error/vacías: {filas_error:,}")
    return uso


# ─────────────────────────────────────────────
# ETAPA 2: PRECEDENCIA
# ─────────────────────────────────────────────

def calcular_precedencia(uso):
    conteo = defaultdict(int)

    for persona, servicios in tqdm(uso.items(), desc="Calculando precedencia", unit="persona", unit_scale=True):
        servicios_lista = list(servicios.items())

        for ii in range(len(servicios_lista)):
            svc_a, (a_min, a_max) = servicios_lista[ii]

            for jj in range(len(servicios_lista)):
                if ii == jj:
                    continue

                svc_b, (b_min, b_max) = servicios_lista[jj]

                if a_min < b_min:
                    conteo[(svc_a, svc_b, "inicio")] += 1

                if a_max < b_max:
                    conteo[(svc_a, svc_b, "integracion")] += 1

    tqdm.write(f"Pares de precedencia únicos: {len(conteo):,}")
    return conteo


# ─────────────────────────────────────────────
# ETAPA 3: DATAFRAME FINAL
# ─────────────────────────────────────────────

def construir_dataframe_precedencia(conteo):
    filas = [
        {"servicio_A": a, "servicio_B": b, "tipo": tipo, "n_personas": n}
        for (a, b, tipo), n in conteo.items()
    ]
    return pl.DataFrame(filas)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    t0 = datetime.now()

    print("Etapa 1: Construyendo uso persona-servicio...")
    uso = construir_uso_persona_servicio()
    tqdm.write(f"Personas únicas: {len(uso):,}")

    print("Etapa 2: Calculando precedencia...")
    conteo = calcular_precedencia(uso)

    print("Etapa 3: Construyendo DataFrame y guardando...")
    df = construir_dataframe_precedencia(conteo)
    df.write_parquet("precedencia.parquet")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"=== LISTO ✔ — Tiempo total: {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
