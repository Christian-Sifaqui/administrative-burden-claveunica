"""
Consulta para ALERTA A4 (extension 2): baseline de permutacion / hipergeometrico
para los pares "hub" de la Tabla A6.1 (Seccion 6.5) que quedaron marcados como
"soporte alto y direccion estable, pero causalidad no confirmada" -- los que
tienen como origen una institucion de uso casi-universal (FONASA, Ministerio
de Desarrollo Social, SII).

Mismo metodo que consulta_soporte_institucional_pjud_mp.py: soporte marginal
institucional (union de todos los client_id de cada institucion, segun
data/cu_con_sectores.csv) y co-uso institucional real deduplicado -- no la
suma de pares de servicios individuales que ya esta en la Tabla A6.1.

Pares evaluados (5 de los 10 visibles en la Tabla A6.1, los que tienen origen
"hub"):
  1. FONASA -> Subsecretaria de Evaluacion Social
  2. Ministerio de Desarrollo Social -> FONASA
  3. Ministerio de Desarrollo Social -> Subsecretaria de Evaluacion Social
  4. SII -> FONASA
  5. FONASA -> Carabineros de Chile

Reusa `primera_fecha` (ya calculada por 2b_pares.py, fase B/C) -- no relee
zips, no recalcula nada nuevo.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

INSTITUCIONES = {
    "FONASA": [
        "c53530ec6ae845db978d8fb2de69e179", "51f3d5f6f2bc4bf39a5c9e4079971b4f",
        "3d6941a46f8b4441973872f1f209bdec", "64e1a053d6f44ff89eae1e4c321a6392",
        "b4206ac5e3224dfd900588a8f7caf95e", "55f13050939c44688cb1bb852199b805",
        "ae5dd79c8078404fa751c6a09730cc78", "80a85cdf159a4ceea99c4025d1c03962",
        "9ae54d261e454c93ae1c2cdec0fd03e6", "387ff07354a74657a44369e05d83234a",
        "525561e2446d40aabe6e142645167f75", "21411e9ba57846ee958313d9f2f81177",
        "21da73257dca4eb5bc4ba33b435c386b", "d153a909f976493a80fa3192b02d6a9d",
        "ecf28c8d3b8c428dadf391801386d31a", "bac44cbe59c24c19b176aec408c418eb",
        "dd7629fec91f4599b3cf8c0dd0b984ac", "ffcbc3f955b4421bbad32293f6f12a97",
    ],
    "MDS": [
        "46dcd2e1b5ef4e01814e0929e2fd6885", "95221aafd854445babe7c42b2b437a03",
        "1f676220d5314b9eb23be7624b5fda57", "8a9f89bedf0940119071ed84f94c570d",
        "2568c649c62d46af960451f6284203d2", "3c57c05c6a9e45a5badd5cf6fe2a5180",
        "a1a7160adf0d4d9a9439ad89eb284a6b", "fe6cad21b5414356a6da4927029f942a",
        "cc20326e83d14be3b457f7214a74d300", "672f76fa24044d36b28fd11b50d7a4ec",
        "75de3dbfbc6640b496c7ae4b291aad05", "998e9b4946ae49439be0ced3927c3da3",
    ],
    "SUBSEC_EVAL_SOCIAL": [
        "b2df2131754740578d280211e1658445", "c63d7e67e1f8454386089193582563ea",
        "747e937b8fcb4b918094cbfdcfaf9c2e", "3d639e0a581e4afbbc3976eed669f5c6",
        "b7c988fa6132435480b57f66696827fd", "aad25b3b101c4ea6897b29f5c58f4edf",
        "7249a9adb1c343aba8db068dbe0c3795", "a1df844c4c6149a4a2680ccb60e095fb",
        "3346db58cd2d474fbcf63e827462b85d", "061230af8491491e8777333096d1d6de",
        "285f4ab87d9a4484a4e8fa6e7db43fe6", "dc8bcd185ce744798ceb1d9370f31bbc",
        "ed6beff4035f49c6997be95df71e5544", "c82f84bf3f2b451393445a0cb6883665",
    ],
    "SII": [
        "ee00832abfba40b38335ce48a12981b5", "ef7f52e241e04cc69c0fad85a88599bc",
        "52449c34bb1e47fe81802bf322dae8ee", "ac8fac68f21540539f02e8fd2a44f651",
        "c5f58b408cee4b0ebf0cec74532175e1", "2549b51828ce40af920a932dd2102217",
        "51e1587ac2364f19bb4a9344841af223",
    ],
    "CARABINEROS": [
        "f8ceda9e2e5a4edbb90c3c8d22e00ed7", "4ef0ced873174ee9afc306e414132c72",
        "1ad6835586034de0b29f4a402b182c6c", "4f22549f9d3a443cb041c0e55395dbf1",
        "a2e46ce2ee1e4150954d6c6864c54578", "4e87bc93e64f42d1bf678f670865c585",
        "bc55fc87c21e47869f2c71d5773bc503", "9de3d28661c84c6a92f3749320e52473",
        "d701305f8fb54019ad127ebec04a07d7", "e0c87bcdad8648f48a779abe012b4401",
        "9f88dc88029e42d3a5b082ab396be6d3", "5b3cd808d93a4eee9b8ed46abd2e2722",
    ],
}

PARES = [
    ("FONASA", "SUBSEC_EVAL_SOCIAL"),
    ("MDS", "FONASA"),
    ("MDS", "SUBSEC_EVAL_SOCIAL"),
    ("SII", "FONASA"),
    ("FONASA", "CARABINEROS"),
]


def marginal(con, ids):
    ph = ",".join(["?"] * len(ids))
    return con.execute(
        f"SELECT COUNT(DISTINCT run) FROM primera_fecha WHERE servicio IN ({ph})", ids
    ).fetchone()[0]


def interseccion(con, ids_a, ids_b):
    ph_a = ",".join(["?"] * len(ids_a))
    ph_b = ",".join(["?"] * len(ids_b))
    return con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT run FROM primera_fecha WHERE servicio IN ({ph_a})
            INTERSECT
            SELECT run FROM primera_fecha WHERE servicio IN ({ph_b})
        )
        """,
        ids_a + ids_b,
    ).fetchone()[0]


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    n_total = con.execute("SELECT COUNT(DISTINCT run) FROM primera_fecha").fetchone()[0]
    print(f"N total: {n_total:,}")

    marginales = {}
    for nombre, ids in INSTITUCIONES.items():
        m = marginal(con, ids)
        marginales[nombre] = m
        print(f"Marginal {nombre}: {m:,} ({m/n_total*100:.2f}% de N)")

    filas = []
    print()
    for a, b in PARES:
        n_both = interseccion(con, INSTITUCIONES[a], INSTITUCIONES[b])
        print(f"{a} <-> {b}: co-uso real = {n_both:,}")
        filas.append((a, b, marginales[a], marginales[b], n_both))

    con.close()

    out_path = OUTPUT_DIR / "soporte_institucional_hubs_a6.csv"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("inst_a,inst_b,marginal_a,marginal_b,n_both,n_total\n")
        for a, b, ma, mb, nb in filas:
            f.write(f"{a},{b},{ma},{mb},{nb},{n_total}\n")
    print(f"\nGuardado en {out_path}")


if __name__ == "__main__":
    main()
