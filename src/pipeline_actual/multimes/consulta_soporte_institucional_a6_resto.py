"""
Consulta para ALERTA A4 (extension 4, ultima): baseline de permutacion /
hipergeometrico para los 10 pares institucionales de la Tabla A6.1 completa
(20 pares, camino_a_precedencia/multimes/output_multimes/tabla_a6_institucional.csv)
que NO estan entre las 10 filas visibles en el cuerpo del paper.

Mismo metodo que los tres scripts anteriores de esta serie: soporte marginal
institucional (union de todos los client_id de cada institucion, segun
data/cu_con_sectores.csv) y co-uso institucional real deduplicado.

Pares (los 10 restantes de las 20 filas del CSV completo):
   8. ChileCompra -> Subsecretaria de Evaluacion Social
   9. Instituto Nacional de Estadisticas -> Subsecretaria de Evaluacion Social
  10. ChileCompra -> FONASA
  14. Instituto Nacional de Estadisticas -> Servicio Electoral
  15. FONASA -> Subsecretaria de Economia y Empresas de Menor Tamano
  16. Subsecretaria de Educacion -> FONASA
  17. Instituto Nacional de Estadisticas -> FONASA
  18. ChileCompra -> Subsecretaria de Economia y Empresas de Menor Tamano
  19. Ministerio de Desarrollo Social -> Fondo de Solidaridad e Inversion Social
  20. FONASA -> AFP Uno

Reusa `primera_fecha` (ya calculada por 2b_pares.py, fase B/C) -- no relee
zips, no recalcula nada nuevo.
"""

from pathlib import Path

import duckdb

OUTPUT_DIR = Path("output_multimes")
DB_PATH = OUTPUT_DIR / "precedencia_multimes.duckdb"

INSTITUCIONES = {
    "INE": [
        "e14510f75a2a488c98edf1c88b445540", "dbc3de69546243e48121124f1033cff8",
        "ce833d51dcb649b7a1fbba15d138a4a5", "d8f5e82ecd9b4b0daeab0eda294831f8",
        "d54d2ea52c314edeb1f9b99736e84681",
    ],
    "CHILECOMPRA": [
        "7d8c828a4733437f868d97e1da57d4e2", "4060f34dc33042fda53efdfd0594b679",
        "4bb573e349504824b5b5a050a83e094d", "8198669878224a47b743149ad102e0a7",
        "10cd2809c6ab49089aeaf14bcf0c5db5", "92b957e1a81c4784b708cf3c750799e9",
        "49ea78109b9b48459b1351e07fb60e1b", "be46b88bd9c44871b3069ca77e9263c4",
        "67339e06f4b44f78994ff4785178807a", "e05706d1b22d4d5e9534daee71e618fa",
        "7886bb1fcf354eef9b8ac029bdbcaded", "c2e3edc90e0542d7bd3cfece63a890e5",
        "834cf291d50b4e3299f82dfeba3c998e", "3ee3884163bf4c18b304ee91c2ec771f",
        "c96cc8cf53e6406a85d7c35beab12e71", "0e336a36e38547508641a1f719ed8faa",
        "b640048c801144a983ae56ebff744ba1",
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
    "SUBSEC_EDUCACION": [
        "705b78c3d59c4e1bbeaec3123c0f6de2", "fb74231009884254b2d13b0fd9056247",
        "176fe485412740d589cc0f127dff0353", "331cb18d7d51484fb07794a30ddbef6e",
        "5af004a3cb1847c189f18042d1528923", "d766af3638194588bfaa68f745e5b24a",
        "1d322c450315498d8a800c3d78501b58", "0a750e36725e495ba32bd43a5f170724",
        "ae7cc0441ee6493d97204f7a5498b861", "c0cd823534c04f9aa7a896f8961ce209",
        "eb6b584fde4141aea5dafddee3e7ad4e", "0bdc586df4b84f2596586ff1a197cbae",
        "3e2a4d42827d43d0b90248199e58e98e", "326e27c196be4490822e785c1dba53d8",
        "41d8c098b14b4f8d9fe3da689678205c", "d132edfeac8c4b9f8fb26216b2109344",
        "c91e6fe86b214cba844a5b657d9fe887", "f2ad242297b04a5ea53ed2df4e5d71b4",
        "26ce850e25c04398a2da12f4478e7597", "831d2c606fb044fc98b3110a389c20c1",
        "21f155670144410a90f1dcdb49d66b09", "c8c2ac707da0446ea44dc5db34347bb7",
        "fd3219c7cb5545d4bb6dbdd651ce8f80", "e5fafa0edd9e4c4586cebde1b6858ccd",
        "80d8324451a74ae887a93c2bee8b5d3d",
    ],
    "MDS": [
        "46dcd2e1b5ef4e01814e0929e2fd6885", "95221aafd854445babe7c42b2b437a03",
        "1f676220d5314b9eb23be7624b5fda57", "8a9f89bedf0940119071ed84f94c570d",
        "2568c649c62d46af960451f6284203d2", "3c57c05c6a9e45a5badd5cf6fe2a5180",
        "a1a7160adf0d4d9a9439ad89eb284a6b", "fe6cad21b5414356a6da4927029f942a",
        "cc20326e83d14be3b457f7214a74d300", "672f76fa24044d36b28fd11b50d7a4ec",
        "75de3dbfbc6640b496c7ae4b291aad05", "998e9b4946ae49439be0ced3927c3da3",
    ],
    "SERVICIO_ELECTORAL": [
        "e22c65a41fbf4ff689d49576ff976483", "8bad0e01746b4a279c0bdbe59d7c44ee",
        "bdd514d10e9e4401bc3979b0e06d3ab6", "60b69c5d160f4b1c90e79d8290b904c1",
        "38b3f29fe39b4e87b5f7982165de8cf1", "9609d8145a7749959bb9f4c7a8f1f3dc",
        "2c12290764aa4f67ba25e9cf801bd06c", "ed085dfc9fdc4f30b995262d6794d670",
        "11dbc789f56b46eca48acad693459cbf", "18bec3e707784a978d9d89f2967f87de",
        "8f791e1ae18a42f8ae0ec7d03fbe5dff", "11f1dcd280f442bfa07de340bfcd4c43",
        "9ad41e803fff463bac913cedefca7796", "58f532f36df74998b1f0ba5e821bd4f9",
        "325e5164453644d882e39f30e8e73329",
    ],
    "SUBSEC_ECONOMIA_MIPYMES": [
        "e1ce4963b14f4377a473c48630795a8c", "509249286e2448d4867d2694330c16d2",
        "e1a6055d78bf444e905bb069bae9b363", "316ddf2744304fe5b4042557a606b80c",
        "9ef5e5148a30437a89b52d1257e27135", "4f4d89fa8ce34da19ecb340c1d267a3e",
        "cecfb3fb24f84dcd98c0601b70677adf", "89578284a7f748ce95f21a5003c7467a",
        "358c6c57a26442f39a8536a70ee8254b", "b05f77bae52e4a98bb458b44d39bd1f6",
        "d5dac98442bb425ba884809990aa064d", "44568175e9ff4fdcb98cd4e9d368ce11",
        "f11432fe52b7432fac77ee711e00de75", "4e45eb931c684f028b32613be6e7e6dd",
        "05b7381cd2624b5f91a7cb5b3d3b339e", "60e6aea28c294af4822614682c0a316b",
        "f883bd6d70b043a09837e8b888fe3f65",
    ],
    "FOSIS": [
        "1a23edd8603542e9bef8d3d29d11d8f0", "146626a8e3914f21958d0e28091cbcfe",
        "35091edbf7b546129807dbb0add0cc46", "abd00cb7c3fa43b2b44132abee9dd7b4",
        "f6aa209f0860477380c878df96de1c84", "cb5950c1b7934f18adc82d161da640ec",
        "729c07f6c16247e6ac27fb1aad47fa63", "da78d3a7e3c34fc6965ba0f236af789c",
        "b6090e4d28ef469a9625f1105df6c13c", "a8d1ad79c34d4bad8196c8278e8fa73f",
        "117d6d62a9844fd38fdb5d53e5cbc844",
    ],
    "AFP_UNO": [
        "b34fe8a89b9a43189ec5be764c3b695c", "162c7417de9b4554bcc6be93e006b7a1",
        "2e0f9d54f6534336a19fffacd7b6af40",
    ],
}

PARES = [
    ("CHILECOMPRA", "SUBSEC_EVAL_SOCIAL"),
    ("INE", "SUBSEC_EVAL_SOCIAL"),
    ("CHILECOMPRA", "FONASA"),
    ("INE", "SERVICIO_ELECTORAL"),
    ("FONASA", "SUBSEC_ECONOMIA_MIPYMES"),
    ("SUBSEC_EDUCACION", "FONASA"),
    ("INE", "FONASA"),
    ("CHILECOMPRA", "SUBSEC_ECONOMIA_MIPYMES"),
    ("MDS", "FOSIS"),
    ("FONASA", "AFP_UNO"),
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

    necesarias = set()
    for a, b in PARES:
        necesarias.add(a)
        necesarias.add(b)

    marginales = {}
    for nombre in sorted(necesarias):
        m = marginal(con, INSTITUCIONES[nombre])
        marginales[nombre] = m
        print(f"Marginal {nombre}: {m:,} ({m/n_total*100:.2f}% de N)")

    filas = []
    print()
    for a, b in PARES:
        n_both = interseccion(con, INSTITUCIONES[a], INSTITUCIONES[b])
        print(f"{a} <-> {b}: co-uso real = {n_both:,}")
        filas.append((a, b, marginales[a], marginales[b], n_both))

    con.close()

    out_path = OUTPUT_DIR / "soporte_institucional_a6_resto.csv"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("inst_a,inst_b,marginal_a,marginal_b,n_both,n_total\n")
        for a, b, ma, mb, nb in filas:
            f.write(f"{a},{b},{ma},{mb},{nb},{n_total}\n")
    print(f"\nGuardado en {out_path}")


if __name__ == "__main__":
    main()
