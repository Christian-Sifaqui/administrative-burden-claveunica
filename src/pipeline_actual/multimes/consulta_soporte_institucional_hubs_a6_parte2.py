"""
Consulta para ALERTA A4 (extension 3): baseline de permutacion / hipergeometrico
para los 5 pares "narrativa especifica" restantes de la Tabla A6.1 (Seccion 6.5)
-- los que NO tienen origen en un hub casi-universal, y que el texto describe
como "los mas defendibles como pathways":

  1. Instituto Nacional de Estadisticas -> Carabineros de Chile
  2. Direccion de Compras y Contratacion Publica (ChileCompra) -> Carabineros de Chile
  3. Instituto Nacional de Estadisticas -> Subsecretaria de Educacion
  4. Instituto de Prevision Social -> Subsecretaria de Evaluacion Social
  5. Secretaria de Gobierno Digital -> FONASA

Mismo metodo que los dos scripts anteriores de esta serie (PJUD-MP, hubs
parte 1): soporte marginal institucional (union de todos los client_id de
cada institucion, segun data/cu_con_sectores.csv) y co-uso institucional
real deduplicado.

NOTA DE CALIDAD DE DATOS: la lista de client_id de "Secretaria de Gobierno
Digital" en data/cu_con_sectores.csv tiene 135 entradas, de las cuales 4 son
claramente invalidas (strings no-hex, incluyendo un "None" literal) y varias
otras son casi-duplicados sospechosos entre si (ej. terminaciones "...eb81",
"...eb83", "...eb84", "...eb85" para el mismo prefijo). Se excluyeron las 4
invalidas; las 131 restantes se usaron tal cual porque son sintacticamente
validas y no hay forma de determinar cuales son duplicados reales sin volver
a los datos crudos -- el efecto esperado es de sobre-conteo acotado, no
usuarios fantasma (un client_id nunca usado simplemente aporta cero al
JOIN). Reportar esta cifra con esa salvedad.

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
    "IPS": [
        "902b942268e4467d93f5515bc38d0848", "66a493258641428ea9797fbe33fc8b27",
        "a4f85f52345a4a36b8453d4dbc768211", "3b73e41e1ad8446daaf873d6ab4ece48",
        "0d9311646b4f47ccb352ac53eeb1d847", "e2eaf95c2e394dc588f1f5e272aff43d",
        "d73213463d8a4eacbb6d6905a607c5ce", "31bb76de8e9d4fdfabb0d1c24bb5fc92",
        "eb58aaa2e342480590cb8c229c81a451", "af0e159816f94fe189f0cd1b077584ab",
        "eba5517a8d8947cfb7af5e7285da9adc", "c8af4763390347679fd6f146b6704f99",
        "e85593238c824767a9dee817a5e31671", "ffaec74bfd414f8394bab077a989e27f",
        "d748dada3f794e19939c0e556da19af0", "37d82ead5f304c2fa7a39f2b955a4b85",
        "bf71a0052a49481882e77b3f7c7663fd", "5c6af7298ffc42a2b827d37d5fe1200d",
        "55a7afe78fd943b6b424ab89d7b7f779", "13cd47e6ddaa49d4b6d80b3a907dd750",
        "9963de59629c454c9b3eb0d426338fcf", "3f5b60933b964357b7cdc0c3077ae24d",
        # Nota: se excluyo "8394b3beb22f419ca01c64dc31db11p1" (invalido, 'p' no es hex)
    ],
    "SGD": [
        "10bba19af23b44419ccb400ecc5d4fa8", "c818ed2d5acc4228970ee8fee77af21a", "0110799749e340c2a80a51e792e58694", "c55f758e1eb348a9a5d5a8c9fb6128fc",
        "15fd49e7f00a4e7290700469cea0cfad", "21aaea492a294d48b7615bac94ab1c2b", "942d80695ebe4218af105e03e6622f49", "8bdf2e73dbe646cda882d71f6842f0a8",
        "c5887967069740efbae781fe8fc3e637", "7f629294ed8c447bb8b6e5c6a07744c2", "296594dab1cb4ff5bfcc6c2085c10db0", "e91245001db443b89e1bd4ba9c990ebb",
        "df1ee21c38dd419ca54654562bc285ff", "fdb3b1812a504d65bb91da49faa9eb83", "ff285fb343284c5c98bae7c21bec6ff2", "6f984abd983a497eab1900252b0b3e8e",
        "e679739cb28a4a299b88a1441fd1db87", "fdb3b1812a504d65bb91da49faa91234", "a2982eaff1894d2ba5936e438b2506c3", "b1ce69efafe649a2a2c9bcfcad5da311",
        "c91bdc6759d9475ea1b196f2f9710ed2", "fdb3b1812a504d65bb91da49faa9eb84", "87548542d5624bd2a454a1b467ee9f52", "8862b08cc1894046ad479753e51b05a8",
        "9a7f10b16366422cb336db617e2ea0fe", "9f5991a6cb1d47c79269e80ff93faa5c", "b2016d5326c1448783c52b0d9a1b0cb9", "69db11a4d9664dd18f8085f2c1d693ca",
        "0ced53360b2144438ae2d0fa3d7a81f0", "f337a463ea764a44976bd50605ad727f", "3401a4277b99482cb0daac31696d91e3", "9bd98cd7364c4e72b682247051fcf757",
        "6797cd1f6826411b80d7265a424dbad0", "1ce80e273c244301a9776f6fda159720", "a0d0b99e4f984ffe989f1ddde5bb774e", "ed3aa842612140f5a3adf859912824e7",
        "eb15c118f1cb4c08bce4815fb038adbf", "e6d9694333084e06a442b7556c586ddd", "81e938a8fc5d4ec3918c2195dd7db26b", "9f9c10ab736e4dd9b1d1afc948d5585b",
        "c52ceac3693c4644bb20f07dd8aa7103", "c242172b23d349018620dae9d39fd8ee", "86dad20a75904047a1de9eb9379abd39", "d6d4327ae40445518a3c8a6e62137cd6",
        "35247f05d768409c89d91ca3d529ecc9", "fdb3b1812a504d65bb91da49faa9eb81", "fdb3b1812a504d65bb91da49faa9eb85", "aae2a79d383f447c98e840d75bb0b692",
        "cf792a417f3f4fb584157050058e06b7", "2b3b76d2eee047edaafb4385548ac331", "b175a4d826ca47a19027d44c26fafd84", "04f5e83aaf8f440ba15ac6f75d75659a",
        "47738b0221084ed8a922e52af27d08bb", "e9054c34643e4f75a687aa50edaa814e", "e58df44d42f947828e3f2845a9f50559", "8cb9c003d54a47eba62869055191718d",
        "56bd13284ee111e8a49a447fd87f23a5", "a7b48af7b48c436398d595c26fc0facb", "449a0da4315e43c288dbc988ed92c7e5", "204f0a0e3190486ebe451ec0513fb779",
        "74c6e7e52ac949759f44837b60042df5", "a2d5a8c587e148b396516512e7739550", "4d34577f628c4c31a9ef62be8238bec7", "ea82c1fecab1451b894b60842869d8c6",
        "9a76efc4c34e4bd7843206658dc45454", "d79df1eba8e2402aa64c8038d693533a", "df4a83a726d14b24b0ce5eb14aacfa13", "12df58b8cf27484d9d3df2a8cb4c6ced",
        "bcd18141397140d0a3f3a45d36cedfdc", "352c27bd8a584cc4a3fbf419786834e8", "9c6e69fd3a9f4b8d8123c11e37f2a673", "f4bed2922bd14e7aba84e439171151dc",
        "603290e173754ceb8f3ac2528baf56b0", "abb2001ffe9a41f7a44165b22f5452ea", "496bbab1df6444cb9490051281608749", "95299da5a5f449f09c2fdf131183b903",
        "a4d06a3542e243aa967e7db159b6c3b9", "6051cc90d3b34560b75ecdb91bc1735f", "dc6de3bf3d0b4f5b922935755194bea7", "f2eaffaf45b84b9c8e39cc303a97e9d7",
        "13a5ea248b85455a8efd253c17941884", "7c259594d95b42ca9d04984e739eaca2", "6ba4b0cb3e26444bb3fab800ccc1eb78", "acb54f4a25f8437a9ad905c696065aac",
        "05ebea3d7e6b4daab5f51ac39b674a0f", "4c7d41887420416793a1d46848eb64cb", "55f8fa647866487c8027cc6efc5815e2", "9010aa837c5548809c58a02b7d749f42",
        "a7fe43f792cd4ce9a0c8181010f4f12a", "e64136a57374446da54f48c8d1911267", "1f946828eb674ce8893855a1c6bdec2d", "44b4638580b940b0b1fef2756423be02",
        "89c58918a2184107b3373e5c3109f490", "89fc7b6252cd4cd0b8e1d20349f2e113", "99c8f03f5ce543a1bf167cca187425f2", "d81f597f4cab42afb8be6fd8f0979801",
        "f3e1888670ba42fb85ea77459e2fc7cc", "ff35c4cfd7e04823a9fc2befba18f418", "cafc97c4760f46a3a339684c7aacb0b1", "6c46a5173cc4401885e8788491ae6393",
        "becd496b05d2467ab483228ecbeea2e5", "ee32e8b07ea2463480585c7a2d9df4e0", "155c407e02e5408e9033f88a4368c9f0", "ae6a622386ac4554838f679ce32cdf9a",
        "8f74f0887fae41d7a62bfed98706889f", "6291815e17ab4e259f984c53f78b7ca9", "68923305fca640df8a0d96980a846401", "74ba4872f8ed4307b2bdb027041075af",
        "bee6bcc5dd2b426ebf26398808c6fff9", "062660c8ef9b498d8a0b5daf5f433ab8", "ea0963619d3f43e1a5ccc337019fe624", "f34fe56997b3429bbcc8ee896c0c0bf1",
        "cafc97c4760f46a3a339684c7aacb0bf", "650b850fe0014eed8565980b49ca3242", "b0aebbbbc3bc427998e17484137f6c03", "868f1db98d35463fb1dd8e763cb68402",
        "eeaf7d1a75ad4095b4dfa9172435915b", "19b0799a04cf472c990ada9dd8ffcc75", "1a45af607442dd1125eac3183a544123", "ccc5af607442dd1125eac3183a544aaa",
        "9dc1315ffcc34b79a3e05efbd60dba18", "928b20c6b4aa43318614860a52ef5796", "d7448e7f719e477dabaf530736e9e160", "b3d0c3237da44b059f56d85a7e7e39a6",
        "d5fccb74194a4020b7788f94ad990d42", "e55f2d5c81be4670ac51c2894c03ddc9", "bf4c91e9328d4a0c81d063618a71b4b3", "66a371d256ca524833eaa863a7facacb",
        "a61234146c52433ea9aadd7faacb6add", "6637d2146c52433ea9863a7faacb6add", "7cb5347ae9dd426fa813f74b788eab05",
    ],
    # Ya calculados en scripts anteriores, se reusan como referencia (no se recalculan aqui):
    # CARABINEROS: marginal=2.539.510 (18.0% de N)
    # FONASA: marginal=6.335.323 (45.0% de N)
    # SUBSEC_EVAL_SOCIAL: marginal=6.124.946 (43.5% de N)
}

# (origen, destino) -- se reusan CARABINEROS, FONASA, SUBSEC_EVAL_SOCIAL como
# "conocidos" y se calculan sus intersecciones con las 5 instituciones nuevas.
# Los marginales de CARABINEROS/FONASA/SUBSEC_EVAL_SOCIAL se recalculan aqui
# tambien por simplicidad (son consultas baratas, evita mantener dos scripts
# sincronizados).
INSTITUCIONES["CARABINEROS"] = [
    "f8ceda9e2e5a4edbb90c3c8d22e00ed7", "4ef0ced873174ee9afc306e414132c72",
    "1ad6835586034de0b29f4a402b182c6c", "4f22549f9d3a443cb041c0e55395dbf1",
    "a2e46ce2ee1e4150954d6c6864c54578", "4e87bc93e64f42d1bf678f670865c585",
    "bc55fc87c21e47869f2c71d5773bc503", "9de3d28661c84c6a92f3749320e52473",
    "d701305f8fb54019ad127ebec04a07d7", "e0c87bcdad8648f48a779abe012b4401",
    "9f88dc88029e42d3a5b082ab396be6d3", "5b3cd808d93a4eee9b8ed46abd2e2722",
]
INSTITUCIONES["FONASA"] = [
    "c53530ec6ae845db978d8fb2de69e179", "51f3d5f6f2bc4bf39a5c9e4079971b4f",
    "3d6941a46f8b4441973872f1f209bdec", "64e1a053d6f44ff89eae1e4c321a6392",
    "b4206ac5e3224dfd900588a8f7caf95e", "55f13050939c44688cb1bb852199b805",
    "ae5dd79c8078404fa751c6a09730cc78", "80a85cdf159a4ceea99c4025d1c03962",
    "9ae54d261e454c93ae1c2cdec0fd03e6", "387ff07354a74657a44369e05d83234a",
    "525561e2446d40aabe6e142645167f75", "21411e9ba57846ee958313d9f2f81177",
    "21da73257dca4eb5bc4ba33b435c386b", "d153a909f976493a80fa3192b02d6a9d",
    "ecf28c8d3b8c428dadf391801386d31a", "bac44cbe59c24c19b176aec408c418eb",
    "dd7629fec91f4599b3cf8c0dd0b984ac", "ffcbc3f955b4421bbad32293f6f12a97",
]
INSTITUCIONES["SUBSEC_EVAL_SOCIAL"] = [
    "b2df2131754740578d280211e1658445", "c63d7e67e1f8454386089193582563ea",
    "747e937b8fcb4b918094cbfdcfaf9c2e", "3d639e0a581e4afbbc3976eed669f5c6",
    "b7c988fa6132435480b57f66696827fd", "aad25b3b101c4ea6897b29f5c58f4edf",
    "7249a9adb1c343aba8db068dbe0c3795", "a1df844c4c6149a4a2680ccb60e095fb",
    "3346db58cd2d474fbcf63e827462b85d", "061230af8491491e8777333096d1d6de",
    "285f4ab87d9a4484a4e8fa6e7db43fe6", "dc8bcd185ce744798ceb1d9370f31bbc",
    "ed6beff4035f49c6997be95df71e5544", "c82f84bf3f2b451393445a0cb6883665",
]

PARES = [
    ("INE", "CARABINEROS"),
    ("CHILECOMPRA", "CARABINEROS"),
    ("INE", "SUBSEC_EDUCACION"),
    ("IPS", "SUBSEC_EVAL_SOCIAL"),
    ("SGD", "FONASA"),
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
    for nombre in ["INE", "CHILECOMPRA", "SUBSEC_EDUCACION", "IPS", "SGD", "CARABINEROS", "FONASA", "SUBSEC_EVAL_SOCIAL"]:
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

    out_path = OUTPUT_DIR / "soporte_institucional_hubs_a6_parte2.csv"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("inst_a,inst_b,marginal_a,marginal_b,n_both,n_total\n")
        for a, b, ma, mb, nb in filas:
            f.write(f"{a},{b},{ma},{mb},{nb},{n_total}\n")
    print(f"\nGuardado en {out_path}")


if __name__ == "__main__":
    main()
