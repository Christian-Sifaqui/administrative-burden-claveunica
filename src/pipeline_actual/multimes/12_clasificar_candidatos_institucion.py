"""
Camino A · fase multi-mes -- Paso 12 (LOCAL, no toca AWS): agrega los 5.113
candidatos de `candidatos_pathway_nuevos.csv` (Seccion 19 del apendice,
pares a nivel SERVICIO) a nivel INSTITUCION, y clasifica cada par
institucion-institucion segun el patron que ya explica la mayoria de ellos
-- para poder reportar honestamente "revisamos todos" sin fingir que se
verifico fuente administrativa oficial para miles de pares uno por uno.

Idea de Christian (sesion 2026-07-17): "habria que revisarlos todos y dejar
en una tabla en el articulo los mas relevantes y en un anexo el resto".
Verificar con busqueda web cada uno de los 5.113 (o incluso de los ~2.400
pares de institucion distintos) no es factible en una sesion. Se acordo con
Christian (AskUserQuestion) un estandar: solo entra a la tabla del cuerpo
del articulo como "Pathway" lo que tiene fuente administrativa verificada
(mismo criterio que ya rige toda la Tabla A5.2/A5.3); todo el resto se
clasifica MECANICAMENTE (sin busqueda web por par) en categorias ya
caracterizadas en otras partes del paper, y se deja como anexo/CSV
suplementario en vez de silenciarlo.

Categorias (ver funcion `clasificar`):
  - confirmado_*        : pathway ya verificado esta sesion o en sesiones previas
  - ya_explicado_rsh     : dominado por un pathway RSH ya confirmado (Tabla A5.2)
  - artefacto_vus        : destino es Subsecretaria de Evaluacion Social (VUS),
                           el mismo artefacto de lanzamiento de plataforma
                           (mayo 2025) ya documentado extensamente en la
                           Seccion 5.5 -- no una dependencia nueva por par
  - efecto_base_lift_bajo: lift maximo <1,5x -- el mismo "efecto de tasa base"
                           entre instituciones grandes ya establecido en la
                           Seccion 6.4 (SII<->AFC, CMF<->AFC, etc.)
  - candidato_sin_verificar: sobrevive los filtros anteriores; queda como
                           candidato estadistico identificado pero SIN
                           verificacion individual de fuente administrativa

De estos ultimos, se investigaron a fondo (busqueda web real) los ~7 de
mayor soporte agregado que no calzaban ya en una categoria explicada -- ver
Seccion 24 del apendice metodologico para el detalle de cada busqueda y por
que ninguno se promovio.

Requiere: candidatos_pathway_nuevos.csv (generado por
9_extender_candidatos_pathway.py). Solo libreria estandar.
"""

import csv
from pathlib import Path

OUTPUT_DIR = Path("output_multimes")

CONFIRMADOS = [
    # (institucion_origen, institucion_destino) exactos, o None para "cualquiera"
]


def origen_destino_inst(r):
    if r["direccion_dominante"] == "a_antes_b":
        return r["institucion_a"], r["institucion_b"]
    return r["institucion_b"], r["institucion_a"]


def clasificar(o: str, d: str, max_lift: float) -> str:
    if o.upper() == "ACEPTA.COM S.A.":
        return "confirmado_acepta"
    if o == "Sociedad Administradora de Fondos de Cesantía de Chile S.A." and d == "Subsecretaría del Trabajo":
        return "confirmado_afc_bne"
    if o == "Subsecretaría de Salud Pública" and d == "Superintendencia de Seguridad Social":
        return "confirmado_milicencia_suseso"
    if o == "Ministerio de Desarrollo Social y Familia" and "Vivienda" in d:
        return "ya_explicado_rsh"
    if d == "Subsecretaría de Evaluación Social":
        return "artefacto_vus"
    if max_lift < 1.5:
        return "efecto_base_lift_bajo"
    return "candidato_sin_verificar"


def main():
    rows = list(csv.DictReader(open(OUTPUT_DIR / "candidatos_pathway_nuevos.csv", encoding="utf-8")))

    pares_inst: dict[tuple[str, str], list] = {}
    for r in rows:
        o, d = origen_destino_inst(r)
        if o == d:
            continue
        pares_inst.setdefault((o, d), []).append(r)

    print(f"Candidatos a nivel servicio: {len(rows):,}")
    print(f"Pares de institucion distintos (excluyendo mismo-institucion): {len(pares_inst):,}")

    agg = []
    for (o, d), lst in pares_inst.items():
        soporte_total = sum(int(r["soporte"]) for r in lst)
        n_servicios = len(lst)
        max_lift = max(float(r["lift"]) for r in lst)
        cat = clasificar(o, d, max_lift)
        agg.append((soporte_total, n_servicios, max_lift, o, d, cat))
    agg.sort(key=lambda x: -x[0])

    cat_n: dict[str, int] = {}
    cat_soporte: dict[str, int] = {}
    for soporte_total, _, _, _, _, cat in agg:
        cat_n[cat] = cat_n.get(cat, 0) + 1
        cat_soporte[cat] = cat_soporte.get(cat, 0) + soporte_total

    print("\nResumen por categoria (ordenado por soporte total):")
    for cat in sorted(cat_n, key=lambda c: -cat_soporte[c]):
        print(f"  {cat:<32} pares={cat_n[cat]:>5}  soporte_total={cat_soporte[cat]:>12,}")

    out_path = OUTPUT_DIR / "candidatos_institucion_clasificados.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["institucion_origen", "institucion_destino", "soporte_total",
                    "n_pares_servicio", "lift_max", "categoria"])
        for soporte_total, n_serv, max_lift, o, d, cat in agg:
            w.writerow([o, d, soporte_total, n_serv, round(max_lift, 2), cat])
    print(f"\nGuardado {out_path} ({len(agg):,} filas -- material suplementario completo)")


if __name__ == "__main__":
    main()
