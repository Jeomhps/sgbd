"""
Demonstration : arbre d'execution MANUEL vs AUTOMATIQUE (Optimizer).

Pour chaque requete, on montre cote a cote :
  - L'arbre construit A LA MAIN (operateurs assembles par le developpeur)
  - L'arbre construit par l'OPTIMIZER (genere automatiquement depuis le SQL)
  - La comparaison des resultats et des temps d'execution

Schema :
  EMPLOYES (A1:id, A2:salaire, A3:dept_id)
  DEPTS    (A1:dept_id, A2:budget)
"""

import time

from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Restrict import Restrict
from operators.Project import Project
from operators.Join import Join
from operators.HashJoin import HashJoin
from operators.Aggregate import Aggregate
from sql import Catalog, SQLParser, QueryPlanner, QueryOptimizer, Executor


# ── Creation des tables ──────────────────────────────────────────────────────

def make_employes():
    tbl = TableMemoire(3)
    for vals in [
        (1, 45000, 10),
        (2, 60000, 20),
        (3, 55000, 10),
        (4, 70000, 30),
        (5, 40000, 20),
        (6, 80000, 10),
    ]:
        t = Tuple(3)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def make_depts():
    tbl = TableMemoire(2)
    for vals in [(10, 500000), (20, 300000), (30, 100000)]:
        t = Tuple(2)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def make_catalog():
    cat = Catalog()
    cat.register("EMPLOYES", make_employes())
    cat.register("DEPTS", make_depts())
    return cat


# ── Helpers d'affichage ──────────────────────────────────────────────────────

def print_rows(rows, headers):
    print("    " + "\t".join(headers))
    print("    " + "-" * (10 * len(headers)))
    for row in rows:
        print("    " + "\t".join(str(v) for v in row))
    print(f"    ({len(rows)} ligne(s))")


def timed_execute(op):
    """Execute un operateur et retourne (resultats, temps_ms)."""
    t0 = time.perf_counter()
    rows = Executor.execute(op)
    dt = (time.perf_counter() - t0) * 1000
    return rows, dt


def run_auto(sql, catalog):
    """Construit et execute via l'Optimizer. Retourne (rows, temps_ms, plan_str)."""
    query = SQLParser(sql).parse()
    optimizer = QueryOptimizer(catalog)
    op, plan = optimizer.optimize(query)
    rows, dt = timed_execute(op)
    return rows, dt, str(plan)


def compare(label, sql, manual_builder):
    """Compare l'arbre manuel et l'arbre automatique pour une meme requete."""
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"  SQL : {sql}")
    print('=' * 70)

    catalog = make_catalog()

    # ── MANUEL ──
    print("\n  [MANUEL] Arbre construit a la main :")
    op_man, tree_str, headers = manual_builder()
    print(tree_str)
    rows_man, dt_man = timed_execute(op_man)
    print_rows(rows_man, headers)
    print(f"    Temps : {dt_man:.3f} ms")

    # ── AUTOMATIQUE ──
    print(f"\n  [AUTO]   Arbre genere par l'Optimizer :")
    rows_auto, dt_auto, plan = run_auto(sql, catalog)
    for line in plan.split("\n"):
        print(f"    {line}")
    print()
    print_rows(rows_auto, headers)
    print(f"    Temps : {dt_auto:.3f} ms")

    # ── COMPARAISON ──
    ok = sorted(map(tuple, rows_man)) == sorted(map(tuple, rows_auto))
    status = "IDENTIQUES" if ok else "DIFFERENTS"
    print(f"\n  Resultats : {status}")
    print(f"  Temps manuel={dt_man:.3f} ms | auto={dt_auto:.3f} ms")
    return ok, dt_man, dt_auto


# ── Demo 1 : FullScan ───────────────────────────────────────────────────────

def demo_fullscan():
    def build():
        scan = FullScanTableMemoire(make_employes())
        tree = "    FullScan(EMPLOYES)\n"
        return scan, tree, ["id", "salaire", "dept_id"]
    return compare("Demo 1 : FullScan", "SELECT * FROM EMPLOYES", build)


# ── Demo 2 : Restrict ───────────────────────────────────────────────────────

def demo_restrict():
    def build():
        scan = FullScanTableMemoire(make_employes())
        filtre = Restrict(scan, 1, 50000, ">")
        tree = (
            "    Restrict(col=1, val=50000, op='>')\n"
            "      └─ FullScan(EMPLOYES)\n"
        )
        return filtre, tree, ["id", "salaire", "dept_id"]
    return compare(
        "Demo 2 : Restrict (Selection)",
        "SELECT * FROM EMPLOYES WHERE EMPLOYES.A2 > 50000",
        build,
    )


# ── Demo 3 : Restrict + Project ─────────────────────────────────────────────

def demo_restrict_project():
    def build():
        scan = FullScanTableMemoire(make_employes())
        filtre = Restrict(scan, 1, 50000, ">")
        proj = Project(filtre, [0, 1])
        tree = (
            "    Project([0, 1])\n"
            "      └─ Restrict(col=1, val=50000, op='>')\n"
            "           └─ FullScan(EMPLOYES)\n"
        )
        return proj, tree, ["id", "salaire"]
    return compare(
        "Demo 3 : Restrict + Project",
        "SELECT EMPLOYES.A1, EMPLOYES.A2 FROM EMPLOYES WHERE EMPLOYES.A2 > 50000",
        build,
    )


# ── Demo 4 : Nested Loop Join ───────────────────────────────────────────────

def demo_join():
    def build():
        scan_emp = FullScanTableMemoire(make_employes())
        scan_dept = FullScanTableMemoire(make_depts())
        join = Join(scan_emp, scan_dept, 2, 0)
        tree = (
            "    Join(left_col=2, right_col=0)\n"
            "      ├─ FullScan(EMPLOYES)\n"
            "      └─ FullScan(DEPTS)\n"
        )
        return join, tree, ["id", "salaire", "dept_id", "dept_id", "budget"]
    return compare(
        "Demo 4 : Nested Loop Join",
        "SELECT * FROM EMPLOYES, DEPTS WHERE EMPLOYES.A3 = DEPTS.A1",
        build,
    )


# ── Demo 5 : Hash Join ──────────────────────────────────────────────────────

def demo_hashjoin():
    def build():
        scan_emp = FullScanTableMemoire(make_employes())
        scan_dept = FullScanTableMemoire(make_depts())
        join = HashJoin(scan_emp, scan_dept, 2, 0)
        tree = (
            "    HashJoin(left_col=2, right_col=0)\n"
            "      ├─ FullScan(EMPLOYES)\n"
            "      └─ FullScan(DEPTS)\n"
        )
        return join, tree, ["id", "salaire", "dept_id", "dept_id", "budget"]
    return compare(
        "Demo 5 : Hash Join (meme requete, autre algorithme)",
        "SELECT * FROM EMPLOYES, DEPTS WHERE EMPLOYES.A3 = DEPTS.A1",
        build,
    )


# ── Demo 6 : Arbre complet avec push-down ───────────────────────────────────

def demo_arbre_complet():
    def build():
        scan_emp = FullScanTableMemoire(make_employes())
        scan_dept = FullScanTableMemoire(make_depts())
        filtre = Restrict(scan_emp, 1, 50000, ">")      # push-down
        join = Join(filtre, scan_dept, 2, 0)
        proj = Project(join, [0, 1, 4])
        tree = (
            "    Project([0, 1, 4])\n"
            "      └─ Join(left_col=2, right_col=0)\n"
            "           ├─ Restrict(col=1, >50000) [PUSH-DOWN]\n"
            "           │    └─ FullScan(EMPLOYES)\n"
            "           └─ FullScan(DEPTS)\n"
        )
        return proj, tree, ["id", "salaire", "budget"]
    return compare(
        "Demo 6 : Arbre complet (push-down + join + project)",
        "SELECT EMPLOYES.A1, EMPLOYES.A2, DEPTS.A2 FROM EMPLOYES, DEPTS "
        "WHERE EMPLOYES.A3 = DEPTS.A1 AND EMPLOYES.A2 > 50000",
        build,
    )


# ── Demo 7 : Aggregate ──────────────────────────────────────────────────────

def demo_aggregate():
    def build():
        scan = FullScanTableMemoire(make_employes())
        agg = Aggregate(scan, 1, "AVG")
        tree = (
            "    Aggregate(col=1, func='AVG')\n"
            "      └─ FullScan(EMPLOYES)\n"
        )
        return agg, tree, ["AVG(salaire)"]
    return compare(
        "Demo 7 : Aggregate (AVG)",
        "SELECT AVG(EMPLOYES.A2) FROM EMPLOYES",
        build,
    )


# ── Demo 8 : Aggregate GROUP BY ─────────────────────────────────────────────

def demo_aggregate_group_by():
    def build():
        scan = FullScanTableMemoire(make_employes())
        agg = Aggregate(scan, 1, "SUM", [2])
        tree = (
            "    Aggregate(col=1, func='SUM', group_by=[2])\n"
            "      └─ FullScan(EMPLOYES)\n"
        )
        return agg, tree, ["dept_id", "SUM(salaire)"]
    return compare(
        "Demo 8 : Aggregate GROUP BY",
        "SELECT SUM(EMPLOYES.A2) FROM EMPLOYES GROUP BY EMPLOYES.A3",
        build,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  DEMONSTRATION : ARBRE MANUEL vs ARBRE AUTOMATIQUE (OPTIMIZER)")
    print("=" * 70)
    print("\n  Tables utilisees :")
    print("    EMPLOYES (A1:id, A2:salaire, A3:dept_id)")
    print("      (1, 45000, 10)  (2, 60000, 20)  (3, 55000, 10)")
    print("      (4, 70000, 30)  (5, 40000, 20)  (6, 80000, 10)")
    print("    DEPTS (A1:dept_id, A2:budget)")
    print("      (10, 500000)  (20, 300000)  (30, 100000)")

    demos = [
        demo_fullscan,
        demo_restrict,
        demo_restrict_project,
        demo_join,
        demo_hashjoin,
        demo_arbre_complet,
        demo_aggregate,
        demo_aggregate_group_by,
    ]

    total_man = 0.0
    total_auto = 0.0
    passed = 0

    for fn in demos:
        try:
            ok, dt_man, dt_auto = fn()
            total_man += dt_man
            total_auto += dt_auto
            if ok:
                passed += 1
        except Exception as exc:
            print(f"\n  ERREUR : {exc}")
            import traceback; traceback.print_exc()

    print(f"\n{'=' * 70}")
    print(f"  BILAN : {passed}/{len(demos)} demos avec resultats identiques")
    print(f"  Temps total MANUEL = {total_man:.3f} ms")
    print(f"  Temps total AUTO   = {total_auto:.3f} ms")
    print('=' * 70)
