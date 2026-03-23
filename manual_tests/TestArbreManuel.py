"""
Demonstration : construction manuelle d'un arbre d'execution.

On construit les operateurs a la main (sans passer par le Parser/Optimizer)
et on les passe directement a l'Executor.

Schema :
  EMPLOYES (id, salaire, dept_id)   — 3 attributs
  DEPTS    (dept_id, budget)        — 2 attributs
"""

from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Restrict import Restrict
from operators.Project import Project
from operators.Join import Join
from operators.HashJoin import HashJoin
from operators.Aggregate import Aggregate
from sql.Executor import Executor


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


def print_rows(rows, headers):
    """Affiche les resultats sous forme de tableau."""
    print("  " + "\t".join(headers))
    print("  " + "-" * (10 * len(headers)))
    for row in rows:
        print("  " + "\t".join(str(v) for v in row))
    print(f"  ({len(rows)} ligne(s))\n")


def separator(title, sql):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"  SQL : {sql}")
    print('=' * 70)


# ── Demo 1 : FullScan ───────────────────────────────────────────────────────

def demo_fullscan():
    sql = "SELECT * FROM EMPLOYES"
    separator("Demo 1 : FullScan", sql)

    print("\n  Arbre d'execution :")
    print("    FullScan(EMPLOYES)\n")

    scan = FullScanTableMemoire(make_employes())
    rows = Executor.execute(scan)
    print_rows(rows, ["id", "salaire", "dept_id"])
    print(f"  Instrumentation : {scan}")


# ── Demo 2 : Restrict ───────────────────────────────────────────────────────

def demo_restrict():
    sql = "SELECT * FROM EMPLOYES WHERE salaire > 50000"
    separator("Demo 2 : Restrict (Selection)", sql)

    print("\n  Arbre d'execution :")
    print("    Restrict(col=1, val=50000, op='>')")
    print("      └─ FullScan(EMPLOYES)\n")

    scan = FullScanTableMemoire(make_employes())
    filtre = Restrict(scan, _col=1, _val=50000, _op=">")
    rows = Executor.execute(filtre)
    print_rows(rows, ["id", "salaire", "dept_id"])
    print(f"  Instrumentation scan   : {scan}")
    print(f"  Instrumentation filtre : {filtre}")


# ── Demo 3 : Restrict + Project ─────────────────────────────────────────────

def demo_restrict_project():
    sql = "SELECT id, salaire FROM EMPLOYES WHERE salaire > 50000"
    separator("Demo 3 : Restrict + Project (Selection + Projection)", sql)

    print("\n  Arbre d'execution :")
    print("    Project([0, 1])")
    print("      └─ Restrict(col=1, val=50000, op='>')")
    print("           └─ FullScan(EMPLOYES)\n")

    scan = FullScanTableMemoire(make_employes())
    filtre = Restrict(scan, 1, 50000, ">")
    proj = Project(filtre, [0, 1])
    rows = Executor.execute(proj)
    print_rows(rows, ["id", "salaire"])
    print(f"  Instrumentation scan   : {scan}")
    print(f"  Instrumentation filtre : {filtre}")
    print(f"  Instrumentation projet : {proj}")


# ── Demo 4 : Nested Loop Join ───────────────────────────────────────────────

def demo_join():
    sql = "SELECT * FROM EMPLOYES, DEPTS WHERE EMPLOYES.dept_id = DEPTS.dept_id"
    separator("Demo 4 : Nested Loop Join", sql)

    print("\n  Arbre d'execution :")
    print("    Join(left_col=2, right_col=0)")
    print("      ├─ FullScan(EMPLOYES)")
    print("      └─ FullScan(DEPTS)\n")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())
    join = Join(scan_emp, scan_dept, _left_col=2, _right_col=0)
    rows = Executor.execute(join)
    print_rows(rows, ["id", "salaire", "dept_id", "dept_id", "budget"])
    print(f"  Instrumentation scan_emp  : {scan_emp}")
    print(f"  Instrumentation scan_dept : {scan_dept}")
    print(f"  Instrumentation join      : {join}")


# ── Demo 5 : Hash Join ──────────────────────────────────────────────────────

def demo_hashjoin():
    sql = "SELECT * FROM EMPLOYES, DEPTS WHERE EMPLOYES.dept_id = DEPTS.dept_id"
    separator("Demo 5 : Hash Join (meme requete, algorithme different)", sql)

    print("\n  Arbre d'execution :")
    print("    HashJoin(left_col=2, right_col=0)")
    print("      ├─ FullScan(EMPLOYES)")
    print("      └─ FullScan(DEPTS)\n")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())
    join = HashJoin(scan_emp, scan_dept, _left_col=2, _right_col=0)
    rows = Executor.execute(join)
    print_rows(rows, ["id", "salaire", "dept_id", "dept_id", "budget"])
    print(f"  Instrumentation scan_emp  : {scan_emp}")
    print(f"  Instrumentation scan_dept : {scan_dept}")
    print(f"  Instrumentation hashjoin  : {join}")


# ── Demo 6 : Arbre complet avec push-down ───────────────────────────────────

def demo_arbre_complet():
    sql = ("SELECT id, salaire, budget FROM EMPLOYES, DEPTS "
           "WHERE EMPLOYES.dept_id = DEPTS.dept_id AND salaire > 50000")
    separator("Demo 6 : Arbre complet (push-down + join + project)", sql)

    print("\n  Arbre d'execution (optimise : restriction AVANT jointure) :")
    print("    Project([0, 1, 4])")
    print("      └─ Join(left_col=2, right_col=0)")
    print("           ├─ Restrict(col=1, val=50000, op='>') [PUSH-DOWN]")
    print("           │    └─ FullScan(EMPLOYES)")
    print("           └─ FullScan(DEPTS)\n")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())
    filtre = Restrict(scan_emp, 1, 50000, ">")       # push-down AVANT join
    join = Join(filtre, scan_dept, 2, 0)
    proj = Project(join, [0, 1, 4])
    rows = Executor.execute(proj)
    print_rows(rows, ["id", "salaire", "budget"])
    print(f"  Instrumentation scan_emp  : {scan_emp}")
    print(f"  Instrumentation filtre    : {filtre}")
    print(f"  Instrumentation scan_dept : {scan_dept}")
    print(f"  Instrumentation join      : {join}")
    print(f"  Instrumentation projet    : {proj}")


# ── Demo 7 : Aggregate ──────────────────────────────────────────────────────

def demo_aggregate():
    sql = "SELECT AVG(salaire) FROM EMPLOYES"
    separator("Demo 7 : Aggregate (AVG)", sql)

    print("\n  Arbre d'execution :")
    print("    Aggregate(col=1, func='AVG')")
    print("      └─ FullScan(EMPLOYES)\n")

    scan = FullScanTableMemoire(make_employes())
    agg = Aggregate(scan, _agg_col=1, _agg_func="AVG")
    rows = Executor.execute(agg)
    print_rows(rows, ["AVG(salaire)"])
    print(f"  Instrumentation scan : {scan}")
    print(f"  Instrumentation agg  : {agg}")


# ── Demo 8 : Aggregate GROUP BY ─────────────────────────────────────────────

def demo_aggregate_group_by():
    sql = "SELECT dept_id, SUM(salaire) FROM EMPLOYES GROUP BY dept_id"
    separator("Demo 8 : Aggregate GROUP BY", sql)

    print("\n  Arbre d'execution :")
    print("    Aggregate(col=1, func='SUM', group_by=[2])")
    print("      └─ FullScan(EMPLOYES)\n")

    scan = FullScanTableMemoire(make_employes())
    agg = Aggregate(scan, 1, "SUM", [2])
    rows = Executor.execute(agg)
    print_rows(rows, ["dept_id", "SUM(salaire)"])
    print(f"  Instrumentation scan : {scan}")
    print(f"  Instrumentation agg  : {agg}")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  DEMONSTRATION : CONSTRUCTION MANUELLE D'ARBRES D'EXECUTION")
    print("=" * 70)
    print("\n  Tables utilisees :")
    print("    EMPLOYES (id, salaire, dept_id)")
    print("      (1, 45000, 10)  (2, 60000, 20)  (3, 55000, 10)")
    print("      (4, 70000, 30)  (5, 40000, 20)  (6, 80000, 10)")
    print("    DEPTS (dept_id, budget)")
    print("      (10, 500000)  (20, 300000)  (30, 100000)")

    demo_fullscan()
    demo_restrict()
    demo_restrict_project()
    demo_join()
    demo_hashjoin()
    demo_arbre_complet()
    demo_aggregate()
    demo_aggregate_group_by()

    print(f"\n{'=' * 70}")
    print("  FIN DE LA DEMONSTRATION — 8 arbres executes avec succes")
    print('=' * 70)
