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


# ── Helpers ──────────────────────────────────────────────────────────────────

def check(label, got, expected):
    ok = sorted(map(tuple, got)) == sorted(map(tuple, expected))
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {label}")
    if not ok:
        print(f"    attendu : {sorted(map(tuple, expected))}")
        print(f"    obtenu  : {sorted(map(tuple, got))}")
    return ok


# ── Test 1 : FullScan simple ────────────────────────────────────────────────

def test_fullscan():
    """
    Arbre :  FullScan(EMPLOYES)

    Equivalent SQL : SELECT * FROM EMPLOYES
    """
    print("\n=== Test 1 : FullScan ===")
    print("  Arbre : FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    rows = Executor.execute(scan)

    return check("SELECT * FROM EMPLOYES → 6 lignes", rows, [
        [1,45000,10], [2,60000,20], [3,55000,10],
        [4,70000,30], [5,40000,20], [6,80000,10],
    ])


# ── Test 2 : Restrict ───────────────────────────────────────────────────────

def test_restrict():
    """
    Arbre :  Restrict(col=1, val=50000, op='>')
               └─ FullScan(EMPLOYES)

    Equivalent SQL : SELECT * FROM EMPLOYES WHERE salaire > 50000
    """
    print("\n=== Test 2 : Restrict ===")
    print("  Arbre : Restrict(col=1, >50000)")
    print("            └─ FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    filtre = Restrict(scan, _col=1, _val=50000, _op=">")
    rows = Executor.execute(filtre)

    return check("WHERE salaire > 50000 → 4 lignes", rows, [
        [2,60000,20], [3,55000,10], [4,70000,30], [6,80000,10],
    ])


# ── Test 3 : Project ────────────────────────────────────────────────────────

def test_project():
    """
    Arbre :  Project([0, 1])
               └─ FullScan(EMPLOYES)

    Equivalent SQL : SELECT id, salaire FROM EMPLOYES
    """
    print("\n=== Test 3 : Project ===")
    print("  Arbre : Project([0, 1])")
    print("            └─ FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    proj = Project(scan, [0, 1])
    rows = Executor.execute(proj)

    return check("SELECT id, salaire → 6 lignes x 2 cols", rows, [
        [1,45000], [2,60000], [3,55000], [4,70000], [5,40000], [6,80000],
    ])


# ── Test 4 : Restrict + Project (chainage) ──────────────────────────────────

def test_restrict_project():
    """
    Arbre :  Project([0, 1])
               └─ Restrict(col=1, val=50000, op='>')
                    └─ FullScan(EMPLOYES)

    Equivalent SQL : SELECT id, salaire FROM EMPLOYES WHERE salaire > 50000
    """
    print("\n=== Test 4 : Restrict + Project ===")
    print("  Arbre : Project([0, 1])")
    print("            └─ Restrict(col=1, >50000)")
    print("                 └─ FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    filtre = Restrict(scan, 1, 50000, ">")
    proj = Project(filtre, [0, 1])
    rows = Executor.execute(proj)

    return check("SELECT id, salaire WHERE salaire > 50000", rows, [
        [2,60000], [3,55000], [4,70000], [6,80000],
    ])


# ── Test 5 : Join (boucle imbriquee) ────────────────────────────────────────

def test_join():
    """
    Arbre :  Join(left_col=2, right_col=0)
               ├─ FullScan(EMPLOYES)
               └─ FullScan(DEPTS)

    Equivalent SQL : SELECT * FROM EMPLOYES, DEPTS
                     WHERE EMPLOYES.dept_id = DEPTS.dept_id
    """
    print("\n=== Test 5 : Nested Loop Join ===")
    print("  Arbre : Join(left_col=2, right_col=0)")
    print("            ├─ FullScan(EMPLOYES)")
    print("            └─ FullScan(DEPTS)")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())
    join = Join(scan_emp, scan_dept, _left_col=2, _right_col=0)
    rows = Executor.execute(join)

    return check("JOIN emp.dept_id = dept.dept_id → 6 lignes", rows, [
        [1,45000,10, 10,500000],
        [2,60000,20, 20,300000],
        [3,55000,10, 10,500000],
        [4,70000,30, 30,100000],
        [5,40000,20, 20,300000],
        [6,80000,10, 10,500000],
    ])


# ── Test 6 : HashJoin ───────────────────────────────────────────────────────

def test_hashjoin():
    """
    Arbre :  HashJoin(left_col=2, right_col=0)
               ├─ FullScan(EMPLOYES)
               └─ FullScan(DEPTS)

    Meme resultat que le Join, mais algorithme different.
    """
    print("\n=== Test 6 : Hash Join ===")
    print("  Arbre : HashJoin(left_col=2, right_col=0)")
    print("            ├─ FullScan(EMPLOYES)")
    print("            └─ FullScan(DEPTS)")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())
    join = HashJoin(scan_emp, scan_dept, _left_col=2, _right_col=0)
    rows = Executor.execute(join)

    return check("HashJoin → meme resultat que NLJ", rows, [
        [1,45000,10, 10,500000],
        [2,60000,20, 20,300000],
        [3,55000,10, 10,500000],
        [4,70000,30, 30,100000],
        [5,40000,20, 20,300000],
        [6,80000,10, 10,500000],
    ])


# ── Test 7 : Arbre complet (push-down + join + project) ─────────────────────

def test_arbre_complet():
    """
    Arbre :  Project([0, 1, 4])
               └─ Join(left_col=2, right_col=0)
                    ├─ Restrict(col=1, val=50000, op='>')   ← push-down
                    │    └─ FullScan(EMPLOYES)
                    └─ FullScan(DEPTS)

    Equivalent SQL : SELECT id, salaire, budget
                     FROM EMPLOYES, DEPTS
                     WHERE dept_id = dept_id AND salaire > 50000
    """
    print("\n=== Test 7 : Arbre complet (push-down + join + project) ===")
    print("  Arbre : Project([0, 1, 4])")
    print("            └─ Join(left_col=2, right_col=0)")
    print("                 ├─ Restrict(col=1, >50000) [PUSH-DOWN]")
    print("                 │    └─ FullScan(EMPLOYES)")
    print("                 └─ FullScan(DEPTS)")

    scan_emp = FullScanTableMemoire(make_employes())
    scan_dept = FullScanTableMemoire(make_depts())

    # Push-down : filtre AVANT la jointure
    filtre = Restrict(scan_emp, 1, 50000, ">")
    join = Join(filtre, scan_dept, 2, 0)
    proj = Project(join, [0, 1, 4])

    rows = Executor.execute(proj)

    # Employes avec salaire > 50000 : (2,60000,20), (3,55000,10), (4,70000,30), (6,80000,10)
    return check("SELECT id, salaire, budget WHERE salaire > 50000", rows, [
        [2, 60000, 300000],
        [3, 55000, 500000],
        [4, 70000, 100000],
        [6, 80000, 500000],
    ])


# ── Test 8 : Agregation ─────────────────────────────────────────────────────

def test_aggregate():
    """
    Arbre :  Aggregate(col=1, func='AVG')
               └─ FullScan(EMPLOYES)

    Equivalent SQL : SELECT AVG(salaire) FROM EMPLOYES
    """
    print("\n=== Test 8 : Aggregate ===")
    print("  Arbre : Aggregate(col=1, AVG)")
    print("            └─ FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    agg = Aggregate(scan, _agg_col=1, _agg_func="AVG")
    rows = Executor.execute(agg)

    avg = (45000 + 60000 + 55000 + 70000 + 40000 + 80000) / 6
    return check(f"AVG(salaire) = {avg}", rows, [[avg]])


# ── Test 9 : Aggregate GROUP BY ─────────────────────────────────────────────

def test_aggregate_group_by():
    """
    Arbre :  Aggregate(col=1, func='SUM', group_by=[2])
               └─ FullScan(EMPLOYES)

    Equivalent SQL : SELECT dept_id, SUM(salaire) FROM EMPLOYES GROUP BY dept_id
    """
    print("\n=== Test 9 : Aggregate GROUP BY ===")
    print("  Arbre : Aggregate(col=1, SUM, group_by=[2])")
    print("            └─ FullScan(EMPLOYES)")

    scan = FullScanTableMemoire(make_employes())
    agg = Aggregate(scan, 1, "SUM", [2])
    rows = Executor.execute(agg)

    # dept 10: 45000+55000+80000=180000
    # dept 20: 60000+40000=100000
    # dept 30: 70000
    return check("SUM(salaire) GROUP BY dept_id", rows, [
        [10, 180000], [20, 100000], [30, 70000],
    ])


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_fullscan,
        test_restrict,
        test_project,
        test_restrict_project,
        test_join,
        test_hashjoin,
        test_arbre_complet,
        test_aggregate,
        test_aggregate_group_by,
    ]

    passed = sum(1 for fn in tests if fn())

    print(f"\n{'=' * 50}")
    print(f"  Resultat : {passed}/{len(tests)} tests passes")
    if passed == len(tests):
        print("  Tous les tests sont passes !")
    print('=' * 50)
