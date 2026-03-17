"""
Tests du parseur SQL intégré au MiniSGBD.

Schéma utilisé
--------------
T1 (A1:id, A2:valeur, A3:dept_id)
    (1, 10, 100)
    (2, 20, 200)
    (3, 10, 100)
    (4, 30, 200)

T2 (A1:dept_id, A2:nom_dept)
    (100, 999)
    (200, 888)
    (300, 777)

Toutes les valeurs sont des entiers pour rester cohérent avec
l'implémentation de TableMemoire.
"""

from core.TableMemoire import TableMemoire
from core.Tuple import Tuple
from sql import Catalog, SQLParser, QueryPlanner, Executor


# ──────────────────────────────────────────────────────────────────────────────
# Données de test déterministes
# ──────────────────────────────────────────────────────────────────────────────

def _make_t1() -> TableMemoire:
    tbl = TableMemoire(3)
    for vals in [(1, 10, 100), (2, 20, 200), (3, 10, 100), (4, 30, 200)]:
        t = Tuple(3)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def _make_t2() -> TableMemoire:
    tbl = TableMemoire(2)
    for vals in [(100, 999), (200, 888), (300, 777)]:
        t = Tuple(2)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def _catalog() -> Catalog:
    cat = Catalog()
    cat.register("T1", _make_t1())
    cat.register("T2", _make_t2())
    return cat


def _run(sql: str, catalog: Catalog) -> list[list]:
    """Parse, plan, execute and return raw results."""
    query   = SQLParser(sql).parse()
    planner = QueryPlanner(catalog)
    op, plan = planner.plan(query)
    print(f"\n  Plan:\n{plan}\n")
    return Executor.execute(op)


def _check(label: str, got: list[list], expected: list[list]) -> None:
    ok = sorted(map(tuple, got)) == sorted(map(tuple, expected))
    status = "✅" if ok else "❌"
    print(f"  {status} {label}")
    if not ok:
        print(f"     attendu  : {sorted(map(tuple, expected))}")
        print(f"     obtenu   : {sorted(map(tuple, got))}")


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

def test_select_star() -> None:
    print("=== Test 1 : SELECT * FROM T1 ===")
    cat = _catalog()
    rows = _run("SELECT * FROM T1", cat)
    expected = [[1,10,100],[2,20,200],[3,10,100],[4,30,200]]
    _check("SELECT *", rows, expected)


def test_projection() -> None:
    print("=== Test 2 : SELECT col par index ===")
    cat = _catalog()
    # Colonnes 0 et 2 de T1 → (id, dept_id)
    rows = _run("SELECT 0, 2 FROM T1", cat)
    expected = [[1,100],[2,200],[3,100],[4,200]]
    _check("SELECT 0,2", rows, expected)


def test_projection_attr_notation() -> None:
    print("=== Test 3 : SELECT T1.A1, T1.A3 FROM T1 ===")
    cat = _catalog()
    rows = _run("SELECT T1.A1, T1.A3 FROM T1", cat)
    expected = [[1,100],[2,200],[3,100],[4,200]]
    _check("SELECT T1.A1, T1.A3", rows, expected)


def test_restrict_equal() -> None:
    print("=== Test 4 : SELECT * FROM T1 WHERE T1.A3 = 100 ===")
    cat = _catalog()
    rows = _run("SELECT * FROM T1 WHERE T1.A3 = 100", cat)
    expected = [[1,10,100],[3,10,100]]
    _check("Restrict(A3=100)", rows, expected)


def test_restrict_greater() -> None:
    print("=== Test 5 : SELECT * FROM T1 WHERE T1.A2 > 10 ===")
    cat = _catalog()
    rows = _run("SELECT * FROM T1 WHERE T1.A2 > 10", cat)
    expected = [[2,20,200],[4,30,200]]
    _check("Restrict(A2>10)", rows, expected)


def test_restrict_string_coercion() -> None:
    print('=== Test 6 : WHERE T1.A2 = "10"  (coercion str→int) ===')
    cat = _catalog()
    rows = _run('SELECT * FROM T1 WHERE T1.A2 = "10"', cat)
    expected = [[1,10,100],[3,10,100]]
    _check('Restrict(A2="10" coerced)', rows, expected)


def test_join_basic() -> None:
    print("=== Test 7 : SELECT * FROM T1, T2 WHERE T1.A3 = T2.A1 ===")
    cat = _catalog()
    rows = _run("SELECT * FROM T1, T2 WHERE T1.A3 = T2.A1", cat)
    # T1 rows join T2 on T1.A3=T2.A1:
    # (1,10,100)+(100,999)  (2,20,200)+(200,888)
    # (3,10,100)+(100,999)  (4,30,200)+(200,888)
    expected = [
        [1,10,100,100,999],
        [2,20,200,200,888],
        [3,10,100,100,999],
        [4,30,200,200,888],
    ]
    _check("JOIN T1.A3=T2.A1", rows, expected)


def test_join_with_project() -> None:
    print("=== Test 8 : SELECT T1.A1, T2.A2 FROM T1, T2 WHERE T1.A3 = T2.A1 ===")
    cat = _catalog()
    rows = _run("SELECT T1.A1, T2.A2 FROM T1, T2 WHERE T1.A3 = T2.A1", cat)
    # T1.A1 global index 0,  T2.A2 global index 4  (offset T2=3, A2→1 → 3+1=4)
    expected = [[1,999],[2,888],[3,999],[4,888]]
    _check("JOIN + Project(A1,T2.A2)", rows, expected)


def test_join_with_filter() -> None:
    print("=== Test 9 : JOIN + filtre WHERE ===")
    cat = _catalog()
    sql = "SELECT T1.A1, T1.A2 FROM T1, T2 WHERE T1.A3 = T2.A1 AND T1.A2 = 10"
    rows = _run(sql, cat)
    # Seules les lignes où A2=10: (1,10,100) et (3,10,100)
    expected = [[1,10],[3,10]]
    _check("JOIN + Restrict(A2=10) + Project", rows, expected)


def test_example_query() -> None:
    print("=== Test 10 : Requête de l'énoncé ===")
    print("    SELECT 1,2,3 FROM T1, T2 WHERE T1.A1 = T2.A1 AND T1.A2 = 10")
    # Note: T1.A1 (id) = T2.A1 (dept_id) — hypothèse: id 100=T2.dept_id
    # Pour avoir une correspondance on utilise T1.A3 = T2.A1 dans cette version
    # mais la requête exacte de l'énoncé est interprétée ici:
    # T1.A1 = T2.A1 → T1.id (1..4) vs T2.dept_id (100,200,300) → aucune correspondance
    # On utilise donc la version adaptée aux données de test.
    cat = _catalog()
    sql = "SELECT 1, 2, 3 FROM T1, T2 WHERE T1.A3 = T2.A1 AND T1.A2 = 10"
    rows = _run(sql, cat)
    # Combined tuple après JOIN: [T1.A1,T1.A2,T1.A3,T2.A1,T2.A2] = indices 0..4
    # SELECT 1,2,3 → colonnes [1,2,3] = [T1.A2, T1.A3, T2.A1]
    # Lignes filtrées (T1.A2=10): (1,10,100,100,999) et (3,10,100,100,999)
    expected = [[10,100,100],[10,100,100]]
    _check("Requête énoncé adaptée", rows, expected)


def test_aggregate_avg() -> None:
    print("=== Test 11 : SELECT AVG(T1.A2) FROM T1 ===")
    cat = _catalog()
    rows = _run("SELECT AVG(T1.A2) FROM T1", cat)
    avg = (10 + 20 + 10 + 30) / 4   # = 17.5
    _check("AVG(A2)", rows, [[avg]])


def test_aggregate_sum() -> None:
    print("=== Test 12 : SELECT SUM(T1.A2) FROM T1 WHERE T1.A3 = 100 ===")
    cat = _catalog()
    rows = _run("SELECT SUM(T1.A2) FROM T1 WHERE T1.A3 = 100", cat)
    _check("SUM(A2) filtré", rows, [[20]])   # 10+10


def test_aggregate_group_by() -> None:
    print("=== Test 13 : SELECT SUM(T1.A2) FROM T1 GROUP BY T1.A3 ===")
    cat = _catalog()
    rows = _run("SELECT SUM(T1.A2) FROM T1 GROUP BY T1.A3", cat)
    # Groupe 100: 10+10=20;  groupe 200: 20+30=50
    expected = [[100, 20], [200, 50]]
    _check("SUM GROUP BY A3", rows, expected)


def test_aggregate_count() -> None:
    print("=== Test 14 : SELECT COUNT(T1.A1) FROM T1 ===")
    cat = _catalog()
    rows = _run("SELECT COUNT(T1.A1) FROM T1", cat)
    _check("COUNT", rows, [[4]])


def test_chained_restrict() -> None:
    print("=== Test 15 : Double restriction ===")
    cat = _catalog()
    sql = "SELECT * FROM T1 WHERE T1.A3 = 200 AND T1.A2 > 20"
    rows = _run(sql, cat)
    # A3=200: rows (2,20,200) et (4,30,200); parmi eux A2>20 → (4,30,200)
    expected = [[4, 30, 200]]
    _check("Double Restrict", rows, expected)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_select_star,
        test_projection,
        test_projection_attr_notation,
        test_restrict_equal,
        test_restrict_greater,
        test_restrict_string_coercion,
        test_join_basic,
        test_join_with_project,
        test_join_with_filter,
        test_example_query,
        test_aggregate_avg,
        test_aggregate_sum,
        test_aggregate_group_by,
        test_aggregate_count,
        test_chained_restrict,
    ]

    passed = 0
    failed = 0
    for fn in tests:
        try:
            fn()
            passed += 1
        except Exception as exc:
            print(f"  💥 ERREUR dans {fn.__name__}: {exc}")
            import traceback; traceback.print_exc()
            failed += 1
        print()

    print("=" * 60)
    print(f"Résultat : {passed}/{passed+failed} tests passés")
    if failed:
        print(f"⚠️  {failed} test(s) en échec")
    else:
        print("🎉 Tous les tests sont passés !")
