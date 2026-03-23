"""
Demonstration de l'optimiseur de requetes.

Compare le plan naif (QueryPlanner) avec le plan optimise (QueryOptimizer)
sur les memes requetes, pour montrer les 3 regles d'optimisation :

  R1 — Descente des restrictions (push-down)
  R2 — Choix du meilleur algorithme de jointure
  R3 — Utilisation d'index (IndexScan au lieu de FullScan)
"""

from core.TableMemoire import TableMemoire
from core.Tuple import Tuple
from index.StaticHashIndex import StaticHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from sql import Catalog, SQLParser, QueryPlanner, QueryOptimizer, Executor


# ── Donnees de test ──────────────────────────────────────────────────────────

def make_small_table(name, nb_att, rows):
    """Cree une petite table deterministe."""
    tbl = TableMemoire(nb_att)
    for vals in rows:
        t = Tuple(nb_att)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def make_large_table(nb_att, size, val_range=1000):
    """Cree une grande table aleatoire."""
    return TableMemoire.randomize(nb_att, val_range, size)


# ── Comparaison naif vs optimise ─────────────────────────────────────────────

def compare(label, sql, catalog):
    """Execute une requete avec le planner naif ET l'optimiseur, compare."""
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"  SQL: {sql}")
    print('=' * 70)

    query = SQLParser(sql).parse()

    # --- Plan naif ---
    print("\n--- PLAN NAIF (QueryPlanner) ---")
    planner = QueryPlanner(catalog)
    op_naive, plan_naive = planner.plan(query)
    print(plan_naive)
    rows_naive = Executor.execute(op_naive)

    # --- Plan optimise ---
    # Re-parser car les operateurs sont consommes
    query = SQLParser(sql).parse()
    print("\n--- PLAN OPTIMISE (QueryOptimizer) ---")
    optimizer = QueryOptimizer(catalog)
    op_opt, plan_opt = optimizer.optimize(query)
    print(plan_opt)
    rows_opt = Executor.execute(op_opt)

    # --- Verification ---
    naive_sorted  = sorted(map(tuple, rows_naive))
    opt_sorted    = sorted(map(tuple, rows_opt))
    ok = naive_sorted == opt_sorted
    status = "PASS" if ok else "FAIL"
    print(f"\n  Resultats identiques : {status}")
    print(f"  Nombre de lignes    : {len(rows_opt)}")
    if not ok:
        print(f"  Naif    : {naive_sorted[:5]}...")
        print(f"  Optimise: {opt_sorted[:5]}...")
    return ok


# ── Tests ────────────────────────────────────────────────────────────────────

def test_push_down():
    """R1 : Les restrictions sont poussees AVANT la jointure."""
    cat = Catalog()
    cat.register("EMPLOYES", make_small_table("EMPLOYES", 3, [
        (1, 50000, 10), (2, 60000, 20), (3, 45000, 10),
        (4, 70000, 20), (5, 55000, 10),
    ]))
    cat.register("DEPTS", make_small_table("DEPTS", 2, [
        (10, 100), (20, 200), (30, 300),
    ]))

    return compare(
        "R1 : PUSH-DOWN des restrictions",
        "SELECT * FROM EMPLOYES, DEPTS WHERE EMPLOYES.A3 = DEPTS.A1 AND EMPLOYES.A2 > 50000",
        cat,
    )


def test_hash_join():
    """R2 : HashJoin choisi pour les grandes tables."""
    cat = Catalog()
    cat.register("GRANDE_R", make_large_table(3, 200))
    cat.register("GRANDE_S", make_large_table(2, 100))

    return compare(
        "R2 : HASH JOIN pour grandes tables",
        "SELECT * FROM GRANDE_R, GRANDE_S WHERE GRANDE_R.A1 = GRANDE_S.A1",
        cat,
    )


def test_sort_merge_join():
    """R2 : SortMergeJoin quand les tables sont de tailles comparables."""
    cat = Catalog()
    cat.register("T_A", make_large_table(3, 100))
    cat.register("T_B", make_large_table(2, 80))

    return compare(
        "R2 : SORT-MERGE JOIN pour tables de tailles comparables",
        "SELECT * FROM T_A, T_B WHERE T_A.A1 = T_B.A1",
        cat,
    )


def test_index_scan():
    """R3 : IndexScan quand un index couvre le filtre WHERE."""
    cat = Catalog()
    tbl = make_small_table("PRODUITS", 3, [
        (1, 100, 10), (2, 200, 20), (3, 100, 30),
        (4, 300, 40), (5, 100, 50),
    ])
    cat.register("PRODUITS", tbl)

    # Construire un index B+ sur la colonne 1 (prix)
    idx = BPlusTreeIndex(order=4)
    idx.build(tbl, col=1)
    cat.register_index("PRODUITS", 1, idx)

    return compare(
        "R3 : INDEX SCAN au lieu de FULL SCAN",
        "SELECT * FROM PRODUITS WHERE PRODUITS.A2 = 100",
        cat,
    )


def test_index_join():
    """R2+R3 : IndexNestedLoopJoin quand un index existe sur la jointure."""
    cat = Catalog()

    employes = make_small_table("EMP", 3, [
        (1, 50000, 10), (2, 60000, 20), (3, 45000, 10),
        (4, 70000, 30), (5, 55000, 10),
    ])
    depts = make_small_table("DEPT", 2, [
        (10, 100), (20, 200), (30, 300),
    ])
    cat.register("EMP", employes)
    cat.register("DEPT", depts)

    # Index sur DEPT.A1 (dept_id) — colonne de jointure
    idx = StaticHashIndex(nb_buckets=5)
    idx.build(depts, col=0)
    cat.register_index("DEPT", 0, idx)

    return compare(
        "R2+R3 : INDEX NESTED LOOP JOIN",
        "SELECT * FROM EMP, DEPT WHERE EMP.A3 = DEPT.A1",
        cat,
    )


def test_combined():
    """R1+R2+R3 : Toutes les optimisations combinees."""
    cat = Catalog()

    commandes = make_large_table(3, 150)
    clients = make_small_table("CLIENTS", 2, [
        (i, i * 10) for i in range(50)
    ])
    cat.register("COMMANDES", commandes)
    cat.register("CLIENTS", clients)

    # Index sur CLIENTS.A1
    idx = StaticHashIndex(nb_buckets=10)
    idx.build(clients, col=0)
    cat.register_index("CLIENTS", 0, idx)

    return compare(
        "R1+R2+R3 : TOUTES LES OPTIMISATIONS",
        "SELECT * FROM COMMANDES, CLIENTS WHERE COMMANDES.A1 = CLIENTS.A1 AND COMMANDES.A2 > 500",
        cat,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        ("R1: Push-down",        test_push_down),
        ("R2: Hash Join",        test_hash_join),
        ("R2: Sort-Merge Join",  test_sort_merge_join),
        ("R3: Index Scan",       test_index_scan),
        ("R2+R3: Index NLJ",     test_index_join),
        ("R1+R2+R3: Combine",    test_combined),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            ok = fn()
            if ok:
                passed += 1
            else:
                failed += 1
        except Exception as exc:
            print(f"\n  ERREUR dans {name}: {exc}")
            import traceback; traceback.print_exc()
            failed += 1

    print(f"\n{'=' * 70}")
    print(f"  Resultat : {passed}/{passed + failed} tests passes")
    if failed:
        print(f"  {failed} test(s) en echec")
    else:
        print("  Tous les tests sont passes !")
    print('=' * 70)
