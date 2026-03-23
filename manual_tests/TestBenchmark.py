"""
Benchmark : grands jeux de donnees pour montrer la difference
entre plan NAIF (QueryPlanner) et plan OPTIMISE (QueryOptimizer).
"""

import time
import random

from core.TableMemoire import TableMemoire
from core.Tuple import Tuple
from index.StaticHashIndex import StaticHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from sql import Catalog, SQLParser, QueryPlanner, QueryOptimizer, Executor


# ── Generation de donnees ────────────────────────────────────────────────────

def make_table(nb_att, size, val_range=1000, seed=42):
    rng = random.Random(seed)
    tbl = TableMemoire(nb_att)
    for _ in range(size):
        t = Tuple(nb_att)
        t.val = [rng.randrange(val_range) for _ in range(nb_att)]
        tbl.valeurs.append(t)
    return tbl


def make_table_with_fk(nb_att, size, fk_col, fk_range, val_range=10000, seed=42):
    rng = random.Random(seed)
    tbl = TableMemoire(nb_att)
    for _ in range(size):
        t = Tuple(nb_att)
        t.val = [rng.randrange(val_range) for _ in range(nb_att)]
        t.val[fk_col] = rng.randrange(fk_range)
        tbl.valeurs.append(t)
    return tbl


# ── Execution ────────────────────────────────────────────────────────────────

def run_naive(sql, catalog):
    query = SQLParser(sql).parse()
    planner = QueryPlanner(catalog)
    op, plan = planner.plan(query)
    t0 = time.perf_counter()
    rows = Executor.execute(op)
    dt = (time.perf_counter() - t0) * 1000
    return rows, dt, str(plan)


def run_optimized(sql, catalog):
    query = SQLParser(sql).parse()
    optimizer = QueryOptimizer(catalog)
    op, plan = optimizer.optimize(query)
    t0 = time.perf_counter()
    rows = Executor.execute(op)
    dt = (time.perf_counter() - t0) * 1000
    return rows, dt, str(plan)


def benchmark(num, label, sql, catalog):
    sep = "-" * 70

    print(f"\n{sep}")
    print(f"  {num}. {label}")
    print(f"  SQL : {sql}")
    print(sep)

    # --- Non optimise ---
    rows_n, dt_n, plan_n = run_naive(sql, catalog)

    print(f"\n  Arbre non optimise :")
    for line in plan_n.split("\n"):
        print(f"    {line}")
    print(f"\n  Resultat  : {len(rows_n)} ligne(s)")
    print(f"  Temps     : {dt_n:.2f} ms")

    # --- Optimise ---
    rows_o, dt_o, plan_o = run_optimized(sql, catalog)

    print(f"\n  Arbre optimise :")
    for line in plan_o.split("\n"):
        print(f"    {line}")
    print(f"\n  Resultat  : {len(rows_o)} ligne(s)")
    print(f"  Temps     : {dt_o:.2f} ms")

    # --- Comparaison ---
    ok = sorted(map(tuple, rows_n)) == sorted(map(tuple, rows_o))
    speedup = dt_n / dt_o if dt_o > 0 else float("inf")
    print(f"\n  Resultats identiques : {'oui' if ok else 'NON'}")
    print(f"  Speedup              : {speedup:.1f}x")

    return ok, dt_n, dt_o


# ── Benchmarks ───────────────────────────────────────────────────────────────

def bench_push_down():
    cat = Catalog()
    cat.register("COMMANDES", make_table_with_fk(
        nb_att=3, size=5000, fk_col=2, fk_range=500, val_range=10000, seed=1,
    ))
    cat.register("PRODUITS", make_table(nb_att=2, size=500, val_range=1000, seed=2))

    return benchmark(
        1,
        "Push-down des restrictions (R1) — COMMANDES(5000) x PRODUITS(500), filtre 1%",
        "SELECT * FROM COMMANDES, PRODUITS "
        "WHERE COMMANDES.A3 = PRODUITS.A1 AND COMMANDES.A2 > 9900",
        cat,
    )


def bench_join_algorithm():
    cat = Catalog()
    cat.register("VENTES", make_table_with_fk(
        nb_att=3, size=3000, fk_col=2, fk_range=500, val_range=10000, seed=10,
    ))
    cat.register("CLIENTS", make_table(nb_att=2, size=2000, val_range=500, seed=20))

    return benchmark(
        2,
        "Choix algorithme jointure (R2) — VENTES(3000) x CLIENTS(2000)",
        "SELECT * FROM VENTES, CLIENTS WHERE VENTES.A3 = CLIENTS.A1",
        cat,
    )


def bench_index_scan():
    cat = Catalog()
    tbl = make_table(nb_att=3, size=10000, val_range=1000, seed=30)
    cat.register("STOCKS", tbl)

    idx = BPlusTreeIndex(order=50)
    idx.build(tbl, col=1)
    cat.register_index("STOCKS", 1, idx)

    return benchmark(
        3,
        "Index Scan (R3) — STOCKS(10000) avec index B+ sur A2",
        "SELECT * FROM STOCKS WHERE STOCKS.A2 = 42",
        cat,
    )


def bench_index_join():
    cat = Catalog()
    cat.register("FACTURES", make_table_with_fk(
        nb_att=3, size=3000, fk_col=2, fk_range=1000, val_range=10000, seed=40,
    ))
    fournisseurs = make_table(nb_att=2, size=1000, val_range=5000, seed=50)
    cat.register("FOURNISSEURS", fournisseurs)

    idx = StaticHashIndex(nb_buckets=200)
    idx.build(fournisseurs, col=0)
    cat.register_index("FOURNISSEURS", 0, idx)

    return benchmark(
        4,
        "Index Nested Loop Join (R2+R3) — FACTURES(3000) x FOURNISSEURS(1000)",
        "SELECT * FROM FACTURES, FOURNISSEURS "
        "WHERE FACTURES.A3 = FOURNISSEURS.A1",
        cat,
    )


def bench_combined():
    cat = Catalog()
    cat.register("EMPLOYES", make_table_with_fk(
        nb_att=3, size=5000, fk_col=2, fk_range=200, val_range=100000, seed=60,
    ))
    departements = make_table(nb_att=2, size=200, val_range=5000, seed=70)
    cat.register("DEPARTEMENTS", departements)

    idx = StaticHashIndex(nb_buckets=50)
    idx.build(departements, col=0)
    cat.register_index("DEPARTEMENTS", 0, idx)

    return benchmark(
        5,
        "Toutes les optimisations (R1+R2+R3) — EMPLOYES(5000) x DEPARTEMENTS(200)",
        "SELECT * FROM EMPLOYES, DEPARTEMENTS "
        "WHERE EMPLOYES.A3 = DEPARTEMENTS.A1 AND EMPLOYES.A2 > 99000",
        cat,
    )


def bench_scalability():
    print(f"\n{'=' * 70}")
    print(f"  6. Scalabilite — jointure + filtre, taille croissante")
    print(f"{'=' * 70}")

    sizes = [500, 1000, 2000, 4000]

    print(f"\n  {'Taille':>8} | {'Naif (ms)':>12} | {'Optimise (ms)':>14} | {'Speedup':>8}")
    print(f"  {'-' * 8}-+-{'-' * 12}-+-{'-' * 14}-+-{'-' * 8}")

    for n in sizes:
        cat = Catalog()
        cat.register("T_LEFT", make_table_with_fk(
            nb_att=3, size=n, fk_col=2, fk_range=100, val_range=10000, seed=100,
        ))
        cat.register("T_RIGHT", make_table(
            nb_att=2, size=max(100, n // 5), val_range=100, seed=200,
        ))

        sql = ("SELECT * FROM T_LEFT, T_RIGHT "
               "WHERE T_LEFT.A3 = T_RIGHT.A1 AND T_LEFT.A2 > 9500")

        _, dt_n, _ = run_naive(sql, cat)
        _, dt_o, _ = run_optimized(sql, cat)
        speedup = dt_n / dt_o if dt_o > 0 else float("inf")
        print(f"  {n:>8} | {dt_n:>12.2f} | {dt_o:>14.2f} | {speedup:>7.1f}x")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  BENCHMARK : NAIF vs OPTIMISE sur grands jeux de donnees")
    print("=" * 70)

    benchmarks = [
        ("R1: Push-down",        bench_push_down),
        ("R2: Join algorithm",   bench_join_algorithm),
        ("R3: Index Scan",       bench_index_scan),
        ("R2+R3: Index Join",    bench_index_join),
        ("R1+R2+R3: Combine",    bench_combined),
    ]

    summary = []

    for name, fn in benchmarks:
        try:
            ok, dt_n, dt_o = fn()
            speedup = dt_n / dt_o if dt_o > 0 else float("inf")
            summary.append((name, dt_n, dt_o, speedup, ok))
        except Exception as exc:
            print(f"\n  ERREUR dans {name}: {exc}")
            import traceback; traceback.print_exc()
            summary.append((name, 0, 0, 0, False))

    bench_scalability()

    # Recapitulatif
    print(f"\n\n{'=' * 70}")
    print("  RECAPITULATIF")
    print('=' * 70)
    print(f"\n  {'Test':.<30} {'Naif':>10} {'Optimise':>10} {'Speedup':>10}")
    print(f"  {'─' * 62}")
    for name, dt_n, dt_o, sp, ok in summary:
        print(f"  {name:.<30} {dt_n:>9.2f}ms {dt_o:>9.2f}ms {sp:>9.1f}x")
    print('=' * 70)
