"""
Benchmark : grands jeux de donnees sur disque (.dat)
Compare le plan NAIF (QueryPlanner) et le plan OPTIMISE (QueryOptimizer).

Prerequis : lancer d'abord  tools/create_benchmark_data.py
pour generer les fichiers dans  data/
"""

import os
import time

from core.TableDisque import TableDisque
from index.StaticHashIndex import StaticHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from sql import Catalog, SQLParser, QueryPlanner, QueryOptimizer, Executor

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")


# ── Chargement des tables disque ─────────────────────────────────────────────

def load_table(filename, block_size=4, memory_blocks=3):
    """Charge une TableDisque depuis un fichier .dat."""
    path = os.path.join(DATA_DIR, filename)
    tbl = TableDisque(path, block_size=block_size, memory_blocks=memory_blocks)
    tbl.open()
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
    """R1 : Push-down des restrictions avant la jointure."""
    cat = Catalog()
    cat.register("COMMANDES", load_table("commandes.dat"))   # 10000 x 3
    cat.register("PRODUITS", load_table("produits.dat"))      #  1000 x 2

    return benchmark(
        1,
        "Push-down des restrictions (R1) — COMMANDES(10000) x PRODUITS(1000), filtre ~1%",
        "SELECT * FROM COMMANDES, PRODUITS "
        "WHERE COMMANDES.A3 = PRODUITS.A1 AND COMMANDES.A2 > 9900",
        cat,
    )


def bench_join_algorithm():
    """R2 : Hash/SortMerge au lieu de Nested Loop."""
    cat = Catalog()
    cat.register("VENTES", load_table("ventes.dat"))    # 5000 x 3
    cat.register("CLIENTS", load_table("clients.dat"))  # 3000 x 2

    return benchmark(
        2,
        "Choix algorithme jointure (R2) — VENTES(5000) x CLIENTS(3000)",
        "SELECT * FROM VENTES, CLIENTS WHERE VENTES.A3 = CLIENTS.A1",
        cat,
    )


def bench_index_scan():
    """R3 : IndexScan au lieu de FullScan+Restrict."""
    cat = Catalog()
    tbl = load_table("stocks.dat")  # 10000 x 3
    cat.register("STOCKS", tbl)

    # Index B+ sur colonne 1 (categorie)
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
    """R2+R3 : Index Nested Loop Join."""
    cat = Catalog()
    cat.register("FACTURES", load_table("factures.dat"))          # 5000 x 3
    fournisseurs = load_table("fournisseurs.dat")                  # 1000 x 2
    cat.register("FOURNISSEURS", fournisseurs)

    idx = StaticHashIndex(nb_buckets=200)
    idx.build(fournisseurs, col=0)
    cat.register_index("FOURNISSEURS", 0, idx)

    return benchmark(
        4,
        "Index Nested Loop Join (R2+R3) — FACTURES(5000) x FOURNISSEURS(1000)",
        "SELECT * FROM FACTURES, FOURNISSEURS "
        "WHERE FACTURES.A3 = FOURNISSEURS.A1",
        cat,
    )


def bench_combined():
    """R1+R2+R3 : Toutes les optimisations."""
    cat = Catalog()
    cat.register("EMPLOYES", load_table("employes.dat"))           # 8000 x 3
    departements = load_table("departements.dat")                   #  200 x 2
    cat.register("DEPARTEMENTS", departements)

    idx = StaticHashIndex(nb_buckets=50)
    idx.build(departements, col=0)
    cat.register_index("DEPARTEMENTS", 0, idx)

    return benchmark(
        5,
        "Toutes les optimisations (R1+R2+R3) — EMPLOYES(8000) x DEPARTEMENTS(200)",
        "SELECT * FROM EMPLOYES, DEPARTEMENTS "
        "WHERE EMPLOYES.A3 = DEPARTEMENTS.A1 AND EMPLOYES.A2 > 99000",
        cat,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Verifier que les donnees existent
    if not os.path.isdir(DATA_DIR):
        print(f"Erreur : le dossier {DATA_DIR} n'existe pas.")
        print("Lancer d'abord :  PYTHONPATH=. uv run python tools/create_benchmark_data.py")
        raise SystemExit(1)

    print("=" * 70)
    print("  BENCHMARK : NAIF vs OPTIMISE sur tables disque (.dat)")
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

    # Recapitulatif
    print(f"\n\n{'=' * 70}")
    print("  RECAPITULATIF")
    print('=' * 70)
    print(f"\n  {'Test':.<30} {'Naif':>10} {'Optimise':>10} {'Speedup':>10}")
    print(f"  {'─' * 62}")
    for name, dt_n, dt_o, sp, ok in summary:
        print(f"  {name:.<30} {dt_n:>9.2f}ms {dt_o:>9.2f}ms {sp:>9.1f}x")
    print('=' * 70)
