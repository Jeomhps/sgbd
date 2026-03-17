"""
Tests des trois types d'index sur TableDisque.

Table de test
-------------
T : 3 colonnes (id, valeur, dept)  -  100 tuples deterministes
    col 0 (id)     : 0 ... 99
    col 1 (valeur) : 0 ... 9  (valeur = id % 10)
    col 2 (dept)   : 0 ... 4  (dept   = id % 5)
"""

import os
import struct

from core.TableDisque import TableDisque
from index.StaticHashIndex import StaticHashIndex
from index.DynamicHashIndex import DynamicHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from operators.IndexScan import IndexScan

# ------------------------------------------------------------------------------
# Construction de la table deterministe
# ------------------------------------------------------------------------------

TABLE_FILE = "/tmp/test_index_table.dat"
NB_TUPLES  = 100
TUPLE_SIZE = 3


def _build_table() -> TableDisque:
    with open(TABLE_FILE, "wb") as f:
        f.write(struct.pack("II", NB_TUPLES, TUPLE_SIZE))
        for i in range(NB_TUPLES):
            f.write(struct.pack("iii", i, i % 10, i % 5))
    tbl = TableDisque(TABLE_FILE)
    tbl.tuple_size = TUPLE_SIZE
    tbl.table_size = NB_TUPLES
    return tbl


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _check(label: str, got, expected) -> None:
    ok = sorted(got) == sorted(expected)
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {label}")
    if not ok:
        print(f"     attendu : {sorted(expected)}")
        print(f"     obtenu  : {sorted(got)}")


def _fetch_indices(table: TableDisque, indices: list) -> list:
    table.open()
    rows = []
    for idx in indices:
        t = table.get_tuple(idx)
        if t:
            rows.append(list(t.val))
    table.close()
    return rows


def _scan_via_indexscan(table, index, value, op="==", high=None) -> list:
    scan = IndexScan(table, index, value, op=op, high=high)
    rows = []
    scan.open()
    while True:
        t = scan.next()
        if t is None:
            break
        rows.append(list(t.val))
    scan.close()
    return rows


# ------------------------------------------------------------------------------
# Tests StaticHashIndex
# ------------------------------------------------------------------------------

def test_static_hash() -> None:
    print("=== StaticHashIndex ===")
    tbl = _build_table()
    idx = StaticHashIndex(nb_buckets=20)
    idx.build(tbl, col=1)
    print(f"  {idx.stats()}")

    # col1=3 -> ids {3,13,23,33,43,53,63,73,83,93}
    indices = idx.search(3)
    rows    = _fetch_indices(tbl, indices)
    expected_ids = [i for i in range(NB_TUPLES) if i % 10 == 3]
    _check("search(col1=3)", [r[0] for r in rows], expected_ids)

    # Via IndexScan
    rows2 = _scan_via_indexscan(tbl, idx, value=7)
    expected_ids2 = [i for i in range(NB_TUPLES) if i % 10 == 7]
    _check("IndexScan(col1=7)", [r[0] for r in rows2], expected_ids2)

    # Aucun resultat
    _check("search(col1=99) -> vide", idx.search(99), [])


# ------------------------------------------------------------------------------
# Tests DynamicHashIndex
# ------------------------------------------------------------------------------

def test_dynamic_hash() -> None:
    print("=== DynamicHashIndex ===")
    tbl = _build_table()
    idx = DynamicHashIndex(bucket_capacity=6)
    idx.build(tbl, col=2)
    print(f"  {idx.stats()}")

    # dept=2 -> ids {2,7,12,...,97}
    indices  = idx.search(2)
    rows     = _fetch_indices(tbl, indices)
    expected = [i for i in range(NB_TUPLES) if i % 5 == 2]
    _check("search(col2=2)", [r[0] for r in rows], expected)

    rows2    = _scan_via_indexscan(tbl, idx, value=0)
    expected2 = [i for i in range(NB_TUPLES) if i % 5 == 0]
    _check("IndexScan(col2=0)", [r[0] for r in rows2], expected2)

    _check("search(col2=9) -> vide", idx.search(9), [])


# ------------------------------------------------------------------------------
# Tests BPlusTreeIndex - recherche exacte
# ------------------------------------------------------------------------------

def test_bplus_exact() -> None:
    print("=== BPlusTreeIndex - recherche exacte ===")
    tbl = _build_table()
    idx = BPlusTreeIndex(order=5)
    idx.build(tbl, col=0)
    print(f"  {idx.stats()}")

    indices = idx.search(42)
    rows    = _fetch_indices(tbl, indices)
    _check("search(col0=42)", [r[0] for r in rows], [42])

    _check("search(col0=0)",  [r[0] for r in _fetch_indices(tbl, idx.search(0))],  [0])
    _check("search(col0=999) -> vide", idx.search(999), [])

    rows2 = _scan_via_indexscan(tbl, idx, value=77)
    _check("IndexScan(col0=77)", [r[0] for r in rows2], [77])


# ------------------------------------------------------------------------------
# Tests BPlusTreeIndex - requete par intervalle
# ------------------------------------------------------------------------------

def test_bplus_range() -> None:
    print("=== BPlusTreeIndex - requete par intervalle ===")
    tbl = _build_table()
    idx = BPlusTreeIndex(order=4)
    idx.build(tbl, col=0)

    indices = idx.range_search(10, 15)
    rows    = _fetch_indices(tbl, indices)
    _check("range_search([10,15])", [r[0] for r in rows], list(range(10, 16)))

    indices2 = idx.range_search(0, 4)
    rows2    = _fetch_indices(tbl, indices2)
    _check("range_search([0,4])", [r[0] for r in rows2], list(range(5)))

    indices3 = idx.range_search(95, 99)
    rows3    = _fetch_indices(tbl, indices3)
    _check("range_search([95,99])", [r[0] for r in rows3], list(range(95, 100)))

    rows4 = _scan_via_indexscan(tbl, idx, value=20, high=25)
    _check("IndexScan(range [20,25])", [r[0] for r in rows4], list(range(20, 26)))


# ------------------------------------------------------------------------------
# Test B+Tree avec cles dupliquees
# ------------------------------------------------------------------------------

def test_bplus_duplicates() -> None:
    print("=== BPlusTreeIndex - cles dupliquees ===")
    tbl = _build_table()
    idx = BPlusTreeIndex(order=4)
    idx.build(tbl, col=1)

    indices  = idx.search(5)
    rows     = _fetch_indices(tbl, indices)
    expected = [i for i in range(NB_TUPLES) if i % 10 == 5]
    _check("search(col1=5) - duplique", sorted([r[0] for r in rows]), sorted(expected))

    indices2 = idx.range_search(0, 2)
    rows2    = _fetch_indices(tbl, indices2)
    expected2 = [i for i in range(NB_TUPLES) if i % 10 in (0, 1, 2)]
    _check("range_search([0,2]) - duplique", [r[0] for r in rows2], expected2)


# ------------------------------------------------------------------------------
# Comparaison FullScan vs IndexScan
# ------------------------------------------------------------------------------

def test_perf_comparison() -> None:
    print("=== Comparaison FullScan vs IndexScan ===")
    import time
    from core.FullScanTableDisque import FullScanTableDisque
    from operators.Restrict import Restrict

    tbl = _build_table()

    # FullScan + Restrict
    t0 = time.perf_counter()
    scan     = FullScanTableDisque(tbl)
    restrict = Restrict(scan, 1, 7, "==")
    restrict.open()
    fs_rows = []
    while True:
        t = restrict.next()
        if t is None:
            break
        fs_rows.append(t.val[0])
    restrict.close()
    t1 = time.perf_counter()

    # Build + IndexScan (StaticHash)
    idx = StaticHashIndex(nb_buckets=15)
    idx.build(tbl, col=1)
    t2 = time.perf_counter()
    is_rows = [r[0] for r in _scan_via_indexscan(tbl, idx, value=7)]
    t3 = time.perf_counter()

    print(f"  FullScan+Restrict : {(t1-t0)*1000:.3f} ms  -> {len(fs_rows)} tuples")
    print(f"  Build StaticHash  : {(t2-t1)*1000:.3f} ms")
    print(f"  IndexScan         : {(t3-t2)*1000:.3f} ms  -> {len(is_rows)} tuples")
    _check("Memes resultats", sorted(fs_rows), sorted(is_rows))


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_static_hash,
        test_dynamic_hash,
        test_bplus_exact,
        test_bplus_range,
        test_bplus_duplicates,
        test_perf_comparison,
    ]

    passed = failed = 0
    for fn in tests:
        try:
            fn()
            passed += 1
        except Exception as exc:
            print(f"  ERREUR dans {fn.__name__}: {exc}")
            import traceback; traceback.print_exc()
            failed += 1
        print()

    if os.path.exists(TABLE_FILE):
        os.remove(TABLE_FILE)

    print("=" * 60)
    print(f"Resultat : {passed}/{passed+failed} tests passes")
    if failed:
        print(f"ATTENTION : {failed} test(s) en echec")
    else:
        print("Tous les tests sont passes !")
