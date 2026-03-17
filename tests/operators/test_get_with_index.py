"""
Tests unitaires pour GetWithIndex.

GetWithIndex est l'opérateur d'accès direct par bloc via un index.
Il interroge l'index pour obtenir des n° de blocs, lit chaque bloc
directement, puis filtre les tuples correspondants.

Schéma de test
--------------
T_dept  : 5 tuples  (dept_id, budget)
    (0, 100), (1, 200), (2, 300), (3, 400), (4, 500)
    block_size=1 (TableMemoire) → chaque tuple est dans son propre "bloc".
"""

import pytest

from core.TableMemoire import TableMemoire
from core.Tuple import Tuple
from index.StaticHashIndex import StaticHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from operators.GetWithIndex import GetWithIndex
from operators.Project import Project
from operators.Restrict import Restrict


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_table(nb_att: int, rows: list) -> TableMemoire:
    tbl = TableMemoire(nb_att)
    for vals in rows:
        t = Tuple(nb_att)
        t.val = list(vals)
        tbl.valeurs.append(t)
    return tbl


def _make_dept() -> TableMemoire:
    return _make_table(2, [(i, (i + 1) * 100) for i in range(5)])


def _run(op) -> list[list]:
    op.open()
    rows = []
    while True:
        t = op.next()
        if t is None:
            break
        rows.append(list(t.val))
    op.close()
    return rows


def _static_idx(table: TableMemoire, col: int = 0) -> StaticHashIndex:
    idx = StaticHashIndex(nb_buckets=10)
    idx.build(table, col=col)
    return idx


def _bplus_idx(table: TableMemoire, col: int = 0) -> BPlusTreeIndex:
    idx = BPlusTreeIndex(order=4)
    idx.build(table, col=col)
    return idx


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestGetWithIndex:

    # ── recherche exacte (StaticHashIndex) ───────────────────────────────

    def test_basic_search(self):
        """Recherche d'un dept_id existant → 1 tuple retourné."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=2)

        rows = _run(op)

        assert len(rows) == 1
        assert rows[0] == [2, 300]

    def test_search_first(self):
        """Recherche du premier tuple (dept_id=0)."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=0)

        rows = _run(op)

        assert len(rows) == 1
        assert rows[0] == [0, 100]

    def test_search_last(self):
        """Recherche du dernier tuple (dept_id=4)."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=4)

        rows = _run(op)

        assert len(rows) == 1
        assert rows[0] == [4, 500]

    # ── valeur absente ────────────────────────────────────────────────────

    def test_value_not_found(self):
        """Valeur absente de la table → 0 résultats."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=99)

        rows = _run(op)

        assert rows == []
        assert op.tuplesProduits == 0

    # ── table vide ───────────────────────────────────────────────────────

    def test_empty_table(self):
        """Table vide → 0 résultats."""
        table = TableMemoire(2)
        idx   = _static_idx(table)
        op    = GetWithIndex(table, idx, value=1)

        rows = _run(op)

        assert rows == []

    # ── plusieurs tuples avec la même clé ────────────────────────────────

    def test_duplicate_keys(self):
        """Plusieurs tuples avec la même clé → tous retournés."""
        table = _make_table(2, [(5, 10), (5, 20), (5, 30), (9, 99)])
        idx   = _static_idx(table)
        op    = GetWithIndex(table, idx, value=5)

        rows = _run(op)

        assert len(rows) == 3
        assert all(row[0] == 5 for row in rows)
        assert sorted(row[1] for row in rows) == [10, 20, 30]

    # ── avec BPlusTreeIndex ───────────────────────────────────────────────

    def test_with_bplus_exact(self):
        """Recherche exacte avec BPlusTreeIndex."""
        dept = _make_dept()
        idx  = _bplus_idx(dept)
        op   = GetWithIndex(dept, idx, value=3)

        rows = _run(op)

        assert len(rows) == 1
        assert rows[0] == [3, 400]

    def test_with_bplus_range(self):
        """Recherche par intervalle avec BPlusTreeIndex."""
        dept = _make_dept()
        idx  = _bplus_idx(dept)
        # dept_id dans [1, 3] → budgets 200, 300, 400
        op   = GetWithIndex(dept, idx, value=1, high=3)

        rows = _run(op)

        assert len(rows) == 3
        budgets = sorted(row[1] for row in rows)
        assert budgets == [200, 300, 400]

    def test_with_bplus_gte(self):
        """Opérateur >= avec BPlusTreeIndex."""
        dept = _make_dept()
        idx  = _bplus_idx(dept)
        op   = GetWithIndex(dept, idx, value=3, op=">=")

        rows = _run(op)

        assert len(rows) == 2
        assert sorted(row[0] for row in rows) == [3, 4]

    def test_with_bplus_lt(self):
        """Opérateur < avec BPlusTreeIndex."""
        dept = _make_dept()
        idx  = _bplus_idx(dept)
        op   = GetWithIndex(dept, idx, value=2, op="<")

        rows = _run(op)

        assert len(rows) == 2
        assert all(row[0] < 2 for row in rows)

    # ── instrumentation ──────────────────────────────────────────────────

    def test_instrumentation(self):
        """tuplesProduits, memoire et time sont tracés."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=2)

        _run(op)

        assert op.tuplesProduits == 1
        assert op.memoire        == 1 * 2   # 1 tuple × 2 colonnes
        assert op.time           > 0

    # ── idempotence open/close ────────────────────────────────────────────

    def test_reopen(self):
        """Un deuxième open() après close() produit les mêmes résultats."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=1)

        rows1 = _run(op)
        rows2 = _run(op)

        assert rows1 == rows2

    # ── composition avec Project ──────────────────────────────────────────

    def test_pipeline_with_project(self):
        """GetWithIndex peut être enchaîné avec Project."""
        dept = _make_dept()
        idx  = _static_idx(dept)
        op   = GetWithIndex(dept, idx, value=2)
        proj = Project(op, [1])   # seulement le budget

        rows = _run(proj)

        assert rows == [[300]]
