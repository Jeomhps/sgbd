"""
Tests unitaires pour IndexNestedLoopJoin.

Schéma de test
--------------
T_dept : 5 tuples  (dept_id, budget)
    (0, 100), (1, 200), (2, 300), (3, 400), (4, 500)

T_emp  : 10 tuples (emp_id, dept_id)
    (0,0),(1,1),(2,2),(3,3),(4,4),(5,0),(6,1),(7,2),(8,3),(9,4)

Join condition : T_emp.col1 == T_dept.col0  (dept_id)
Résultat attendu : 10 lignes (chaque employé rejoint son département)
"""

import pytest

from core.TableMemoire import TableMemoire
from core.Tuple import Tuple
from core.FullScanTableMemoire import FullScanTableMemoire
from index.StaticHashIndex import StaticHashIndex
from index.BPlusTreeIndex import BPlusTreeIndex
from operators.IndexNestedLoopJoin import IndexNestedLoopJoin
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
    # (0,100),(1,200),(2,300),(3,400),(4,500)


def _make_emp() -> TableMemoire:
    return _make_table(2, [(i, i % 5) for i in range(10)])
    # (0,0),(1,1),(2,2),(3,3),(4,4),(5,0),(6,1),(7,2),(8,3),(9,4)


def _static_index_on_col0(table: TableMemoire, nb_buckets: int = 10) -> StaticHashIndex:
    idx = StaticHashIndex(nb_buckets=nb_buckets)
    idx.build(table, col=0)
    return idx


def _bplus_index_on_col0(table: TableMemoire, order: int = 4) -> BPlusTreeIndex:
    idx = BPlusTreeIndex(order=order)
    idx.build(table, col=0)
    return idx


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


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestIndexNestedLoopJoin:

    # ── équi-join de base (StaticHashIndex) ───────────────────────────────

    def test_basic_equijoin(self):
        """Chaque employé doit rejoindre exactement son département."""
        dept  = _make_dept()
        emp   = _make_emp()
        idx   = _static_index_on_col0(dept)
        scan  = FullScanTableMemoire(emp)
        join  = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows = _run(join)

        assert len(rows) == 10
        # Chaque ligne : (emp_id, dept_id_left, dept_id_right, budget)
        for row in rows:
            emp_id, dept_left, dept_right, budget = row
            assert dept_left == dept_right
            assert budget == (dept_left + 1) * 100

    # ── résultats identiques à Join classique ─────────────────────────────

    def test_same_results_as_classic_join(self):
        """IndexNLJ doit produire les mêmes tuples qu'un Join classique."""
        from operators.Join import Join

        dept = _make_dept()
        emp  = _make_emp()

        # IndexNLJ
        idx    = _static_index_on_col0(dept)
        scan_e = FullScanTableMemoire(emp)
        inlj   = IndexNestedLoopJoin(scan_e, dept, idx, left_col=1)
        rows_inlj = _run(inlj)

        # Join classique
        scan_e2 = FullScanTableMemoire(emp)
        scan_d2 = FullScanTableMemoire(dept)
        classic = Join(scan_e2, scan_d2, _left_col=1, _right_col=0)
        rows_classic = _run(classic)

        assert sorted(map(tuple, rows_inlj)) == sorted(map(tuple, rows_classic))

    # ── aucune correspondance ─────────────────────────────────────────────

    def test_no_matches(self):
        """Table gauche avec des clés absentes de l'index → 0 résultats."""
        dept = _make_dept()                          # dept_id 0..4
        emp  = _make_table(2, [(0, 99), (1, 100)])   # dept_id 99,100 inexistants
        idx  = _static_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows = _run(join)

        assert rows == []
        assert join.tuplesProduits == 0

    # ── table gauche vide ─────────────────────────────────────────────────

    def test_empty_left(self):
        """Table gauche vide → next() retourne None immédiatement."""
        dept  = _make_dept()
        emp   = TableMemoire(2)                      # vide
        idx   = _static_index_on_col0(dept)
        scan  = FullScanTableMemoire(emp)
        join  = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows = _run(join)

        assert rows == []

    # ── table droite vide (index vide) ────────────────────────────────────

    def test_empty_right(self):
        """Index construit sur table droite vide → 0 résultats."""
        dept  = TableMemoire(2)                      # vide
        emp   = _make_emp()
        idx   = _static_index_on_col0(dept)
        scan  = FullScanTableMemoire(emp)
        join  = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows = _run(join)

        assert rows == []

    # ── plusieurs correspondances par tuple gauche ────────────────────────

    def test_multiple_right_matches(self):
        """Clé dupliquée côté droit : chaque match doit être produit."""
        # Droite : 3 lignes avec dept_id=1
        right = _make_table(2, [(1, 10), (1, 20), (1, 30), (2, 99)])
        # Gauche : 2 lignes dont la clé vaut 1
        left  = _make_table(1, [(1,), (1,)])
        idx   = _static_index_on_col0(right)
        scan  = FullScanTableMemoire(left)
        join  = IndexNestedLoopJoin(scan, right, idx, left_col=0)

        rows = _run(join)

        # 2 gauches × 3 droits = 6 combinaisons
        assert len(rows) == 6
        for row in rows:
            assert row[0] == 1       # clé gauche
            assert row[1] == 1       # dept_id droit
            assert row[2] in (10, 20, 30)

    # ── composition avec Project ──────────────────────────────────────────

    def test_pipeline_with_project(self):
        """IndexNLJ peut être enchaîné avec Project."""
        dept = _make_dept()
        emp  = _make_emp()
        idx  = _static_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)
        # Projeter seulement (emp_id, budget) = colonnes 0 et 3
        proj = Project(join, [0, 3])

        rows = _run(proj)

        assert len(rows) == 10
        for emp_id, budget in rows:
            expected_budget = (emp_id % 5 + 1) * 100
            assert budget == expected_budget

    # ── composition avec Restrict ─────────────────────────────────────────

    def test_pipeline_with_restrict(self):
        """IndexNLJ + Restrict filtre correctement les résultats."""
        dept = _make_dept()
        emp  = _make_emp()
        idx  = _static_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)
        # Garder seulement budget > 200 (dept_id >= 2)
        filt = Restrict(join, _col=3, _val=200, _op=">")

        rows = _run(filt)

        assert all(row[3] > 200 for row in rows)
        assert len(rows) == 6   # dept 2,3,4 → 2 emp chacun

    # ── instrumentation ───────────────────────────────────────────────────

    def test_instrumentation(self):
        """tuplesProduits, memoire et time doivent être tracés."""
        dept = _make_dept()
        emp  = _make_emp()
        idx  = _static_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        _run(join)

        assert join.tuplesProduits == 10
        assert join.memoire        == 10 * 4    # 10 tuples × 4 colonnes
        assert join.time           > 0

    # ── avec BPlusTreeIndex ───────────────────────────────────────────────

    def test_with_bplus_index(self):
        """Vérifier que BPlusTreeIndex fonctionne comme source d'index."""
        dept = _make_dept()
        emp  = _make_emp()
        idx  = _bplus_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows = _run(join)

        assert len(rows) == 10
        for row in rows:
            assert row[1] == row[2]    # dept_id gauche == dept_id droit

    # ── idempotence open/close ────────────────────────────────────────────

    def test_reopen(self):
        """Un deuxième open() après close() doit produire les mêmes résultats."""
        dept = _make_dept()
        emp  = _make_emp()
        idx  = _static_index_on_col0(dept)
        scan = FullScanTableMemoire(emp)
        join = IndexNestedLoopJoin(scan, dept, idx, left_col=1)

        rows1 = _run(join)
        rows2 = _run(join)

        assert sorted(map(tuple, rows1)) == sorted(map(tuple, rows2))
