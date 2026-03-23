"""
Tests unitaires pour SortMergeJoin.

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
from operators.SortMergeJoin import SortMergeJoin
from operators.Join import Join
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


def _scan(table: TableMemoire) -> FullScanTableMemoire:
    return FullScanTableMemoire(table)


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

class TestSortMergeJoin:

    # ── équi-join de base ─────────────────────────────────────────────────

    def test_basic_equijoin(self):
        """Chaque employé doit rejoindre exactement son département."""
        join = SortMergeJoin(_scan(_make_emp()), _scan(_make_dept()),
                             left_col=1, right_col=0)
        rows = _run(join)

        assert len(rows) == 10
        for row in rows:
            emp_id, dept_left, dept_right, budget = row
            assert dept_left == dept_right
            assert budget == (dept_left + 1) * 100

    # ── résultats identiques au Join classique ────────────────────────────

    def test_same_results_as_classic_join(self):
        """SortMergeJoin doit produire les mêmes tuples qu'un Join classique."""
        emp  = _make_emp()
        dept = _make_dept()

        smj  = SortMergeJoin(_scan(emp), _scan(dept), left_col=1, right_col=0)
        rows_smj = _run(smj)

        classic  = Join(_scan(emp), _scan(dept), _left_col=1, _right_col=0)
        rows_classic = _run(classic)

        assert sorted(map(tuple, rows_smj)) == sorted(map(tuple, rows_classic))

    # ── aucune correspondance ─────────────────────────────────────────────

    def test_no_matches(self):
        """Clés sans intersection → 0 résultats."""
        left  = _make_table(1, [(99,), (100,)])   # clés 99, 100
        right = _make_table(1, [(0,),  (1,)])     # clés 0, 1
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        assert rows == []
        assert join.tuplesProduits == 0

    # ── table gauche vide ─────────────────────────────────────────────────

    def test_empty_left(self):
        """Table gauche vide → next() retourne None immédiatement."""
        left  = TableMemoire(2)      # vide
        right = _make_dept()
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        assert rows == []

    # ── table droite vide ─────────────────────────────────────────────────

    def test_empty_right(self):
        """Table droite vide → 0 résultats."""
        left  = _make_emp()
        right = TableMemoire(2)      # vide
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=1, right_col=0)

        rows = _run(join)

        assert rows == []

    # ── doublons côté gauche (replay du groupe S) ─────────────────────────

    def test_duplicate_left_keys(self):
        """Plusieurs tuples gauches avec la même clé → groupe S rejoué pour chacun."""
        # Gauche : 3 tuples avec clé=5
        left  = _make_table(1, [(5,), (5,), (5,)])
        # Droite : 2 tuples avec clé=5
        right = _make_table(2, [(5, 10), (5, 20), (9, 99)])
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        # 3 gauches × 2 droits = 6 combinaisons
        assert len(rows) == 6
        for row in rows:
            assert row[0] == 5
            assert row[1] == 5
            assert row[2] in (10, 20)

    # ── doublons côté droit ───────────────────────────────────────────────

    def test_duplicate_right_keys(self):
        """Plusieurs tuples droits avec la même clé → tous émis."""
        left  = _make_table(1, [(1,)])
        right = _make_table(2, [(1, 10), (1, 20), (1, 30)])
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        assert len(rows) == 3
        values = [row[2] for row in rows]
        assert sorted(values) == [10, 20, 30]

    # ── doublons des deux côtés (produit cartésien du groupe) ─────────────

    def test_duplicates_both_sides(self):
        """Clé dupliquée des deux côtés → produit cartésien des groupes."""
        left  = _make_table(1, [(7,), (7,)])         # 2 gauches avec clé=7
        right = _make_table(2, [(7, 10), (7, 20)])   # 2 droits  avec clé=7
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        assert len(rows) == 4   # 2 × 2

    # ── entrées non triées ────────────────────────────────────────────────

    def test_unsorted_input(self):
        """SortMergeJoin doit trier lui-même ses entrées."""
        # Données dans le désordre
        left  = _make_table(1, [(3,), (1,), (2,)])
        right = _make_table(2, [(2, 200), (1, 100), (3, 300)])
        join  = SortMergeJoin(_scan(left), _scan(right), left_col=0, right_col=0)

        rows = _run(join)

        assert len(rows) == 3
        key_pairs = [(row[0], row[1]) for row in rows]
        for k_left, k_right in key_pairs:
            assert k_left == k_right

    # ── composition avec Project ──────────────────────────────────────────

    def test_pipeline_with_project(self):
        """SortMergeJoin peut être enchaîné avec Project."""
        emp  = _make_emp()
        dept = _make_dept()
        join = SortMergeJoin(_scan(emp), _scan(dept), left_col=1, right_col=0)
        proj = Project(join, [0, 3])   # (emp_id, budget)

        rows = _run(proj)

        assert len(rows) == 10
        for emp_id, budget in rows:
            expected_budget = (emp_id % 5 + 1) * 100
            assert budget == expected_budget

    # ── composition avec Restrict ─────────────────────────────────────────

    def test_pipeline_with_restrict(self):
        """SortMergeJoin + Restrict filtre correctement les résultats."""
        emp  = _make_emp()
        dept = _make_dept()
        join = SortMergeJoin(_scan(emp), _scan(dept), left_col=1, right_col=0)
        filt = Restrict(join, _col=3, _val=200, _op=">")   # budget > 200

        rows = _run(filt)

        assert all(row[3] > 200 for row in rows)
        assert len(rows) == 6   # dept 2,3,4 → 2 emp chacun

    # ── instrumentation ───────────────────────────────────────────────────

    def test_instrumentation(self):
        """tuplesProduits, memoire et time doivent être tracés."""
        emp  = _make_emp()
        dept = _make_dept()
        join = SortMergeJoin(_scan(emp), _scan(dept), left_col=1, right_col=0)

        _run(join)

        assert join.tuplesProduits == 10
        assert join.memoire        == 10 * 4    # 10 tuples × 4 colonnes
        assert join.time           > 0

    # ── idempotence open/close ────────────────────────────────────────────

    def test_reopen(self):
        """Un deuxième open() après close() doit produire les mêmes résultats."""
        emp  = _make_emp()
        dept = _make_dept()
        join = SortMergeJoin(_scan(emp), _scan(dept), left_col=1, right_col=0)

        rows1 = _run(join)
        rows2 = _run(join)

        assert sorted(map(tuple, rows1)) == sorted(map(tuple, rows2))
