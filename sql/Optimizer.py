"""
Optimiseur de requetes — transforme un plan naif en plan optimise.

Regles d'optimisation appliquees
---------------------------------
R1 — Descente des restrictions (push-down)
    Les filtres WHERE portant sur une seule table sont pousses
    AVANT la jointure, pour reduire le nombre de tuples a joindre.

R2 — Choix de l'algorithme de jointure
    En fonction de la taille des tables et de la disponibilite d'index :
      • IndexNestedLoopJoin  si un index existe sur la colonne de jointure droite
      • HashJoin             si les tables sont grandes (> HASH_THRESHOLD)
      • SortMergeJoin        si les deux tables sont grandes et de tailles comparables
      • NestedLoopJoin       par defaut (petites tables)

R3 — Acces par index (IndexScan)
    Quand un filtre WHERE porte sur une colonne indexee,
    remplacer le FullScan par un IndexScan.

Architecture
------------
L'optimiseur se place entre le Parser et l'ancien Planner :

    SQL -> Parser -> AST -> Optimizer -> Operateur + PlanNode
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Union

from sql.Catalog import Catalog
from sql.Parser import AggExpr, ColumnRef, Condition, SelectQuery

from core.FullScanTableMemoire import FullScanTableMemoire
from core.FullScanTableDisque import FullScanTableDisque
from core.TableDisque import TableDisque
from core.Operateur import Operateur

from operators.Aggregate import Aggregate
from operators.Join import Join
from operators.HashJoin import HashJoin
from operators.SortMergeJoin import SortMergeJoin
from operators.IndexNestedLoopJoin import IndexNestedLoopJoin
from operators.IndexScan import IndexScan
from operators.Project import Project
from operators.Restrict import Restrict


# ── Seuils pour le choix de l'algorithme de jointure ─────────────────────────

HASH_THRESHOLD = 50       # HashJoin si une table depasse ce seuil
SORT_MERGE_RATIO = 3.0    # SortMerge si ratio tailles < ce seuil


# ── Plan d'execution lisible ────────────────────────────────────────────────

@dataclass
class PlanNode:
    """Noeud de l'arbre de plan (pour affichage)."""
    label:    str
    children: List["PlanNode"] = field(default_factory=list)

    def __str__(self, indent: int = 0) -> str:
        prefix = "  " * indent
        connector = "└─ " if indent > 0 else ""
        lines = [prefix + connector + self.label]
        for child in self.children:
            lines.append(child.__str__(indent + 1))
        return "\n".join(lines)


class OptimizerError(Exception):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Optimiseur
# ──────────────────────────────────────────────────────────────────────────────

class QueryOptimizer:
    """
    Optimiseur de requetes SQL.

    Applique les regles R1 (push-down), R2 (choix jointure), R3 (index scan)
    pour produire un plan d'execution performant.
    """

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    # ── API publique ─────────────────────────────────────────────────────

    def optimize(self, query: SelectQuery) -> tuple[Operateur, PlanNode]:
        """
        Optimise et construit l'arbre d'operateurs + le plan lisible.
        """
        # 1. Valider les tables
        for t in query.tables:
            if self.catalog.get_table(t) is None:
                raise OptimizerError(f"Table inconnue : '{t}'")

        # 2. Calculer les offsets de colonnes
        offsets = self._compute_offsets(query.tables)

        # 3. Classer les conditions WHERE
        join_conds   = [c for c in query.conditions if c.is_join()]
        filter_conds = [c for c in query.conditions if not c.is_join()]

        # ── R1 : Descente des restrictions ────────────────────────────────
        # Separer les filtres en "pre-jointure" (sur une seule table)
        # et "post-jointure" (sur le tuple combine)
        pre_filters, post_filters = self._classify_filters(
            filter_conds, query.tables, offsets
        )

        # 4. Construire l'arbre scan/jointure optimise
        op, plan = self._build_optimized_tree(
            query.tables, join_conds, pre_filters, offsets
        )

        # 5. Appliquer les filtres post-jointure restants
        for cond in post_filters:
            col_idx = self._resolve_col(cond.left, offsets)
            val = self._coerce_value(cond.right)
            op = Restrict(op, col_idx, val, cond.op)
            plan = PlanNode(
                f"Restrict(col={col_idx}, val={val!r}, op='{cond.op}')",
                [plan],
            )

        # 6a. Agregation
        agg_exprs = [c for c in query.columns if isinstance(c, AggExpr)]
        if agg_exprs:
            agg = agg_exprs[0]
            agg_col = self._resolve_col(agg.col, offsets)
            group_cols = (
                [self._resolve_col(c, offsets) for c in query.group_by]
                if query.group_by else None
            )
            op = Aggregate(op, agg_col, agg.func, group_cols)
            plan = PlanNode(
                f"Aggregate(func={agg.func}, col={agg_col}, group_by={group_cols})",
                [plan],
            )

        # 6b. Projection
        elif "*" not in query.columns:
            proj_indices = [
                self._resolve_col(c, offsets)
                for c in query.columns
                if not isinstance(c, AggExpr)
            ]
            op = Project(op, proj_indices)
            plan = PlanNode(f"Project(cols={proj_indices})", [plan])

        return op, plan

    # ── R1 : Classification des filtres ──────────────────────────────────

    def _classify_filters(
        self,
        filters: list[Condition],
        tables:  list[str],
        offsets: dict[str, int],
    ) -> tuple[dict[str, list[Condition]], list[Condition]]:
        """
        Separe les filtres en :
          - pre_filters  : {TABLE_NAME -> [conditions]} — poussables avant jointure
          - post_filters : [conditions] — a appliquer apres jointure
        """
        pre:  dict[str, list[Condition]] = {t.upper(): [] for t in tables}
        post: list[Condition] = []

        for cond in filters:
            table = self._owning_table(cond.left, tables, offsets)
            if table is not None:
                pre[table].append(cond)
            else:
                post.append(cond)

        return pre, post

    def _owning_table(
        self,
        col_ref: ColumnRef,
        tables:  list[str],
        offsets: dict[str, int],
    ) -> Optional[str]:
        """Retourne le nom de la table qui possede la colonne, ou None."""
        if col_ref.table:
            upper = col_ref.table.upper()
            return upper if upper in offsets else None

        # Colonne sans qualificateur : determiner par offset
        col_idx = self._resolve_col(col_ref, offsets)
        for t in tables:
            t_upper = t.upper()
            start = offsets[t_upper]
            end = start + self.catalog.get_nb_att(t)
            if start <= col_idx < end:
                return t_upper
        return None

    # ── Construction de l'arbre optimise ─────────────────────────────────

    def _build_optimized_tree(
        self,
        tables:      list[str],
        join_conds:  list[Condition],
        pre_filters: dict[str, list[Condition]],
        offsets:     dict[str, int],
    ) -> tuple[Operateur, PlanNode]:
        """Construit l'arbre avec R1 (push-down) + R2 (choix jointure) + R3 (index scan)."""

        # Table unique — scan simple (avec index si possible)
        if len(tables) == 1:
            t = tables[0].upper()
            op, node = self._make_optimized_scan(t, pre_filters.get(t, []), offsets)
            return op, node

        # Plusieurs tables : arbre left-deep
        first = tables[0].upper()
        left_op, left_node = self._make_optimized_scan(
            first, pre_filters.get(first, []), offsets
        )
        accumulated = [first]

        for next_table in tables[1:]:
            next_upper = next_table.upper()

            # Scan optimise pour la table droite (R1 + R3)
            right_op, right_node = self._make_optimized_scan(
                next_upper, pre_filters.get(next_upper, []), offsets
            )

            # Trouver la condition de jointure
            jc = self._find_join_condition(join_conds, accumulated, next_upper)
            if jc is None:
                raise OptimizerError(
                    f"Pas de condition de jointure entre {accumulated} et {next_upper}. "
                    "Les produits cartesiens ne sont pas supportes."
                )

            left_col  = self._resolve_col(jc.left, offsets)
            right_col = self._resolve_local_col(jc.right.col)

            # ── R2 : Choix de l'algorithme de jointure ────────────────────
            left_size  = self._estimate_left_size(accumulated)
            right_size = self.catalog.get_table_size(next_upper)
            right_local_col = self._resolve_local_col(jc.right.col)

            join_op, join_label = self._choose_join(
                left_op, right_op, left_col, right_col,
                left_size, right_size,
                next_upper, right_local_col,
            )

            left_op = join_op
            left_node = PlanNode(join_label, [left_node, right_node])
            accumulated.append(next_upper)

        return left_op, left_node

    # ── R2 : Choix de la jointure ────────────────────────────────────────

    def _choose_join(
        self,
        left_op, right_op,
        left_col, right_col,
        left_size, right_size,
        right_table, right_local_col,
    ) -> tuple[Operateur, str]:
        """
        Choisit le meilleur algorithme de jointure.

        Priorite :
          1. IndexNestedLoopJoin  si index disponible sur la table droite
          2. HashJoin             si tables grandes
          3. SortMergeJoin        si tables grandes et de tailles comparables
          4. NestedLoopJoin       par defaut
        """

        # 1. Index disponible sur la colonne de jointure droite ?
        idx = self.catalog.get_index(right_table, right_local_col)
        if idx is not None:
            right_tbl = self.catalog.get_table(right_table)
            op = IndexNestedLoopJoin(
                left_op, right_tbl, idx, left_col
            )
            label = (
                f"IndexNestedLoopJoin(left_col={left_col}, "
                f"index={right_table}.col{right_local_col}) [OPTIMISE]"
            )
            return op, label

        # 2. Tables grandes -> HashJoin
        if left_size > HASH_THRESHOLD or right_size > HASH_THRESHOLD:
            # Si tailles comparables -> SortMergeJoin
            if (left_size > 0 and right_size > 0):
                ratio = max(left_size, right_size) / max(min(left_size, right_size), 1)
                if ratio <= SORT_MERGE_RATIO:
                    op = SortMergeJoin(left_op, right_op, left_col, right_col)
                    label = (
                        f"SortMergeJoin(left_col={left_col}, right_col={right_col}) "
                        f"[OPTIMISE: tailles comparables {left_size}/{right_size}]"
                    )
                    return op, label

            op = HashJoin(left_op, right_op, left_col, right_col)
            label = (
                f"HashJoin(left_col={left_col}, right_col={right_col}) "
                f"[OPTIMISE: grande table {max(left_size, right_size)} tuples]"
            )
            return op, label

        # 3. Petites tables -> NestedLoopJoin (simple et suffisant)
        op = Join(left_op, right_op, left_col, right_col)
        label = f"NestedLoopJoin(left_col={left_col}, right_col={right_col})"
        return op, label

    # ── R3 : Scan optimise (FullScan ou IndexScan) ───────────────────────

    def _make_optimized_scan(
        self,
        table_name: str,
        filters:    list[Condition],
        offsets:    dict[str, int],
    ) -> tuple[Operateur, PlanNode]:
        """
        Construit le meilleur operateur d'acces pour une table :
          - IndexScan si un filtre porte sur une colonne indexee
          - FullScan  sinon

        Puis applique les filtres restants comme Restrict.
        """
        tbl = self.catalog.get_table(table_name)
        table_offset = offsets[table_name]

        # Chercher un filtre exploitable par un index
        index_filter = None
        remaining_filters = []

        for cond in filters:
            if index_filter is not None:
                remaining_filters.append(cond)
                continue

            col_idx = self._resolve_col(cond.left, offsets)
            local_col = col_idx - table_offset
            idx = self.catalog.get_index(table_name, local_col)

            if idx is not None and cond.op in ("==", ">", ">=", "<", "<="):
                index_filter = (cond, idx, local_col)
            else:
                remaining_filters.append(cond)

        # Construire le scan
        if index_filter is not None:
            cond, idx, local_col = index_filter
            val = self._coerce_value(cond.right)
            op = IndexScan(tbl, idx, val, cond.op)
            node = PlanNode(
                f"IndexScan({table_name}, col={local_col}, "
                f"val={val!r}, op='{cond.op}') [OPTIMISE]"
            )
        else:
            op = self._make_scan(tbl)
            node = PlanNode(f"FullScan({table_name})")

        # Appliquer les filtres restants (R1 : push-down)
        for cond in remaining_filters:
            col_idx = self._resolve_col(cond.left, offsets)
            val = self._coerce_value(cond.right)
            # Recalculer l'index relatif au scan local pour cette table
            op = Restrict(op, col_idx - table_offset, val, cond.op)
            node = PlanNode(
                f"Restrict(col={col_idx - table_offset}, val={val!r}, "
                f"op='{cond.op}') [PUSH-DOWN]",
                [node],
            )

        return op, node

    # ── Estimation de taille ─────────────────────────────────────────────

    def _estimate_left_size(self, accumulated: list[str]) -> int:
        """Estime la taille du cote gauche (produit des tailles)."""
        size = 1
        for t in accumulated:
            s = self.catalog.get_table_size(t)
            size = size * s if s > 0 else size
        return size

    # ── Utilitaires (memes que dans le Planner) ──────────────────────────

    def _compute_offsets(self, tables: list[str]) -> dict[str, int]:
        offsets: dict[str, int] = {}
        offset = 0
        for t in tables:
            offsets[t.upper()] = offset
            offset += self.catalog.get_nb_att(t)
        return offsets

    def _find_join_condition(
        self,
        join_conds:  list[Condition],
        left_tables: list[str],
        right_table: str,
    ) -> Optional[Condition]:
        left_uppers = [t.upper() for t in left_tables]
        right_upper = right_table.upper()

        for cond in join_conds:
            lt = cond.left.table.upper() if cond.left.table else None
            rt = (cond.right.table.upper()
                  if isinstance(cond.right, ColumnRef) and cond.right.table
                  else None)
            if lt is None or rt is None:
                continue
            if lt in left_uppers and rt == right_upper:
                return cond
            if rt in left_uppers and lt == right_upper:
                return Condition(left=cond.right, op=cond.op, right=cond.left)
        return None

    @staticmethod
    def _make_scan(tbl) -> Operateur:
        if isinstance(tbl, TableDisque):
            return FullScanTableDisque(tbl)
        return FullScanTableMemoire(tbl)

    def _resolve_col(self, col_ref, offsets) -> int:
        if isinstance(col_ref, int):
            return col_ref
        local = self._resolve_local_col(col_ref.col)
        if col_ref.table:
            key = col_ref.table.upper()
            if key not in offsets:
                raise OptimizerError(f"Table inconnue '{col_ref.table}'")
            return offsets[key] + local
        return local

    @staticmethod
    def _resolve_local_col(col) -> int:
        if isinstance(col, int):
            return col
        s = str(col).upper()
        if s and s[0] == "A" and s[1:].isdigit():
            return int(s[1:]) - 1
        if s.isdigit():
            return int(s)
        raise OptimizerError(f"Impossible de resoudre la colonne '{col}'")

    @staticmethod
    def _coerce_value(val):
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str):
            try:
                return int(val)
            except ValueError:
                pass
            try:
                return float(val)
            except ValueError:
                pass
        return val
