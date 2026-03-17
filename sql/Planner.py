"""
QueryPlanner – translates a SelectQuery AST into an operator tree.

Column resolution
-----------------
After joining N tables the combined tuple layout is:

    [ T1.col0 … T1.colK | T2.col0 … T2.colM | … ]
    ^                   ^
    offset[T1]=0        offset[T2]=nb_att(T1)

Column-name conventions understood by the planner:
  • ``T.AN``  – attribute N (1-indexed) of table T  →  offset[T] + (N-1)
  • ``AN``    – attribute N, no qualifier            →  N-1   (global)
  • bare int  – already a 0-based global index      →  as-is

Plan-building steps
-------------------
1. Compute per-table offsets.
2. Separate WHERE clauses into join conditions vs. filter conditions.
3. Build FullScan operators and chain them with Join operators.
4. Wrap with Restrict for each filter condition.
5. Wrap with Aggregate when aggregate expressions are present.
6. Wrap with Project for column selection (omitted for SELECT *).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Union

from sql.Catalog import Catalog
from sql.Parser import AggExpr, ColumnRef, Condition, SelectQuery

from core.FullScanTableMemoire import FullScanTableMemoire
from core.Operateur import Operateur
from operators.Aggregate import Aggregate
from operators.Join import Join
from operators.Project import Project
from operators.Restrict import Restrict


# ──────────────────────────────────────────────────────────────────────────────
# Plan description tree  (for human-readable output)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PlanNode:
    """A node in the human-readable plan description tree."""
    label:    str
    children: List["PlanNode"] = field(default_factory=list)

    def __str__(self, indent: int = 0) -> str:
        prefix = "  " * indent
        connector = "└─ " if indent > 0 else ""
        lines = [prefix + connector + self.label]
        for child in self.children:
            lines.append(child.__str__(indent + 1))
        return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class PlannerError(Exception):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# QueryPlanner
# ──────────────────────────────────────────────────────────────────────────────

class QueryPlanner:
    """
    Build an executable operator tree from a parsed SelectQuery.

    Returns a tuple ``(operator, plan_node)`` from :py:meth:`plan`.
    """

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    # ── public API ─────────────────────────────────────────────────────────

    def plan(self, query: SelectQuery) -> tuple[Operateur, PlanNode]:
        """
        Build and return ``(root_operator, plan_description)``.

        The caller can print ``plan_description`` before executing.
        """
        # 1. Validate all referenced tables
        for t in query.tables:
            if self.catalog.get_table(t) is None:
                raise PlannerError(f"Unknown table: '{t}'")

        # 2. Compute column offsets in the joined tuple
        offsets = self._compute_offsets(query.tables)

        # 3. Classify WHERE conditions
        join_conds   = [c for c in query.conditions if c.is_join()]
        filter_conds = [c for c in query.conditions if not c.is_join()]

        # 4. Build scan/join tree
        op, plan = self._build_join_tree(query.tables, join_conds, offsets)

        # 5. Apply filter conditions (Restrict)
        for cond in filter_conds:
            col_idx = self._resolve_col(cond.left, offsets)
            val     = self._coerce_value(cond.right)
            op      = Restrict(op, col_idx, val, cond.op)
            plan    = PlanNode(
                f"Restrict(col={col_idx}, val={val!r}, op='{cond.op}')",
                [plan],
            )

        # 6a. Aggregate
        agg_exprs: list[AggExpr] = [
            c for c in query.columns if isinstance(c, AggExpr)
        ]
        if agg_exprs:
            agg  = agg_exprs[0]
            agg_col = self._resolve_col(agg.col, offsets)
            group_cols: Optional[list[int]] = (
                [self._resolve_col(c, offsets) for c in query.group_by]
                if query.group_by else None
            )
            op   = Aggregate(op, agg_col, agg.func, group_cols)
            plan = PlanNode(
                f"Aggregate(func={agg.func}, col={agg_col}, group_by={group_cols})",
                [plan],
            )

        # 6b. Projection (skip when SELECT *)
        elif "*" not in query.columns:
            proj_indices = [
                self._resolve_col(c, offsets)
                for c in query.columns
                if not isinstance(c, AggExpr)
            ]
            op   = Project(op, proj_indices)
            plan = PlanNode(f"Project(cols={proj_indices})", [plan])

        return op, plan

    # ── offset computation ─────────────────────────────────────────────────

    def _compute_offsets(self, tables: list[str]) -> dict[str, int]:
        """Return {TABLE_NAME: first_column_index_in_combined_tuple}."""
        offsets: dict[str, int] = {}
        offset = 0
        for t in tables:
            offsets[t.upper()] = offset
            offset += self.catalog.get_nb_att(t)
        return offsets

    # ── join-tree construction ─────────────────────────────────────────────

    def _build_join_tree(
        self,
        tables:     list[str],
        join_conds: list[Condition],
        offsets:    dict[str, int],
    ) -> tuple[Operateur, PlanNode]:
        """Chain FullScan operators with Join operators."""

        # Single table – just a full scan
        if len(tables) == 1:
            tbl  = self.catalog.get_table(tables[0])
            op   = FullScanTableMemoire(tbl)
            node = PlanNode(f"FullScan({tables[0]})")
            return op, node

        # Multiple tables: left-deep join tree
        left_op, left_node = (
            FullScanTableMemoire(self.catalog.get_table(tables[0])),
            PlanNode(f"FullScan({tables[0]})"),
        )
        accumulated = [tables[0].upper()]

        for next_table in tables[1:]:
            next_upper = next_table.upper()
            right_op   = FullScanTableMemoire(self.catalog.get_table(next_table))
            right_node = PlanNode(f"FullScan({next_table})")

            jc = self._find_join_condition(join_conds, accumulated, next_upper)

            if jc is None:
                raise PlannerError(
                    f"No join condition found between {accumulated} and {next_upper}. "
                    "Cross joins are not supported. "
                    "Add a WHERE condition like T1.A1 = T2.A1."
                )

            # left_col  → index inside the accumulated (left) operator output
            # right_col → local index inside the new table's scan
            left_col  = self._resolve_col(jc.left, offsets)
            right_col = self._resolve_local_col(jc.right.col)   # type: ignore[union-attr]

            left_op   = Join(left_op, right_op, left_col, right_col)
            left_node = PlanNode(
                f"Join(left_col={left_col}, right_col={right_col})",
                [left_node, right_node],
            )
            accumulated.append(next_upper)

        return left_op, left_node

    def _find_join_condition(
        self,
        join_conds:     list[Condition],
        left_tables:    list[str],
        right_table:    str,
    ) -> Optional[Condition]:
        """
        Return a join condition that links the accumulated left tables
        with *right_table*, normalised so that ``cond.left`` refers to
        the accumulated side and ``cond.right`` refers to *right_table*.
        """
        left_uppers  = [t.upper() for t in left_tables]
        right_upper  = right_table.upper()

        for cond in join_conds:
            lt = cond.left.table.upper()  if cond.left.table  else None
            rt = cond.right.table.upper() if isinstance(cond.right, ColumnRef) and cond.right.table else None  # type: ignore[union-attr]

            if lt is None or rt is None:
                continue

            if lt in left_uppers and rt == right_upper:
                # Already in the correct orientation
                return cond

            if rt in left_uppers and lt == right_upper:
                # Swap so that .left → accumulated, .right → new table
                return Condition(left=cond.right, op=cond.op, right=cond.left)  # type: ignore[arg-type]

        return None

    # ── column resolution ──────────────────────────────────────────────────

    def _resolve_col(
        self,
        col_ref: Union[ColumnRef, int],
        offsets: dict[str, int],
    ) -> int:
        """
        Resolve a ColumnRef to its 0-based global index in the combined tuple.

        Examples with offsets = {'T1': 0, 'T2': 3}:
          ColumnRef('T1', 'A2') → 0 + 1 = 1
          ColumnRef('T2', 'A1') → 3 + 0 = 3
          ColumnRef(None,  2  ) → 2
        """
        if isinstance(col_ref, int):
            return col_ref

        local = self._resolve_local_col(col_ref.col)

        if col_ref.table:
            key = col_ref.table.upper()
            if key not in offsets:
                raise PlannerError(f"Unknown table '{col_ref.table}' in column reference")
            return offsets[key] + local

        return local

    @staticmethod
    def _resolve_local_col(col: Union[str, int]) -> int:
        """
        Resolve a column name/index to a 0-based local index.

          'A1' → 0,  'A2' → 1,  …
          3    → 3
        """
        if isinstance(col, int):
            return col
        s = str(col).upper()
        if s and s[0] == "A" and s[1:].isdigit():
            return int(s[1:]) - 1          # A1 → 0, A2 → 1, …
        if s.isdigit():
            return int(s)
        raise PlannerError(f"Cannot resolve column name '{col}'")

    # ── value coercion ─────────────────────────────────────────────────────

    @staticmethod
    def _coerce_value(val) -> Union[int, float, str]:
        """
        Coerce a parsed literal to the most appropriate Python type.

        String "5" → int 5;  "3.14" → float 3.14;  "abc" → "abc".
        """
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
