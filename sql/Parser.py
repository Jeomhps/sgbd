"""
SQL Parser – builds an AST for a subset of SELECT statements:

    SELECT col, …  |  SELECT AGG(col), …  |  SELECT *
    FROM   table [, table …]
    [WHERE cond [AND cond …]]
    [GROUP BY col [, col …]]

Column references
-----------------
  TABLE.AN   – attribute N (1-indexed) of TABLE     e.g. T1.A2
  N          – 0-indexed global column index        e.g.  2
  AN         – attribute N (1-indexed), no table    e.g.  A2
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Union

from sql.Lexer import Lexer, Token, TokenType, AGG_TYPES


# ──────────────────────────────────────────────────────────────────────────────
# AST node types
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnRef:
    """A reference to a single column, optionally qualified by a table name."""
    table: Optional[str]          # None when no qualifier
    col:   Union[str, int]        # 'A1', 'A2', … or bare integer index

    def __repr__(self) -> str:
        return f"{self.table}.{self.col}" if self.table else str(self.col)


@dataclass
class AggExpr:
    """An aggregate function call:  AVG(T1.A2)"""
    func: str          # 'AVG', 'SUM', 'MIN', 'MAX', 'COUNT'
    col:  ColumnRef    # the column being aggregated (* is represented as col=0)

    def __repr__(self) -> str:
        col_str = "*" if (self.func == "COUNT" and self.col.col == 0) else repr(self.col)
        return f"{self.func}({col_str})"


@dataclass
class Condition:
    """A single WHERE predicate:  left op right"""
    left:  ColumnRef
    op:    str                             # '==', '!=', '>', '<', '>=', '<='
    right: Union[ColumnRef, str, int, float]

    def is_join(self) -> bool:
        """True when both sides reference columns (equi-join condition)."""
        return isinstance(self.right, ColumnRef)

    def __repr__(self) -> str:
        return f"{self.left!r} {self.op} {self.right!r}"


@dataclass
class SelectQuery:
    """Root AST node for a complete SELECT statement."""
    columns:    List[Union[ColumnRef, AggExpr, str]]   # str == '*'
    tables:     List[str]
    conditions: List[Condition]
    group_by:   List[ColumnRef] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# Operator-token mapping
# ──────────────────────────────────────────────────────────────────────────────

_OP_MAP: dict[TokenType, str] = {
    TokenType.EQ:  "==",
    TokenType.NEQ: "!=",
    TokenType.GT:  ">",
    TokenType.LT:  "<",
    TokenType.GTE: ">=",
    TokenType.LTE: "<=",
}


class ParseError(SyntaxError):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────────

class SQLParser:
    """
    Recursive-descent parser.  Entry point: ``parse()`` returns a SelectQuery.
    """

    def __init__(self, sql: str) -> None:
        self._tokens: list[Token] = Lexer(sql).tokens
        self._pos:    int         = 0

    # ── public API ─────────────────────────────────────────────────────────

    def parse(self) -> SelectQuery:
        self._expect(TokenType.SELECT)
        columns = self._parse_select_list()

        self._expect(TokenType.FROM)
        tables = self._parse_table_list()

        conditions: list[Condition] = []
        group_by:   list[ColumnRef] = []

        if self._match(TokenType.WHERE):
            self._consume()
            conditions = self._parse_condition_list()

        if self._match(TokenType.GROUP):
            self._consume()
            self._expect(TokenType.BY)
            group_by = self._parse_col_ref_list()

        return SelectQuery(
            columns    = columns,
            tables     = tables,
            conditions = conditions,
            group_by   = group_by,
        )

    # ── SELECT list ────────────────────────────────────────────────────────

    def _parse_select_list(self) -> list:
        items = [self._parse_select_item()]
        while self._match(TokenType.COMMA):
            self._consume()
            items.append(self._parse_select_item())
        return items

    def _parse_select_item(self):
        tok = self._peek()

        # SELECT *
        if tok.type == TokenType.STAR:
            self._consume()
            return "*"

        # SELECT AGG(col)
        if tok.type in AGG_TYPES:
            func = tok.value
            self._consume()
            self._expect(TokenType.LPAREN)
            # COUNT(*) special case
            if self._match(TokenType.STAR):
                self._consume()
                col = ColumnRef(table=None, col=0)
            else:
                col = self._parse_col_ref()
            self._expect(TokenType.RPAREN)
            return AggExpr(func=func, col=col)

        # SELECT col_ref
        return self._parse_col_ref()

    # ── FROM list ──────────────────────────────────────────────────────────

    def _parse_table_list(self) -> list[str]:
        tables = [self._expect(TokenType.IDENT).value]
        while self._match(TokenType.COMMA):
            self._consume()
            tables.append(self._expect(TokenType.IDENT).value)
        return tables

    # ── WHERE conditions ───────────────────────────────────────────────────

    def _parse_condition_list(self) -> list[Condition]:
        conds = [self._parse_condition()]
        while self._match(TokenType.AND):
            self._consume()
            conds.append(self._parse_condition())
        return conds

    def _parse_condition(self) -> Condition:
        left = self._parse_col_ref()

        tok = self._peek()
        if tok.type not in _OP_MAP:
            raise ParseError(
                f"Expected comparison operator at pos {tok.pos}, got {tok.type} ({tok.value!r})"
            )
        self._consume()
        op = _OP_MAP[tok.type]

        right = self._parse_value_or_col_ref()
        return Condition(left=left, op=op, right=right)

    def _parse_value_or_col_ref(self):
        """Parse the right-hand side of a condition: literal or column ref."""
        tok = self._peek()
        if tok.type == TokenType.STRING:
            self._consume()
            return tok.value
        if tok.type == TokenType.NUMBER:
            self._consume()
            return tok.value
        # Must be a column reference
        return self._parse_col_ref()

    # ── GROUP BY list ──────────────────────────────────────────────────────

    def _parse_col_ref_list(self) -> list[ColumnRef]:
        cols = [self._parse_col_ref()]
        while self._match(TokenType.COMMA):
            self._consume()
            cols.append(self._parse_col_ref())
        return cols

    # ── Column reference  TABLE.AN  |  AN  |  N ───────────────────────────

    def _parse_col_ref(self) -> ColumnRef:
        tok = self._peek()

        # Bare integer: 0-indexed global column
        if tok.type == TokenType.NUMBER:
            self._consume()
            return ColumnRef(table=None, col=int(tok.value))

        # Identifier: table name, column name, or A-style attribute
        if tok.type == TokenType.IDENT:
            name = tok.value
            self._consume()
            if self._match(TokenType.DOT):
                # TABLE.COL form
                self._consume()
                col_tok = self._peek()
                if col_tok.type not in (TokenType.IDENT, TokenType.NUMBER):
                    raise ParseError(
                        f"Expected column name after '.', got {col_tok.type} at pos {col_tok.pos}"
                    )
                self._consume()
                col_val: Union[str, int] = (
                    int(col_tok.value) if col_tok.type == TokenType.NUMBER else col_tok.value
                )
                return ColumnRef(table=name, col=col_val)
            else:
                # Bare identifier used as column name (e.g. A1, A2)
                return ColumnRef(table=None, col=name)

        raise ParseError(
            f"Expected column reference at pos {tok.pos}, got {tok.type} ({tok.value!r})"
        )

    # ── Token stream helpers ───────────────────────────────────────────────

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _consume(self) -> Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _match(self, *types: TokenType) -> bool:
        return self._peek().type in types

    def _expect(self, ttype: TokenType) -> Token:
        tok = self._peek()
        if tok.type != ttype:
            raise ParseError(
                f"Expected {ttype.name} but got {tok.type.name} ({tok.value!r}) at pos {tok.pos}"
            )
        return self._consume()
