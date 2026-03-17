"""
Simple SQL Parser – builds an AST for basic SELECT statements.

Supports:
    SELECT col, … | SELECT AGG(col), … | SELECT *
    FROM table [, table …]
    [WHERE condition [AND condition …]]

Column references:
  TABLE.COL – qualified column reference
  COL       – simple column name
  N         – 0-indexed column index
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Union

from sql.Lexer import Lexer, Token, TokenType, AGG_TYPES


# ──────────────────────────────────────────────────────────────────────────────
# AST node types (simplified)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnRef:
    """A reference to a single column, optionally qualified by a table name."""
    table: str | None          # None when no qualifier
    col:   Union[str, int]     # Column name or index

    def __repr__(self) -> str:
        return f"{self.table}.{self.col}" if self.table else str(self.col)


@dataclass
class AggExpr:
    """An aggregate function call: AVG(col), SUM(col), etc."""
    func: str          # 'AVG', 'SUM', 'MIN', 'MAX', 'COUNT'
    col:  ColumnRef    # The column being aggregated

    def __repr__(self) -> str:
        col_str = "*" if (self.func == "COUNT" and self.col.col == 0) else repr(self.col)
        return f"{self.func}({col_str})"


@dataclass
class Condition:
    """A single WHERE predicate: left op right"""
    left:  ColumnRef
    op:    str                             # '==', '!=', '>', '<', '>=', '<='
    right: Union[ColumnRef, str, int, float]  # Literal value or column reference

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
    """Raised when SQL parsing fails."""
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Simple SQL Parser
# ──────────────────────────────────────────────────────────────────────────────

class SQLParser:
    """
    Simple SQL parser using recursive descent.
    
    Entry point: parse() returns a SelectQuery AST.
    Supports basic SELECT statements without GROUP BY.
    """

    def __init__(self, sql: str) -> None:
        self._tokens: list[Token] = Lexer(sql).tokens
        self._pos:    int         = 0

    # ── Public API ────────────────────────────────────────────────────────────

    def parse(self) -> SelectQuery:
        """Parse SQL query and return AST."""
        self._expect(TokenType.SELECT)
        columns = self._parse_select_list()

        self._expect(TokenType.FROM)
        tables = self._parse_table_list()

        conditions: list[Condition] = []

        # Parse WHERE clause if present
        if self._match(TokenType.WHERE):
            self._consume()
            conditions = self._parse_condition_list()

        return SelectQuery(
            columns    = columns,
            tables     = tables,
            conditions = conditions,
        )

    # ── SELECT list parsing ──────────────────────────────────────────────────

    def _parse_select_list(self) -> list:
        """Parse comma-separated list of SELECT items."""
        items = [self._parse_select_item()]
        while self._match(TokenType.COMMA):
            self._consume()
            items.append(self._parse_select_item())
        return items

    def _parse_select_item(self):
        """Parse a single SELECT item: * | AGG(col) | col_ref"""
        tok = self._peek()

        # SELECT *
        if tok.type == TokenType.STAR:
            self._consume()
            return "*"

        # SELECT AGG(col)
        if tok.type in AGG_TYPES:
            return self._parse_agg_expr()

        # SELECT col_ref
        return self._parse_col_ref()

    def _parse_agg_expr(self) -> AggExpr:
        """Parse aggregate function: AVG(col), SUM(col), etc."""
        func = self._peek().value
        self._consume()  # Consume function name
        self._expect(TokenType.LPAREN)
        
        # COUNT(*) special case
        if self._match(TokenType.STAR):
            self._consume()
            col = ColumnRef(table=None, col=0)
        else:
            col = self._parse_col_ref()
        
        self._expect(TokenType.RPAREN)
        return AggExpr(func=func, col=col)

    # ── FROM list parsing ───────────────────────────────────────────────────

    def _parse_table_list(self) -> list[str]:
        """Parse comma-separated list of table names."""
        tables = [self._expect(TokenType.IDENT).value]
        while self._match(TokenType.COMMA):
            self._consume()
            tables.append(self._expect(TokenType.IDENT).value)
        return tables

    # ── WHERE conditions parsing ──────────────────────────────────────────

    def _parse_condition_list(self) -> list[Condition]:
        """Parse AND-separated list of conditions."""
        conds = [self._parse_condition()]
        while self._match(TokenType.AND):
            self._consume()
            conds.append(self._parse_condition())
        return conds

    def _parse_condition(self) -> Condition:
        """Parse a single condition: col op value"""
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
        
        # String literal
        if tok.type == TokenType.STRING:
            self._consume()
            return tok.value
        
        # Numeric literal
        if tok.type == TokenType.NUMBER:
            self._consume()
            return tok.value
        
        # Must be a column reference
        return self._parse_col_ref()

    # ── Column reference parsing ────────────────────────────────────────────

    def _parse_col_ref(self) -> ColumnRef:
        """Parse column reference: TABLE.COL | COL | N"""
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
        """Look at current token without consuming it."""
        return self._tokens[self._pos]

    def _consume(self) -> Token:
        """Consume and return current token."""
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _match(self, *types: TokenType) -> bool:
        """Check if current token matches any of the given types."""
        return self._peek().type in types

    def _expect(self, ttype: TokenType) -> Token:
        """Consume current token if it matches expected type, else raise error."""
        tok = self._peek()
        if tok.type != ttype:
            raise ParseError(
                f"Expected {ttype.name} but got {tok.type.name} ({tok.value!r}) at pos {tok.pos}"
            )
        return self._consume()