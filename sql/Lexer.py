"""
Simple SQL Lexer – tokenizes SQL queries into tokens.

Supports basic SQL syntax for SELECT statements with WHERE clauses and aggregations.
"""

from enum import Enum, auto
from dataclasses import dataclass
from typing import Any


class TokenType(Enum):
    """Token types for SQL parsing."""
    # SQL keywords
    SELECT  = auto()
    FROM    = auto()
    WHERE   = auto()
    AND     = auto()
    GROUP   = auto()
    BY      = auto()

    # Aggregate functions
    AVG   = auto()
    SUM   = auto()
    MIN   = auto()
    MAX   = auto()
    COUNT = auto()

    # Comparison operators
    EQ  = auto()   # =
    NEQ = auto()   # != or <>
    GT  = auto()   # >
    LT  = auto()   # <
    GTE = auto()   # >=
    LTE = auto()   # <=

    # Punctuation
    COMMA   = auto()
    DOT     = auto()
    LPAREN  = auto()
    RPAREN  = auto()
    STAR    = auto()

    # Literals
    IDENT  = auto()   # Table/column names
    NUMBER = auto()   # Numeric literals
    STRING = auto()   # String literals

    EOF = auto()      # End of input


@dataclass
class Token:
    """A single token from the SQL input."""
    type:  TokenType   # Token type
    value: Any        # Token value (e.g., "SELECT", 42, "employees")
    pos:   int        # Position in original SQL string


# Reserved words mapped to their token type.
# All comparisons done against the UPPER-cased source text.
_KEYWORDS: dict[str, TokenType] = {
    "SELECT": TokenType.SELECT,
    "FROM":   TokenType.FROM,
    "WHERE":  TokenType.WHERE,
    "AND":    TokenType.AND,
    "GROUP":  TokenType.GROUP,
    "BY":     TokenType.BY,
    "AVG":    TokenType.AVG,
    "SUM":    TokenType.SUM,
    "MIN":    TokenType.MIN,
    "MAX":    TokenType.MAX,
    "COUNT":  TokenType.COUNT,
}

# Token types that represent aggregate functions
AGG_TYPES: frozenset[TokenType] = frozenset({
    TokenType.AVG, TokenType.SUM, TokenType.MIN, TokenType.MAX, TokenType.COUNT,
})


class LexerError(Exception):
    pass


class Lexer:
    """Tokenise a SQL string into a flat list of Tokens."""

    def __init__(self, text: str) -> None:
        self.tokens: list[Token] = []
        self._run(text)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, text: str) -> None:
        pos = 0
        n   = len(text)

        while pos < n:
            c = text[pos]

            # ── whitespace ──────────────────────────────────────────────
            if c.isspace():
                pos += 1
                continue

            start = pos

            # ── string literal  (" … " or ' … ') ───────────────────────
            if c in ('"', "'"):
                quote = c
                pos  += 1
                buf   = []
                while pos < n and text[pos] != quote:
                    buf.append(text[pos])
                    pos += 1
                if pos >= n:
                    raise LexerError(f"Unterminated string starting at {start}")
                pos += 1  # closing quote
                self.tokens.append(Token(TokenType.STRING, "".join(buf), start))
                continue

            # ── numeric literal ─────────────────────────────────────────
            if c.isdigit():
                buf = []
                while pos < n and (text[pos].isdigit() or text[pos] == "."):
                    buf.append(text[pos])
                    pos += 1
                raw = "".join(buf)
                val: int | float = float(raw) if "." in raw else int(raw)
                self.tokens.append(Token(TokenType.NUMBER, val, start))
                continue

            # ── identifier / keyword ────────────────────────────────────
            if c.isalpha() or c == "_":
                buf = []
                while pos < n and (text[pos].isalnum() or text[pos] == "_"):
                    buf.append(text[pos])
                    pos += 1
                word   = "".join(buf)
                upper  = word.upper()
                ttype  = _KEYWORDS.get(upper, TokenType.IDENT)
                # Store the upper-cased form so comparisons are case-insensitive
                self.tokens.append(Token(ttype, upper, start))
                continue

            # ── two-character operators ──────────────────────────────────
            if c == "!" and pos + 1 < n and text[pos + 1] == "=":
                self.tokens.append(Token(TokenType.NEQ, "!=", start))
                pos += 2
                continue

            if c == "<":
                if pos + 1 < n and text[pos + 1] == "=":
                    self.tokens.append(Token(TokenType.LTE, "<=", start))
                    pos += 2
                elif pos + 1 < n and text[pos + 1] == ">":
                    self.tokens.append(Token(TokenType.NEQ, "!=", start))
                    pos += 2
                else:
                    self.tokens.append(Token(TokenType.LT, "<", start))
                    pos += 1
                continue

            if c == ">":
                if pos + 1 < n and text[pos + 1] == "=":
                    self.tokens.append(Token(TokenType.GTE, ">=", start))
                    pos += 2
                else:
                    self.tokens.append(Token(TokenType.GT, ">", start))
                    pos += 1
                continue

            # ── single-character operators / punctuation ─────────────────
            _SINGLE: dict[str, TokenType] = {
                "=": TokenType.EQ,
                ",": TokenType.COMMA,
                ".": TokenType.DOT,
                "(": TokenType.LPAREN,
                ")": TokenType.RPAREN,
                "*": TokenType.STAR,
            }
            if c in _SINGLE:
                self.tokens.append(Token(_SINGLE[c], c, start))
                pos += 1
                continue

            raise LexerError(f"Unexpected character {c!r} at position {pos}")

        self.tokens.append(Token(TokenType.EOF, None, n))
