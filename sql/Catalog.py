"""
Catalog – registry that maps table names to TableMemoire objects.

Usage::

    cat = Catalog()
    cat.register("T1", my_table)
    cat.register("T2", other_table)
"""

from __future__ import annotations

from typing import Optional

from core.TableMemoire import TableMemoire


class Catalog:

    def __init__(self) -> None:
        # Internal storage: all names are stored UPPER-CASED
        self._tables: dict[str, TableMemoire] = {}

    # ── registration ───────────────────────────────────────────────────────

    def register(self, name: str, table: TableMemoire) -> None:
        """Add or replace a table in the catalog."""
        self._tables[name.upper()] = table

    # ── lookup ─────────────────────────────────────────────────────────────

    def get_table(self, name: str) -> Optional[TableMemoire]:
        return self._tables.get(name.upper())

    def get_nb_att(self, name: str) -> int:
        """Return the number of attributes of the named table."""
        tbl = self.get_table(name)
        if tbl is None:
            raise KeyError(f"Table '{name}' not found in catalog")
        return tbl.nb_att

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def __repr__(self) -> str:
        entries = ", ".join(
            f"{n}({self._tables[n].nb_att} cols)" for n in self._tables
        )
        return f"Catalog({entries})"
