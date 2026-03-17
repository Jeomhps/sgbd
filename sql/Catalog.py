"""
Catalog – registry that maps table names to table objects.

Accepte indifféremment des TableMemoire et des TableDisque.

Usage::

    cat = Catalog()
    cat.register("T1", TableMemoire.randomize(3, 100, 50))
    cat.register("T2", TableDisque("t2.dat"))   # après table.create(...)
"""

from __future__ import annotations

from typing import Optional


class Catalog:

    def __init__(self) -> None:
        # Internal storage: all names are stored UPPER-CASED
        self._tables: dict[str, object] = {}

    # ── registration ───────────────────────────────────────────────────────

    def register(self, name: str, table) -> None:
        """Add or replace a table in the catalog (TableMemoire ou TableDisque)."""
        self._tables[name.upper()] = table

    # ── lookup ─────────────────────────────────────────────────────────────

    def get_table(self, name: str) -> Optional[object]:
        return self._tables.get(name.upper())

    def get_nb_att(self, name: str) -> int:
        """Return the number of attributes of the named table."""
        tbl = self.get_table(name)
        if tbl is None:
            raise KeyError(f"Table '{name}' not found in catalog")
        # TableMemoire expose nb_att ; TableDisque expose tuple_size
        if hasattr(tbl, "nb_att"):
            return tbl.nb_att
        if hasattr(tbl, "tuple_size") and tbl.tuple_size > 0:
            return tbl.tuple_size
        raise ValueError(
            f"Cannot determine number of attributes for table '{name}'. "
            "Pour une TableDisque, appelez create() ou open() avant l'enregistrement."
        )

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def __repr__(self) -> str:
        parts = []
        for n, t in self._tables.items():
            try:
                nb = self.get_nb_att(n)
                parts.append(f"{n}({nb} cols)")
            except Exception:
                parts.append(f"{n}(?)")
        return f"Catalog({', '.join(parts)})"
