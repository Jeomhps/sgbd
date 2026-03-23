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
        self._tables: dict[str, object] = {}
        # Index : (TABLE, col_locale) -> objet index
        self._indexes: dict[tuple[str, int], object] = {}

    # ── tables ────────────────────────────────────────────────────────────

    def register(self, name: str, table) -> None:
        """Enregistre une table (TableMemoire ou TableDisque)."""
        self._tables[name.upper()] = table

    def get_table(self, name: str) -> Optional[object]:
        return self._tables.get(name.upper())

    def get_nb_att(self, name: str) -> int:
        """Nombre d'attributs de la table."""
        tbl = self.get_table(name)
        if tbl is None:
            raise KeyError(f"Table '{name}' introuvable dans le catalogue")
        if hasattr(tbl, "nb_att"):
            return tbl.nb_att
        if hasattr(tbl, "tuple_size") and tbl.tuple_size > 0:
            return tbl.tuple_size
        raise ValueError(
            f"Impossible de determiner le nombre d'attributs de '{name}'. "
            "Pour TableDisque, appeler create() ou open() avant register()."
        )

    def get_table_size(self, name: str) -> int:
        """Nombre de tuples (estimation). Retourne 0 si inconnu."""
        tbl = self.get_table(name)
        if tbl is None:
            return 0
        if hasattr(tbl, "valeurs"):
            return len(tbl.valeurs)
        if hasattr(tbl, "table_size"):
            return tbl.table_size
        return 0

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    # ── index ─────────────────────────────────────────────────────────────

    def register_index(self, table_name: str, col: int, index) -> None:
        """Enregistre un index sur (table, colonne locale)."""
        self._indexes[(table_name.upper(), col)] = index

    def get_index(self, table_name: str, col: int) -> Optional[object]:
        """Retourne l'index sur (table, colonne locale), ou None."""
        return self._indexes.get((table_name.upper(), col))

    def list_indexes(self) -> list[tuple[str, int]]:
        return list(self._indexes.keys())

    def __repr__(self) -> str:
        parts = []
        for n, t in self._tables.items():
            try:
                nb = self.get_nb_att(n)
                parts.append(f"{n}({nb} cols)")
            except Exception:
                parts.append(f"{n}(?)")
        idx_parts = [f"idx({t}.col{c})" for (t, c) in self._indexes]
        return f"Catalog({', '.join(parts + idx_parts)})"
