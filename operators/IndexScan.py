"""
IndexScan – opérateur d'accès par index.

Se substitue à FullScanTableDisque quand un index est disponible.
Au lieu de lire tous les tuples, il consulte l'index pour obtenir
les indices des tuples correspondants et les lit directement.

Opérateurs supportés
--------------------
  ==          → index.search(value)           (tous les index)
  >, >=, <, <=, range → index.range_search()  (B+Tree uniquement)

Pipeline
--------
    IndexScan(table, index, val, op='==')
        └── [Project / Restrict / …]
"""

from __future__ import annotations

from typing import Optional, List

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple


class IndexScan(Instrumentation, Operateur):
    """
    Accès à une TableDisque via un index pré-construit.

    Paramètres
    ----------
    table :
        TableDisque (doit avoir été initialisée avec create() ou open()).
    index :
        StaticHashIndex | DynamicHashIndex | BPlusTreeIndex
    value :
        Valeur de recherche (pour == / > / < …).
    op : str
        Opérateur de comparaison : '==', '!=', '>', '>=', '<', '<='.
    high :
        Borne supérieure pour une recherche par intervalle avec un BPlusTreeIndex.
        Si fourni, la recherche devient range_search(value, high).
    """

    def __init__(
        self,
        table,
        index,
        value,
        op:   str  = "==",
        high        = None,
    ) -> None:
        super().__init__("IndexScan" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.table  = table
        self.index  = index
        self.value  = value
        self.op     = op
        self.high   = high

        self._indices: List[int] = []
        self._pos:     int       = 0

    # ── interface Operateur ────────────────────────────────────────────────

    def open(self) -> None:
        self.start()
        self.table.open()
        self._pos = 0
        self.tuplesProduits = 0
        self.memoire        = 0
        self._indices = self._query_index()
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()
        while self._pos < len(self._indices):
            idx = self._indices[self._pos]
            self._pos += 1
            t = self.table.get_tuple(idx)
            if t is not None:
                self.produit(t)
                self.stop()
                return t
        self.stop()
        return None

    def close(self) -> None:
        self.table.close()
        self._indices = []
        self._pos     = 0

    # ── résolution de l'index ──────────────────────────────────────────────

    def _query_index(self) -> List[int]:
        """Interroge l'index selon self.op et retourne les indices bruts."""

        # Recherche par intervalle (B+Tree uniquement)
        if self.high is not None:
            if not hasattr(self.index, "range_search"):
                raise TypeError(
                    "La recherche par intervalle nécessite un BPlusTreeIndex."
                )
            return self.index.range_search(self.value, self.high)

        # Recherche exacte sur index quelconque
        if self.op == "==":
            return self.index.search(self.value)

        # Recherche par comparaison (B+Tree avec range_search)
        if not hasattr(self.index, "range_search"):
            raise TypeError(
                f"L'opérateur '{self.op}' nécessite un BPlusTreeIndex "
                "(range_search non disponible sur cet index)."
            )

        import math
        _NEG_INF = -math.inf
        _POS_INF =  math.inf

        if self.op == ">":
            candidates = self.index.range_search(self.value, _POS_INF)
            return [i for i in candidates
                    if self.table.get_tuple(i) is not None
                    and self.table.get_tuple(i).val[self.index._col] > self.value]

        if self.op == ">=":
            return self.index.range_search(self.value, _POS_INF)

        if self.op == "<":
            candidates = self.index.range_search(_NEG_INF, self.value)
            return [i for i in candidates
                    if self.table.get_tuple(i) is not None
                    and self.table.get_tuple(i).val[self.index._col] < self.value]

        if self.op == "<=":
            return self.index.range_search(_NEG_INF, self.value)

        if self.op == "!=":
            # Pas adapté à un index hash mais on supporte quand même via full-index scan
            all_idx: List[int] = []
            for key in self._all_keys():
                if key != self.value:
                    all_idx.extend(self.index.search(key))
            return all_idx

        raise ValueError(f"Opérateur non supporté : '{self.op}'")

    def _all_keys(self):
        """Retourne toutes les clés distinctes connues de l'index (pour !=)."""
        # Tous les index disque exposent _storage (IndexDisque)
        if hasattr(self.index, "_storage"):
            storage = self.index._storage
            storage.open()
            seen = set()
            for i in range(storage.table_size):
                e = storage.get_entry(i)
                if e is not None and e[0] not in seen:
                    seen.add(e[0])
                    yield e[0]
            storage.close()
