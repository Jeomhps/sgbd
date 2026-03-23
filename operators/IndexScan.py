"""
IndexScan — acces a une table via un index (par bloc).

Au lieu de lire tous les blocs (full scan), interroge l'index pour obtenir
les numeros de blocs pertinents, lit chaque bloc et filtre les tuples.

Operateurs supportes :
  ==           -> index.search(value)        (tous les index)
  >, >=, <, <= -> index.range_search()       (B+Tree uniquement)
"""

from __future__ import annotations
import math
from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple


class IndexScan(Instrumentation, Operateur):
    """Acces a une table via un index pre-construit."""

    def __init__(self, table, index, value, op="==", high=None):
        super().__init__("IndexScan" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.table = table
        self.index = index
        self.value = value
        self.op = op
        self.high = high
        self._col = -1
        self._block_numbers: List[int] = []
        self._block_pos = 0
        self._current_block: List[Tuple] = []
        self._slot = 0

    def open(self):
        self.start()
        if hasattr(self.table, "open"):
            self.table.open()
        self._col = self.index._col
        self._block_numbers = self._query_index()
        self._block_pos = 0
        self._current_block = []
        self._slot = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()
        while True:
            # Emettre le prochain tuple correspondant dans le bloc courant
            while self._slot < len(self._current_block):
                t = self._current_block[self._slot]
                self._slot += 1
                if t is not None and self._matches(t):
                    self.produit(t)
                    self.stop()
                    return t

            # Bloc epuise -> passer au suivant
            if self._block_pos >= len(self._block_numbers):
                self.stop()
                return None
            self._current_block = self.table.get_block(self._block_numbers[self._block_pos])
            self._block_pos += 1
            self._slot = 0

    def close(self):
        if hasattr(self.table, "close"):
            self.table.close()
        self._block_numbers = []
        self._current_block = []

    def _query_index(self) -> List[int]:
        """Interroge l'index et retourne les n° de blocs a lire."""
        if self.high is not None:
            return self.index.range_search(self.value, self.high)
        if self.op == "==":
            return self.index.search(self.value)

        if not hasattr(self.index, "range_search"):
            raise TypeError(f"L'operateur '{self.op}' necessite un BPlusTreeIndex.")

        if self.op in (">=", ">"):
            return self.index.range_search(self.value, math.inf)
        if self.op in ("<=", "<"):
            return self.index.range_search(-math.inf, self.value)
        raise ValueError(f"Operateur non supporte : '{self.op}'")

    def _matches(self, t: Tuple) -> bool:
        """Verifie si le tuple satisfait la condition de recherche."""
        if self._col < 0 or self._col >= len(t.val):
            return False
        v = t.val[self._col]
        if self.high is not None:
            return self.value <= v <= self.high
        if self.op == "==": return v == self.value
        if self.op == "!=": return v != self.value
        if self.op == ">":  return v > self.value
        if self.op == ">=": return v >= self.value
        if self.op == "<":  return v < self.value
        if self.op == "<=": return v <= self.value
        return False
