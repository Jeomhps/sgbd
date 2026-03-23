"""
Jointure boucle imbriquee sur index (Index Nested Loop Join).

Pour chaque tuple gauche :
  1. Extraire la cle  left_tuple.val[left_col]
  2. Interroger l'index : index.search(key) -> liste de n° de blocs
  3. Lire chaque bloc et filtrer les tuples correspondants
  4. Produire le tuple concatene (gauche + droit)

Complexite : O(n x k) au lieu de O(n x m), avec k << m.
"""

from __future__ import annotations
import math
from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.TableDisque import TableDisque
from core.Tuple import Tuple


class IndexNestedLoopJoin(Instrumentation, Operateur):
    """Jointure par index sur la table droite."""

    def __init__(self, left, right_table, right_index, left_col, op="==", high=None):
        super().__init__("IndexNLJ" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = left
        self.right_table = right_table
        self.right_index = right_index
        self.left_col = left_col
        self.op = op
        self.high = high

        self._is_disk = isinstance(right_table, TableDisque)
        self._left_tuple: Optional[Tuple] = None
        self._current_key = None
        self._right_blocks: List[int] = []
        self._right_block_pos = 0
        self._block_tuples: List[Tuple] = []
        self._block_tuple_pos = 0

    def open(self):
        self.start()
        self.left.open()
        if self._is_disk:
            self.right_table.open()
        self._left_tuple = None
        self._current_key = None
        self._right_blocks = []
        self._right_block_pos = 0
        self._block_tuples = []
        self._block_tuple_pos = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()
        while True:
            # (a) Emettre le prochain tuple correspondant dans le bloc courant
            while self._block_tuple_pos < len(self._block_tuples):
                right = self._block_tuples[self._block_tuple_pos]
                self._block_tuple_pos += 1
                if right is None:
                    continue
                # Filtrer : colonne indexee == cle gauche
                if right.val[self.right_index._col] != self._current_key:
                    continue
                joined = _concat(self._left_tuple, right)
                self.produit(joined)
                self.stop()
                return joined

            # (b) Bloc epuise -> prochain bloc
            while self._right_block_pos < len(self._right_blocks):
                block_no = self._right_blocks[self._right_block_pos]
                self._right_block_pos += 1
                self._block_tuples = self.right_table.get_block(block_no)
                self._block_tuple_pos = 0
                break
            else:
                # (c) Plus de blocs -> prochain tuple gauche
                self._left_tuple = self.left.next()
                if self._left_tuple is None:
                    self.stop()
                    return None
                self._current_key = self._left_tuple.val[self.left_col]
                self._right_blocks = self._lookup(self._current_key)
                self._right_block_pos = 0
                self._block_tuples = []
                self._block_tuple_pos = 0

    def close(self):
        self.left.close()
        if self._is_disk:
            self.right_table.close()

    def _lookup(self, key) -> List[int]:
        """Retourne les n° de blocs correspondant a la cle."""
        if self.high is not None:
            return self.right_index.range_search(key, self.high)
        if self.op == "==":
            return self.right_index.search(key)
        if not hasattr(self.right_index, "range_search"):
            raise TypeError(f"L'operateur '{self.op}' necessite un BPlusTreeIndex.")
        if self.op in (">=", ">"):
            return self.right_index.range_search(key, math.inf)
        if self.op in ("<=", "<"):
            return self.right_index.range_search(-math.inf, key)
        raise ValueError(f"Operateur non supporte : '{self.op}'")


def _concat(left, right):
    combined = Tuple(len(left.val) + len(right.val))
    combined.val = left.val + right.val
    return combined
