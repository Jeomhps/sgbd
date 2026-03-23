"""
Jointure par tri-fusion (Sort-Merge Join).

Phase 1 — Tri : trier R et S sur la colonne de jointure.
Phase 2 — Fusion : deux pointeurs avancent sur R_triee et S_triee.

Cout theorique : M log M + N log N + (M + N)
"""

from __future__ import annotations
from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple


class SortMergeJoin(Instrumentation, Operateur):
    """
    Jointure par tri-fusion.

    Complexite : O((n+m) log(n+m))
    """

    def __init__(self, left, right, left_col, right_col):
        super().__init__("SortMergeJoin" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = left
        self.right = right
        self.left_col = left_col
        self.right_col = right_col

        self._sorted_left: List[Tuple] = []
        self._sorted_right: List[Tuple] = []
        self._left_pos = 0
        self._right_scan_pos = 0
        self._right_group: List[Tuple] = []
        self._right_group_pos = 0
        self._current_left: Optional[Tuple] = None
        self._current_key = None

    def open(self):
        self.start()

        # Phase 1 : collecte et tri
        self.left.open()
        left_tuples = []
        while True:
            t = self.left.next()
            if t is None:
                break
            left_tuples.append(t)
        self.left.close()

        self.right.open()
        right_tuples = []
        while True:
            t = self.right.next()
            if t is None:
                break
            right_tuples.append(t)
        self.right.close()

        self._sorted_left = sorted(left_tuples, key=lambda t: t.val[self.left_col])
        self._sorted_right = sorted(right_tuples, key=lambda t: t.val[self.right_col])

        self._left_pos = 0
        self._right_scan_pos = 0
        self._right_group = []
        self._right_group_pos = 0
        self._current_left = None
        self._current_key = None
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        while True:
            # (a) Emettre le prochain tuple du groupe S
            if self._right_group_pos < len(self._right_group):
                right_t = self._right_group[self._right_group_pos]
                self._right_group_pos += 1
                joined = _concat(self._current_left, right_t)
                self.produit(joined)
                self.stop()
                return joined

            # (b) Groupe S epuise -> prochain tuple R
            if self._left_pos >= len(self._sorted_left):
                self.stop()
                return None

            left_t = self._sorted_left[self._left_pos]
            left_key = left_t.val[self.left_col]
            self._left_pos += 1
            self._current_left = left_t

            # (c) Meme cle que le groupe precedent -> rejouer le groupe S
            if left_key == self._current_key and self._right_group:
                self._right_group_pos = 0
                continue

            # (d) Nouvelle cle -> avancer le pointeur S et collecter le groupe
            self._current_key = left_key

            while (self._right_scan_pos < len(self._sorted_right)
                   and self._sorted_right[self._right_scan_pos].val[self.right_col] < left_key):
                self._right_scan_pos += 1

            self._right_group = []
            j = self._right_scan_pos
            while (j < len(self._sorted_right)
                   and self._sorted_right[j].val[self.right_col] == left_key):
                self._right_group.append(self._sorted_right[j])
                j += 1

            self._right_group_pos = 0

    def close(self):
        self._sorted_left = []
        self._sorted_right = []
        self._right_group = []
        self._current_left = None
        self._current_key = None


def _concat(left, right):
    combined = Tuple(len(left.val) + len(right.val))
    combined.val = left.val + right.val
    return combined
