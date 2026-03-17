"""
Jointure par tri-fusion (Sort-Merge Join).

Principe (cf. cours4.pdf p.113-119)
-------------------------------------
Phase 1 — Tri :
    Trier R sur la colonne de jointure X  →  R_triée
    Trier S sur la colonne de jointure X  →  S_triée
    (Le cours utilise le tri rapide externe ; ici on collecte tous les tuples
    en mémoire puis on appelle sorted(), ce qui est équivalent dans le contexte
    de ce moteur en mémoire.)

Phase 2 — Fusion (two-pointer merge) :
    Deux pointeurs avancent sur R_triée et S_triée.
    • R_triée est parcourue exactement 1 fois (pointeur gauche ne recule jamais).
    • Quand plusieurs tuples consécutifs de R ont la même clé, le groupe S
      correspondant est *rejoué* pour chacun d'eux (d'où « plusieurs parcours
      de parties de S-triée » selon le cours).

Coût théorique : M log M + N log N + (M + N)

Comparaison avec les autres jointures du moteur
-------------------------------------------------
Join          : O(n × m)   — boucle imbriquée, cache S en mémoire
HashJoin      : O(n + m)   — hash-table sur S, probe avec R
IndexNLJ      : O(n × k)   — index sur S, k matches par tuple R
SortMergeJoin : O((n+m) log(n+m))  — tri puis fusion linéaire
"""

from __future__ import annotations

from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple


class SortMergeJoin(Instrumentation, Operateur):
    """
    Jointure par tri-fusion.

    Paramètres
    ----------
    left      : Operateur  — source des tuples gauches (R)
    right     : Operateur  — source des tuples droits  (S)
    left_col  : int        — indice de la colonne de jointure dans R
    right_col : int        — indice de la colonne de jointure dans S
    """

    def __init__(
        self,
        left:      Operateur,
        right:     Operateur,
        left_col:  int,
        right_col: int,
    ) -> None:
        super().__init__("SortMergeJoin" + str(Instrumentation.number))
        Instrumentation.number += 1

        self.left      = left
        self.right     = right
        self.left_col  = left_col
        self.right_col = right_col

        # État interne (initialisé dans open)
        self._sorted_left:       List[Tuple]       = []
        self._sorted_right:      List[Tuple]       = []
        self._left_pos:          int               = 0   # pointeur dans R_triée
        self._right_scan_pos:    int               = 0   # début du scan dans S_triée
        self._right_group:       List[Tuple]       = []  # groupe S courant (même clé)
        self._right_group_pos:   int               = 0   # position dans le groupe S
        self._current_left:      Optional[Tuple]   = None  # tuple R actif
        self._current_key:       Optional[int]     = None  # clé du groupe courant

    # ── interface Operateur ────────────────────────────────────────────────

    def open(self) -> None:
        self.start()

        # ── Phase 1 : collecte et tri de R ──
        self.left.open()
        left_tuples: List[Tuple] = []
        while True:
            t = self.left.next()
            if t is None:
                break
            left_tuples.append(t)
        self.left.close()

        # ── Phase 1 : collecte et tri de S ──
        self.right.open()
        right_tuples: List[Tuple] = []
        while True:
            t = self.right.next()
            if t is None:
                break
            right_tuples.append(t)
        self.right.close()

        self._sorted_left  = sorted(left_tuples,  key=lambda t: t.val[self.left_col])
        self._sorted_right = sorted(right_tuples, key=lambda t: t.val[self.right_col])

        # ── Réinitialisation de l'état de la phase fusion ──
        self._left_pos        = 0
        self._right_scan_pos  = 0
        self._right_group     = []
        self._right_group_pos = 0
        self._current_left    = None
        self._current_key     = None
        self.tuplesProduits   = 0
        self.memoire          = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        while True:
            # ── (a) Émettre le prochain tuple du groupe S pour le tuple R courant ──
            if self._right_group_pos < len(self._right_group):
                right_t = self._right_group[self._right_group_pos]
                self._right_group_pos += 1
                joined = _concat(self._current_left, right_t)
                self.produit(joined)
                self.stop()
                return joined

            # ── (b) Groupe S épuisé → passer au prochain tuple R ──
            if self._left_pos >= len(self._sorted_left):
                self.stop()
                return None

            left_t   = self._sorted_left[self._left_pos]
            left_key = left_t.val[self.left_col]
            self._left_pos += 1
            self._current_left = left_t

            # ── (c) Même clé que le groupe précédent → rejouer le groupe S ──
            if left_key == self._current_key and self._right_group:
                self._right_group_pos = 0
                continue

            # ── (d) Nouvelle clé → avancer le pointeur S et collecter le groupe ──
            self._current_key = left_key

            # Sauter les tuples S dont la clé est strictement inférieure
            while (
                self._right_scan_pos < len(self._sorted_right)
                and self._sorted_right[self._right_scan_pos].val[self.right_col] < left_key
            ):
                self._right_scan_pos += 1

            # Collecter tous les tuples S avec cette clé
            self._right_group = []
            j = self._right_scan_pos
            while (
                j < len(self._sorted_right)
                and self._sorted_right[j].val[self.right_col] == left_key
            ):
                self._right_group.append(self._sorted_right[j])
                j += 1

            self._right_group_pos = 0
            # Si aucune correspondance, la boucle reprend → prochain tuple R

    def close(self) -> None:
        self._sorted_left     = []
        self._sorted_right    = []
        self._right_group     = []
        self._current_left    = None
        self._current_key     = None
        self._left_pos        = 0
        self._right_scan_pos  = 0
        self._right_group_pos = 0


# ── helpers ────────────────────────────────────────────────────────────────

def _concat(left: Tuple, right: Tuple) -> Tuple:
    combined     = Tuple(len(left.val) + len(right.val))
    combined.val = left.val + right.val
    return combined
