"""
Opérateur d'accès direct par bloc via un index (GET WITH INDEX).

Principe
--------
L'index stocke des paires (clé, n° de bloc).  Cet opérateur :

  1. Interroge l'index pour obtenir les numéros de blocs correspondant à
     la valeur recherchée.
  2. Lit chaque bloc directement depuis la table (accès direct, pas de
     full scan).
  3. Filtre les tuples du bloc dont la colonne indexée vaut la valeur
     recherchée.
  4. Retourne ces tuples un par un via l'interface open/next/close.

Pourquoi l'index seul ne suffit pas
-------------------------------------
L'index retourne des n° de blocs, pas des tuples.  Il faut un opérateur
pour lire le bloc et trouver les tuples correspondants à l'intérieur.
C'est ce que fait GetWithIndex.

Avantage vs FullScan
---------------------
• FullScan lit tous les blocs : O(N_blocs)
• GetWithIndex lit seulement les blocs renvoyés par l'index : O(k_blocs)
  où k_blocs ≪ N_blocs quand les valeurs indexées sont uniques ou rares.

Comparaison avec IndexScan
---------------------------
GetWithIndex est la version explicitement orientée "blocs" de IndexScan.
IndexScan a été mis à jour pour utiliser le même mécanisme en interne.
"""

from __future__ import annotations

import math
from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple


class GetWithIndex(Instrumentation, Operateur):
    """
    Accès direct à une table via un index orienté blocs.

    Paramètres
    ----------
    table :
        TableDisque ou TableMemoire.  Doit exposer ``get_block(block_no)``.
    index :
        StaticHashIndex | DynamicHashIndex | BPlusTreeIndex.
        Doit avoir été construit avec ``build()`` avant l'ouverture.
    value :
        Valeur de recherche (pour l'opérateur ``==`` ou bornes de range).
    op : str
        Opérateur de comparaison : ``'=='``, ``'>'``, ``'>='``, ``'<'``,
        ``'<='``.  Les opérateurs de range nécessitent un BPlusTreeIndex.
    high :
        Borne supérieure pour une recherche par intervalle.
        Si fourni, la recherche devient ``range_search(value, high)``.
    """

    def __init__(
        self,
        table,
        index,
        value,
        op:   str = "==",
        high        = None,
    ) -> None:
        super().__init__("GetWithIndex" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.table  = table
        self.index  = index
        self.value  = value
        self.op     = op
        self.high   = high

        # Colonne indexée (pour filtrer les tuples dans le bloc)
        self._col: int = -1

        # État de l'itération
        self._block_numbers:   List[int]   = []
        self._block_pos:       int         = 0
        self._current_block:   List[Tuple] = []
        self._slot:            int         = 0

    # ── interface Operateur ────────────────────────────────────────────────

    def open(self) -> None:
        self.start()
        if hasattr(self.table, "open"):
            self.table.open()
        self._col           = self.index._col
        self._block_numbers = self._query_index()
        self._block_pos     = 0
        self._current_block = []
        self._slot          = 0
        self.tuplesProduits = 0
        self.memoire        = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        while True:
            # ── (a) Émettre le prochain tuple correspondant dans le bloc courant ──
            while self._slot < len(self._current_block):
                t = self._current_block[self._slot]
                self._slot += 1
                if t is not None and self._matches(t):
                    self.produit(t)
                    self.stop()
                    return t

            # ── (b) Bloc courant épuisé → passer au suivant ──
            if self._block_pos >= len(self._block_numbers):
                self.stop()
                return None

            block_no            = self._block_numbers[self._block_pos]
            self._block_pos    += 1
            self._current_block = self.table.get_block(block_no)
            self._slot          = 0

    def close(self) -> None:
        if hasattr(self.table, "close"):
            self.table.close()
        self._block_numbers = []
        self._current_block = []
        self._block_pos     = 0
        self._slot          = 0

    # ── résolution de l'index ──────────────────────────────────────────────

    def _query_index(self) -> List[int]:
        """Interroge l'index et retourne les n° de blocs à lire."""

        if self.high is not None:
            if not hasattr(self.index, "range_search"):
                raise TypeError(
                    "La recherche par intervalle nécessite un BPlusTreeIndex."
                )
            return self.index.range_search(self.value, self.high)

        if self.op == "==":
            return self.index.search(self.value)

        if not hasattr(self.index, "range_search"):
            raise TypeError(
                f"L'opérateur '{self.op}' nécessite un BPlusTreeIndex."
            )

        _POS_INF =  math.inf
        _NEG_INF = -math.inf

        if self.op == ">=":
            return self.index.range_search(self.value, _POS_INF)
        if self.op == ">":
            return self.index.range_search(self.value, _POS_INF)
        if self.op == "<=":
            return self.index.range_search(_NEG_INF, self.value)
        if self.op == "<":
            return self.index.range_search(_NEG_INF, self.value)

        raise ValueError(f"Opérateur non supporté : '{self.op}'")

    # ── filtre intra-bloc ──────────────────────────────────────────────────

    def _matches(self, t: Tuple) -> bool:
        """Vérifie si le tuple *t* satisfait la condition de recherche."""
        if self._col < 0 or self._col >= len(t.val):
            return False
        v = t.val[self._col]
        if self.high is not None:
            return self.value <= v <= self.high
        op = self.op
        if op == "==":  return v == self.value
        if op == ">":   return v >  self.value
        if op == ">=":  return v >= self.value
        if op == "<":   return v <  self.value
        if op == "<=":  return v <= self.value
        return False
