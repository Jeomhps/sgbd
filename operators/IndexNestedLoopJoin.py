"""
Jointure boucle imbriquée sur index (Index Nested Loop Join).

Principe
--------
Pour chaque tuple de la table gauche (outer loop) :
  1. Extraire la valeur de clé  left_tuple.val[left_col]
  2. Interroger l'index de la table droite : index.search(key)
     → liste d'indices de tuples droits correspondants
  3. Récupérer chaque tuple droit via right_table.get_tuple(idx)
  4. Produire le tuple concaténé (gauche + droit)

Avantage par rapport au Join classique
---------------------------------------
• O(n × k) au lieu de O(n × m) : on ne parcourt que les k lignes
  correspondantes côté droit (souvent k ≪ m).
• Pas de chargement de la table droite en mémoire.

Contraintes
-----------
• L'index doit avoir été construit AVANT l'ouverture de l'opérateur.
• La table droite est passée directement (TableDisque ou TableMemoire),
  pas encapsulée dans un opérateur.
• Seul le join par égalité (==) est supporté nativement.
  Pour les plages, utiliser un BPlusTreeIndex et passer op + high.

Comparaison avec Join / HashJoin
---------------------------------
• Join     : cache toute la table droite en mémoire, O(n × m)
• HashJoin : construit une hash-table de la droite en mémoire, O(n + m)
• IndexNLJ : aucun cache mémoire, index lu sur disque, O(n × k)
"""

from __future__ import annotations

import math
from typing import List, Optional

from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.TableDisque import TableDisque
from core.Tuple import Tuple


class IndexNestedLoopJoin(Instrumentation, Operateur):
    """
    Jointure boucle imbriquée sur index.

    Paramètres
    ----------
    left        : Operateur
        Source des tuples gauches (outer loop).
    right_table : TableDisque | TableMemoire
        Table droite — accès direct par indice, pas d'itération.
    right_index : StaticHashIndex | DynamicHashIndex | BPlusTreeIndex
        Index pré-construit sur la colonne de jointure de la table droite.
    left_col    : int
        Indice de la colonne gauche utilisée comme clé de jointure.
    op          : str
        Opérateur de comparaison (défaut '==').
        Pour op ≠ '==' un BPlusTreeIndex est requis.
    high        :
        Borne supérieure pour une recherche par intervalle (BPlusTree uniquement).
    """

    def __init__(
        self,
        left,
        right_table,
        right_index,
        left_col:  int,
        op:        str = "==",
        high             = None,
    ) -> None:
        super().__init__("IndexNLJ" + str(Instrumentation.number))
        Instrumentation.number += 1

        self.left         = left
        self.right_table  = right_table
        self.right_index  = right_index
        self.left_col     = left_col
        self.op           = op
        self.high         = high

        self._is_disk:          bool            = isinstance(right_table, TableDisque)
        self._left_tuple:       Optional[Tuple] = None
        self._current_key:      object          = None   # clé du tuple gauche courant
        # Niveau 1 : liste des n° de blocs retournés par l'index
        self._right_blocks:     List[int]       = []
        self._right_block_pos:  int             = 0
        # Niveau 2 : tuples du bloc courant, après filtrage
        self._block_tuples:     List[Tuple]     = []
        self._block_tuple_pos:  int             = 0

    # ── interface Operateur ────────────────────────────────────────────────

    def open(self) -> None:
        self.start()
        self.left.open()
        if self._is_disk:
            self.right_table.open()
        self._left_tuple      = None
        self._current_key     = None
        self._right_blocks    = []
        self._right_block_pos = 0
        self._block_tuples    = []
        self._block_tuple_pos = 0
        self.tuplesProduits   = 0
        self.memoire          = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        while True:
            # ── (a) Émettre le prochain tuple correspondant dans le bloc courant ──
            while self._block_tuple_pos < len(self._block_tuples):
                right = self._block_tuples[self._block_tuple_pos]
                self._block_tuple_pos += 1
                if right is None:
                    continue
                # Filtrer : seuls les tuples dont la colonne indexée == clé gauche
                right_col = self.right_index._col
                if right.val[right_col] != self._current_key:
                    continue
                joined = self._concat(self._left_tuple, right)
                self.produit(joined)
                self.stop()
                return joined

            # ── (b) Bloc courant épuisé → passer au prochain bloc ──
            while self._right_block_pos < len(self._right_blocks):
                block_no              = self._right_blocks[self._right_block_pos]
                self._right_block_pos += 1
                self._block_tuples    = self._get_block(block_no)
                self._block_tuple_pos = 0
                break   # retourner en (a) pour parcourir ce bloc
            else:
                # ── (c) Plus de blocs → passer au prochain tuple gauche ──
                self._left_tuple = self.left.next()
                if self._left_tuple is None:
                    self.stop()
                    return None

                self._current_key     = self._left_tuple.val[self.left_col]
                self._right_blocks    = self._lookup(self._current_key)
                self._right_block_pos = 0
                self._block_tuples    = []
                self._block_tuple_pos = 0

    def close(self) -> None:
        self.left.close()
        if self._is_disk:
            self.right_table.close()
        self._left_tuple      = None
        self._current_key     = None
        self._right_blocks    = []
        self._right_block_pos = 0
        self._block_tuples    = []
        self._block_tuple_pos = 0

    # ── requête sur l'index ────────────────────────────────────────────────

    def _lookup(self, key) -> List[int]:
        """Retourne les indices droits correspondant à *key* selon self.op."""

        # Recherche par intervalle (BPlusTree uniquement)
        if self.high is not None:
            if not hasattr(self.right_index, "range_search"):
                raise TypeError(
                    "La recherche par intervalle nécessite un BPlusTreeIndex."
                )
            return self.right_index.range_search(key, self.high)

        # Égalité — tous les index
        if self.op == "==":
            return self.right_index.search(key)

        # Comparaisons étendues — BPlusTree uniquement
        if not hasattr(self.right_index, "range_search"):
            raise TypeError(
                f"L'opérateur '{self.op}' nécessite un BPlusTreeIndex."
            )

        _NEG_INF = -math.inf
        _POS_INF =  math.inf
        col      = self.right_index._col

        if self.op == ">=":
            return self.right_index.range_search(key, _POS_INF)

        if self.op == ">":
            candidates = self.right_index.range_search(key, _POS_INF)
            result = []
            for i in candidates:
                t = self._get_right(i)
                if t is not None and t.val[col] > key:
                    result.append(i)
            return result

        if self.op == "<=":
            return self.right_index.range_search(_NEG_INF, key)

        if self.op == "<":
            candidates = self.right_index.range_search(_NEG_INF, key)
            result = []
            for i in candidates:
                t = self._get_right(i)
                if t is not None and t.val[col] < key:
                    result.append(i)
            return result

        raise ValueError(f"Opérateur non supporté : '{self.op}'")

    # ── accès table droite par bloc ────────────────────────────────────────

    def _get_block(self, block_no: int) -> List[Tuple]:
        """Retourne tous les tuples du bloc *block_no* de la table droite."""
        return self.right_table.get_block(block_no)

    # ── concaténation ──────────────────────────────────────────────────────

    @staticmethod
    def _concat(left: Tuple, right: Tuple) -> Tuple:
        combined     = Tuple(len(left.val) + len(right.val))
        combined.val = left.val + right.val
        return combined
