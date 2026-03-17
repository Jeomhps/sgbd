"""
Index par arbre B+.

Structure
---------
• Nœuds internes : clés séparatrices + pointeurs vers les fils.
• Feuilles       : paires (clé, [indices_tuples]) triées, chaînées entre elles.
• Ordre m        : chaque nœud contient au plus m-1 clés (m fils pour les internes).

Propriétés
----------
• Toutes les données sont dans les feuilles.
• La clé promue lors d'un éclatement de feuille est copiée (pas déplacée).
• Le chaînage des feuilles permet les requêtes par intervalle en O(log n + k).

Complexité
----------
• Construction : O(n log n)
• Recherche exacte    : O(log n)
• Recherche par plage : O(log n + k) avec k = taille du résultat

Paramètre clé
-------------
order : int (défaut 4)
    Nombre maximum de fils d'un nœud interne (= max clés + 1).
    Minimum recommandé : 3.
"""

from __future__ import annotations

from typing import Any, List, Optional


# ──────────────────────────────────────────────────────────────────────────────
# Nœuds
# ──────────────────────────────────────────────────────────────────────────────

class _Leaf:
    """Nœud feuille du B+."""

    def __init__(self, order: int) -> None:
        self.order   = order
        self.keys:    list       = []          # clés triées
        self.records: list[list[int]] = []     # records[i] = liste d'indices pour keys[i]
        self.next:    Optional[_Leaf] = None   # chaînage vers la feuille suivante

    # -- insertion dans la feuille --------------------------------------------

    def insert(self, key: Any, idx: int) -> None:
        pos = self._bisect(key)
        if pos < len(self.keys) and self.keys[pos] == key:
            self.records[pos].append(idx)          # clé dupliquée
        else:
            self.keys.insert(pos, key)
            self.records.insert(pos, [idx])

    # -- éclatement -----------------------------------------------------------

    def split(self) -> tuple["_Leaf", Any]:
        """
        Retourne (nouvelle_feuille_droite, clé_promue).
        La clé promue est COPIÉE (elle reste aussi dans la feuille droite).
        """
        mid   = len(self.keys) // 2
        right = _Leaf(self.order)
        right.keys    = self.keys[mid:]
        right.records = self.records[mid:]
        right.next    = self.next
        self.keys     = self.keys[:mid]
        self.records  = self.records[:mid]
        self.next     = right
        return right, right.keys[0]

    def is_overflow(self) -> bool:
        return len(self.keys) >= self.order        # >= order → order-1 max

    # -- recherche binaire ----------------------------------------------------

    def _bisect(self, key: Any) -> int:
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def search(self, key: Any) -> List[int]:
        pos = self._bisect(key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return list(self.records[pos])
        return []


class _Internal:
    """Nœud interne du B+."""

    def __init__(self, order: int) -> None:
        self.order    = order
        self.keys:     list = []
        self.children: list = []    # len == len(keys) + 1

    # -- quel fils suivre ? ---------------------------------------------------

    def child_pos(self, key: Any) -> int:
        """Indice du fils à descendre pour *key*."""
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] <= key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    # -- éclatement -----------------------------------------------------------

    def split(self) -> tuple["_Internal", Any]:
        """
        Retourne (nouveau_nœud_interne_droit, clé_promue).
        La clé promue est DÉPLACÉE (elle n'est ni dans le gauche ni dans le droit).
        """
        mid      = len(self.keys) // 2
        promoted = self.keys[mid]
        right    = _Internal(self.order)
        right.keys     = self.keys[mid + 1:]
        right.children = self.children[mid + 1:]
        self.keys      = self.keys[:mid]
        self.children  = self.children[:mid + 1]
        return right, promoted

    def is_overflow(self) -> bool:
        return len(self.keys) >= self.order


# ──────────────────────────────────────────────────────────────────────────────
# Arbre B+
# ──────────────────────────────────────────────────────────────────────────────

class BPlusTreeIndex:
    """
    Index par arbre B+.

    Paramètres
    ----------
    order : int
        Ordre de l'arbre (nombre max de fils d'un nœud interne).
        Chaque nœud contient au plus ``order - 1`` clés.
        Valeur minimale : 3.
    """

    def __init__(self, order: int = 4) -> None:
        if order < 3:
            raise ValueError("L'ordre du B+ doit être ≥ 3")
        self.order = order
        self._root: _Leaf | _Internal = _Leaf(order)
        self._col:  int  = -1
        self._size: int  = 0     # nombre total d'entrées
        self._height: int = 1    # hauteur de l'arbre

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """Parcourt *table* et indexe la colonne *col*."""
        self._col   = col
        self._root  = _Leaf(self.order)
        self._size  = 0
        self._height = 1

        for idx, t in enumerate(self._iter_table(table)):
            self.insert(t.val[col], idx)

        print(
            f"[B+Tree] index construit : {self._size} entrées "
            f"hauteur={self._height} ordre={self.order} (col={col})"
        )

    def insert(self, key: Any, tuple_idx: int) -> None:
        """Insère (key, tuple_idx) dans l'arbre."""
        pkey, new_child = self._insert(self._root, key, tuple_idx)
        if new_child is not None:
            # La racine a éclaté → nouvelle racine
            new_root = _Internal(self.order)
            new_root.keys     = [pkey]
            new_root.children = [self._root, new_child]
            self._root = new_root
            self._height += 1
        self._size += 1

    # ── recherche ──────────────────────────────────────────────────────────

    def search(self, value: Any) -> List[int]:
        """Retourne les indices des tuples dont la clé vaut *value*."""
        leaf = self._find_leaf(value)
        return leaf.search(value)

    def range_search(self, low: Any, high: Any) -> List[int]:
        """
        Retourne les indices des tuples dont la clé est dans [low, high].
        Exploite le chaînage des feuilles pour un parcours efficace.
        """
        leaf    = self._find_leaf(low)
        results: List[int] = []

        while leaf is not None:
            for i, key in enumerate(leaf.keys):
                if key > high:
                    return results
                if key >= low:
                    results.extend(leaf.records[i])
            leaf = leaf.next

        return results

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        nb_leaves, nb_internal = self._count_nodes(self._root)
        return (
            f"BPlusTreeIndex | ordre={self.order} hauteur={self._height} "
            f"feuilles={nb_leaves} nœuds_internes={nb_internal} "
            f"entrées={self._size}"
        )

    # ── helpers internes ───────────────────────────────────────────────────

    def _insert(
        self,
        node: _Leaf | _Internal,
        key:  Any,
        idx:  int,
    ) -> tuple[Any, Optional[_Leaf | _Internal]]:
        """
        Insère récursivement dans *node*.
        Retourne (clé_promue, nouveau_nœud_droit) si éclatement, sinon (None, None).
        """
        if isinstance(node, _Leaf):
            node.insert(key, idx)
            if node.is_overflow():
                right, pkey = node.split()
                return pkey, right
            return None, None

        # Nœud interne : descendre dans le bon fils
        pos       = node.child_pos(key)
        pkey, new_right = self._insert(node.children[pos], key, idx)

        if new_right is not None:
            # Insérer la clé promue et le nouveau fils droit
            node.keys.insert(pos, pkey)
            node.children.insert(pos + 1, new_right)
            if node.is_overflow():
                right, pkey2 = node.split()
                return pkey2, right

        return None, None

    def _find_leaf(self, key: Any) -> _Leaf:
        node = self._root
        while isinstance(node, _Internal):
            node = node.children[node.child_pos(key)]
        return node  # type: ignore[return-value]

    def _count_nodes(self, node) -> tuple[int, int]:
        """Retourne (nb_feuilles, nb_internes) dans le sous-arbre."""
        if isinstance(node, _Leaf):
            return 1, 0
        leaves, internals = 0, 1
        for child in node.children:
            l, i = self._count_nodes(child)
            leaves   += l
            internals += i
        return leaves, internals

    @staticmethod
    def _iter_table(table):
        from core.TableDisque import TableDisque
        if isinstance(table, TableDisque):
            from core.FullScanTableDisque import FullScanTableDisque
            scan = FullScanTableDisque(table)
            scan.open()
            while True:
                t = scan.next()
                if t is None:
                    break
                yield t
            scan.close()
        else:
            yield from table.valeurs
