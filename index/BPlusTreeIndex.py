"""
Index par arbre B+ — feuilles stockées sur disque (IndexDisque).

Structure
---------
• Construction : toutes les paires (clé, indice_tuple) sont insérées dans
  un arbre B+ en mémoire, puis les feuilles sont sérialisées triées dans
  un IndexDisque.
• La structure des nœuds internes est reconstruite en mémoire depuis les
  données disque à la première recherche (légère, O(n/B) entrées de
  pointeurs de blocs).

Stockage disque
---------------
Fichier .dat  : IndexDisque — paires (clé, idx) triées par clé.
               Pour les clés dupliquées : plusieurs entrées consécutives
               avec la même clé.
Fichier .dir  : métadonnées minimes
               col         : 4B (colonne indexée)
               nb_entries  : 4B

Complexité
----------
• Construction          : O(n log n)
• Recherche exacte      : O(log(n/B))   — recherche dichotomique sur blocs
• Recherche par plage   : O(log(n/B) + k)  avec k = taille du résultat

Paramètre clé
-------------
order : int (défaut 4) — utilisé uniquement pendant la construction
        en mémoire pour contrôler la hauteur de l'arbre intermédiaire.
"""

from __future__ import annotations

import os
import struct
import tempfile
from typing import Any, List, Optional

from core.IndexDisque import IndexDisque

_DEFAULT_DIR = tempfile.gettempdir()


# ──────────────────────────────────────────────────────────────────────────────
# Nœuds B+ mémoire (utilisés uniquement pendant la construction)
# ──────────────────────────────────────────────────────────────────────────────

class _Leaf:
    def __init__(self, order: int) -> None:
        self.order   = order
        self.keys:    list             = []
        self.records: list[list[int]]  = []
        self.next:    Optional[_Leaf]  = None

    def insert(self, key: Any, idx: int) -> None:
        pos = self._bisect(key)
        if pos < len(self.keys) and self.keys[pos] == key:
            self.records[pos].append(idx)
        else:
            self.keys.insert(pos, key)
            self.records.insert(pos, [idx])

    def split(self) -> tuple["_Leaf", Any]:
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
        return len(self.keys) >= self.order

    def _bisect(self, key: Any) -> int:
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid
        return lo


class _Internal:
    def __init__(self, order: int) -> None:
        self.order    = order
        self.keys:     list = []
        self.children: list = []

    def child_pos(self, key: Any) -> int:
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] <= key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def split(self) -> tuple["_Internal", Any]:
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
# Index
# ──────────────────────────────────────────────────────────────────────────────

class BPlusTreeIndex:
    """
    Index par arbre B+ — feuilles persistées dans un IndexDisque.

    Paramètres
    ----------
    order     : ordre de l'arbre (taille max des nœuds pendant construction).
    file_path : chemin du fichier .dat. Si None, fichier temporaire.
    """

    def __init__(
        self,
        order:     int       = 4,
        file_path: str | None = None,
    ) -> None:
        if order < 3:
            raise ValueError("L'ordre du B+ doit être >= 3")
        self.order = order

        if file_path is None:
            file_path = os.path.join(_DEFAULT_DIR, "bplus_index.dat")
        self._data_path = file_path
        self._dir_path  = file_path + ".dir"

        self._storage: IndexDisque = IndexDisque(self._data_path)
        self._col:  int = -1
        self._size: int = 0

        # Arbre B+ mémoire (uniquement pendant construction)
        self._root: _Leaf | _Internal = _Leaf(order)
        self._height: int = 1

        # Indicateur : données déjà sur disque ?
        self._built: bool = False

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """Parcourt *table*, construit l'arbre B+ en mémoire, sérialise sur disque."""
        self._col    = col
        self._root   = _Leaf(self.order)
        self._height = 1
        self._size   = 0
        self._built  = False
        block_size   = self._get_block_size(table)

        # Chaque entrée stocke (clé, n° de bloc) au lieu de (clé, indice tuple).
        for idx, t in enumerate(self._iter_table(table)):
            block_no = idx // block_size
            self._insert_mem(t.val[col], block_no)

        # Sérialiser les feuilles triées dans IndexDisque
        self._flush_to_disk()
        self._built = True

        # index construit

    # ── recherche exacte ───────────────────────────────────────────────────

    def search(self, value: Any) -> List[int]:
        """
        Retourne les numéros de blocs contenant des tuples dont la clé vaut *value*.

        Les doublons sont supprimés (plusieurs tuples dans le même bloc → un seul
        numéro de bloc retourné) tout en préservant l'ordre de découverte.
        """
        self._ensure_loaded()
        pos = self._bisect_pos(value)
        if pos is None:
            return []

        seen: dict[int, None] = {}
        self._storage.open()
        i = pos
        while i < self._storage.table_size:
            e = self._storage.get_entry(i)
            if e is None or e[0] != value:
                break
            if e[1] not in seen:
                seen[e[1]] = None
            i += 1
        self._storage.close()
        return list(seen)

    # ── recherche par intervalle ───────────────────────────────────────────

    def range_search(self, low: Any, high: Any) -> List[int]:
        """
        Retourne les numéros de blocs contenant des tuples dont la clé est dans [low, high].

        Les doublons sont supprimés tout en préservant l'ordre de découverte.
        """
        self._ensure_loaded()
        pos = self._bisect_pos_ge(low)
        if pos is None:
            return []

        seen: dict[int, None] = {}
        self._storage.open()
        i = pos
        while i < self._storage.table_size:
            e = self._storage.get_entry(i)
            if e is None or e[0] > high:
                break
            if e[1] not in seen:
                seen[e[1]] = None
            i += 1
        self._storage.close()
        return list(seen)

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        nb_leaves, nb_internal = self._count_nodes(self._root)
        return (
            f"BPlusTreeIndex | ordre={self.order} hauteur={self._height} "
            f"feuilles={nb_leaves} noeuds_internes={nb_internal} "
            f"entrées={self._size}"
        )

    # ── helpers construction mémoire ───────────────────────────────────────

    def insert(self, key: Any, tuple_idx: int) -> None:
        self._insert_mem(key, tuple_idx)

    def _insert_mem(self, key: Any, tuple_idx: int) -> None:
        pkey, new_child = self._insert_node(self._root, key, tuple_idx)
        if new_child is not None:
            new_root           = _Internal(self.order)
            new_root.keys      = [pkey]
            new_root.children  = [self._root, new_child]
            self._root         = new_root
            self._height      += 1
        self._size += 1

    def _insert_node(
        self,
        node: _Leaf | _Internal,
        key:  Any,
        idx:  int,
    ) -> tuple[Any, Optional[_Leaf | _Internal]]:
        if isinstance(node, _Leaf):
            node.insert(key, idx)
            if node.is_overflow():
                right, pkey = node.split()
                return pkey, right
            return None, None

        pos             = node.child_pos(key)
        pkey, new_right = self._insert_node(node.children[pos], key, idx)

        if new_right is not None:
            node.keys.insert(pos, pkey)
            node.children.insert(pos + 1, new_right)
            if node.is_overflow():
                right, pkey2 = node.split()
                return pkey2, right

        return None, None

    # ── sérialisation ─────────────────────────────────────────────────────

    def _flush_to_disk(self) -> None:
        """Parcourt les feuilles (déjà triées) et écrit les paires sur disque."""
        flat: list[tuple] = []
        node = self._root
        while isinstance(node, _Internal):
            node = node.children[0]
        while node is not None:
            for i, key in enumerate(node.keys):
                for idx in node.records[i]:
                    flat.append((key, idx))
            node = node.next

        self._storage.write_entries(flat)
        self._save_dir()

    def _save_dir(self) -> None:
        with open(self._dir_path, "wb") as f:
            f.write(struct.pack("Ii", self._size, self._col))

    def _load_dir(self) -> None:
        with open(self._dir_path, "rb") as f:
            self._size, self._col = struct.unpack("Ii", f.read(8))
        self._storage.open()
        # table_size already read by open()
        self._storage.close()

    def _ensure_loaded(self) -> None:
        if not self._built:
            if os.path.exists(self._dir_path):
                self._load_dir()
                self._built = True
            else:
                raise RuntimeError(
                    "BPlusTreeIndex : index non construit (appeler build() d'abord)."
                )

    # ── recherche dichotomique sur disque ──────────────────────────────────

    def _bisect_pos_ge(self, key: Any) -> Optional[int]:
        """
        Retourne l'indice de la première entrée avec clé >= *key*,
        ou None si le fichier est vide.
        """
        self._storage.open()
        n = self._storage.table_size
        if n == 0:
            self._storage.close()
            return None

        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            e   = self._storage.get_entry(mid)
            if e is not None and e[0] < key:
                lo = mid + 1
            else:
                hi = mid
        self._storage.close()
        return lo if lo < n else None

    def _bisect_pos(self, key: Any) -> Optional[int]:
        """
        Retourne l'indice de la première entrée avec clé == *key*,
        ou None si absente.
        """
        self._storage.open()
        n = self._storage.table_size
        if n == 0:
            self._storage.close()
            return None

        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            e   = self._storage.get_entry(mid)
            if e is not None and e[0] < key:
                lo = mid + 1
            else:
                hi = mid

        # Vérifier que lo pointe bien sur key
        e = self._storage.get_entry(lo) if lo < n else None
        self._storage.close()
        if e is not None and e[0] == key:
            return lo
        return None

    # ── comptage nœuds (arbre mémoire — utile après construction) ─────────

    def _count_nodes(self, node) -> tuple[int, int]:
        if isinstance(node, _Leaf):
            return 1, 0
        leaves, internals = 0, 1
        for child in node.children:
            l, i = self._count_nodes(child)
            leaves    += l
            internals += i
        return leaves, internals

    # ── itération table ────────────────────────────────────────────────────

    @staticmethod
    def _get_block_size(table) -> int:
        """Retourne la taille de bloc de *table* (1 pour TableMemoire)."""
        return getattr(table, "block_size", 1)

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
