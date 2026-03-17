"""
Index par hachage statique.

Structure
---------
• nb_buckets seaux fixes, non redimensionnables.
• Fonction de hachage : hash(key) % nb_buckets
• Chaque seau contient une liste de paires (clé, indice_tuple).
  La collision est résolue par chaînage en mémoire (liste Python).

Complexité
----------
• Construction : O(n)
• Recherche    : O(1) en moyenne, O(n) dans le pire cas (collisions)

Limitation
----------
• Efficacité se dégrade si nb_buckets est mal choisi par rapport à n.
• Ne supporte pas les requêtes par intervalle.
"""

from __future__ import annotations

from typing import List


class StaticHashIndex:
    """
    Index par hachage statique à nb_buckets seaux.

    Paramètres
    ----------
    nb_buckets : int
        Nombre de seaux. Choisir proche du nombre de tuples distincts.
    """

    def __init__(self, nb_buckets: int = 10) -> None:
        self.nb_buckets = nb_buckets
        # Chaque seau : liste de (clé, indice_tuple)
        self._buckets: list[list[tuple]] = [[] for _ in range(nb_buckets)]
        self._col: int = -1
        self._size: int = 0        # nombre d'entrées insérées

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """
        Parcourt *table* et indexe la colonne *col*.

        Accepte une TableMemoire ou une TableDisque.
        """
        self._col = col
        self._buckets = [[] for _ in range(self.nb_buckets)]
        self._size = 0

        for idx, t in enumerate(self._iter_table(table)):
            key = t.val[col]
            self._insert(key, idx)

        print(
            f"[StaticHash] index construit : {self._size} entrées "
            f"dans {self.nb_buckets} seaux (col={col})"
        )

    def _insert(self, key, tuple_idx: int) -> None:
        b = self._bucket_id(key)
        self._buckets[b].append((key, tuple_idx))
        self._size += 1

    # ── recherche ──────────────────────────────────────────────────────────

    def search(self, value) -> List[int]:
        """Retourne les indices des tuples dont la clé vaut *value*."""
        b = self._bucket_id(value)
        return [idx for key, idx in self._buckets[b] if key == value]

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        sizes   = [len(b) for b in self._buckets]
        non_empty = sum(1 for s in sizes if s > 0)
        avg     = self._size / self.nb_buckets if self.nb_buckets else 0
        mx      = max(sizes) if sizes else 0
        return (
            f"StaticHashIndex | seaux={self.nb_buckets} "
            f"non-vides={non_empty} | "
            f"avg/seau={avg:.1f} max/seau={mx} "
            f"entrées={self._size}"
        )

    # ── helpers ────────────────────────────────────────────────────────────

    def _bucket_id(self, key) -> int:
        return hash(key) % self.nb_buckets

    @staticmethod
    def _iter_table(table):
        """Itère sur les tuples d'une TableMemoire ou TableDisque."""
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
            # TableMemoire
            yield from table.valeurs
