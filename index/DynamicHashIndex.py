"""
Index par hachage dynamique (hachage extensible — Fagin et al. 1979).

Principe
--------
Un répertoire de 2^d pointeurs (d = profondeur globale) indexe des seaux.
Chaque seau a une profondeur locale ℓ ≤ d.

Quand un seau sature :
  • Si ℓ < d  → on éclate juste ce seau (pas de doublement du répertoire).
  • Si ℓ = d  → on double le répertoire (d ← d+1) puis on éclate.

Clé discriminante à chaque niveau : bit ℓ de hash(key).

Complexité
----------
• Construction : O(n) amortie
• Recherche    : O(1) en moyenne
• Pas de requête par intervalle.

Paramètre clé
-------------
bucket_capacity : taille maximale d'un seau avant éclatement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

_MAX_DEPTH = 30   # garde-fou contre les clés toutes identiques


@dataclass
class _Bucket:
    local_depth: int
    entries: list = field(default_factory=list)   # [(key, tuple_idx)]
    capacity: int = 4


class DynamicHashIndex:
    """
    Hachage extensible (extendible hashing).

    Paramètres
    ----------
    bucket_capacity : int
        Nombre maximum d'entrées par seau avant éclatement.
    """

    def __init__(self, bucket_capacity: int = 4) -> None:
        self.bucket_capacity = bucket_capacity
        self.global_depth    = 1
        # Répertoire initial : 2 seaux de profondeur 1
        b0 = _Bucket(local_depth=1, capacity=bucket_capacity)
        b1 = _Bucket(local_depth=1, capacity=bucket_capacity)
        self.directory: list[_Bucket] = [b0, b1]
        self._col: int  = -1
        self._size: int = 0

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """Parcourt *table* et indexe la colonne *col*."""
        self._col = col
        # Réinitialisation
        self.global_depth = 1
        b0 = _Bucket(local_depth=1, capacity=self.bucket_capacity)
        b1 = _Bucket(local_depth=1, capacity=self.bucket_capacity)
        self.directory = [b0, b1]
        self._size = 0

        for idx, t in enumerate(self._iter_table(table)):
            self._insert(t.val[col], idx)

        print(
            f"[DynamicHash] index construit : {self._size} entrées | "
            f"profondeur globale={self.global_depth} "
            f"répertoire={len(self.directory)} entrées (col={col})"
        )

    def _insert(self, key, tuple_idx: int) -> None:
        di     = self._dir_index(key)
        bucket = self.directory[di]
        bucket.entries.append((key, tuple_idx))
        self._size += 1
        if len(bucket.entries) > bucket.capacity:
            self._split(bucket)

    # ── recherche ──────────────────────────────────────────────────────────

    def search(self, value) -> List[int]:
        """Retourne les indices des tuples dont la clé vaut *value*."""
        di = self._dir_index(value)
        return [idx for k, idx in self.directory[di].entries if k == value]

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        seaux = list({id(b): b for b in self.directory}.values())
        sizes = [len(b.entries) for b in seaux]
        avg   = sum(sizes) / len(sizes) if sizes else 0
        return (
            f"DynamicHashIndex | profondeur globale={self.global_depth} "
            f"répertoire={len(self.directory)} "
            f"seaux distincts={len(seaux)} "
            f"avg/seau={avg:.1f} max/seau={max(sizes, default=0)} "
            f"entrées={self._size}"
        )

    # ── helpers internes ───────────────────────────────────────────────────

    def _hash(self, key) -> int:
        return hash(key) & 0x7FFF_FFFF   # entier positif 31 bits

    def _dir_index(self, key) -> int:
        """Indice dans le répertoire pour *key* avec la profondeur courante."""
        return self._hash(key) & ((1 << self.global_depth) - 1)

    def _split(self, bucket: _Bucket) -> None:
        """Éclate *bucket* en deux nouveaux seaux."""
        old_depth = bucket.local_depth

        # Garde-fou : clés toutes identiques
        if old_depth >= _MAX_DEPTH:
            return

        # Doublement du répertoire si nécessaire
        if old_depth == self.global_depth:
            self.directory = self.directory * 2
            self.global_depth += 1

        new_depth = old_depth + 1
        b0 = _Bucket(new_depth, [], self.bucket_capacity)   # bit old_depth = 0
        b1 = _Bucket(new_depth, [], self.bucket_capacity)   # bit old_depth = 1

        # Redistribution selon le bit discriminant (bit à la position old_depth)
        for key, idx in bucket.entries:
            if (self._hash(key) >> old_depth) & 1:
                b1.entries.append((key, idx))
            else:
                b0.entries.append((key, idx))

        # Si la scission n'a rien redistribué (toutes les clés hachent
        # vers le même seau), on accepte le débordement plutôt que de
        # recurser indéfiniment.
        if len(b0.entries) == len(bucket.entries) or len(b1.entries) == len(bucket.entries):
            # Restaurer le seau original pour éviter de perdre des entrées
            bucket.local_depth = old_depth  # ne pas changer la profondeur
            for i in range(len(self.directory)):
                if self.directory[i] is b0 or self.directory[i] is b1:
                    self.directory[i] = bucket
            return

        # Mise à jour du répertoire
        for i in range(len(self.directory)):
            if self.directory[i] is bucket:
                self.directory[i] = b1 if (i >> old_depth) & 1 else b0

        # Récursion si un des nouveaux seaux déborde encore
        if len(b0.entries) > self.bucket_capacity:
            self._split(b0)
        if len(b1.entries) > self.bucket_capacity:
            self._split(b1)

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
