"""
Index par hachage dynamique (hachage extensible — Fagin et al. 1979).

Principe
--------
Un répertoire de 2^d pointeurs (d = profondeur globale) indexe des seaux.
Chaque seau a une profondeur locale ℓ ≤ d.

Quand un seau sature :
  • Si ℓ < d  → on éclate juste ce seau (pas de doublement du répertoire).
  • Si ℓ = d  → on double le répertoire (d ← d+1) puis on éclate.

Stockage sur disque
-------------------
Après construction en mémoire, les entrées de chaque seau distinct sont
sérialisées dans un IndexDisque :

    entrées seau 0 | entrées seau 1 | …

Le répertoire (global_depth, pour chaque entrée dir → start/count) est
sauvegardé dans un fichier .dir compagnon.

Format fichier .dir
--------------------
  global_depth : 4B
  dir_size     : 4B   (= 2^global_depth)
  Pour chaque entrée du répertoire :
    bucket_start : 4B
    bucket_count : 4B

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

import os
import struct
import tempfile
from dataclasses import dataclass, field
from typing import List

from core.IndexDisque import IndexDisque

_MAX_DEPTH   = 30
_DEFAULT_DIR = tempfile.gettempdir()


# ── seau mémoire (utilisé pendant la construction) ────────────────────────

@dataclass
class _Bucket:
    local_depth: int
    entries: list = field(default_factory=list)   # [(key, tuple_idx)]
    capacity: int = 4


# ── index ─────────────────────────────────────────────────────────────────

class DynamicHashIndex:
    """
    Hachage extensible (extendible hashing) — données stockées sur disque.

    Paramètres
    ----------
    bucket_capacity : int
        Nombre maximum d'entrées par seau avant éclatement.
    file_path : str | None
        Chemin du fichier de données (.dat). Si None, fichier temporaire.
    """

    def __init__(
        self,
        bucket_capacity: int = 4,
        file_path: str | None = None,
    ) -> None:
        self.bucket_capacity = bucket_capacity

        if file_path is None:
            file_path = os.path.join(_DEFAULT_DIR, "dynamic_hash_index.dat")
        self._data_path = file_path
        self._dir_path  = file_path + ".dir"

        self._storage = IndexDisque(self._data_path)
        self._col: int  = -1
        self._size: int = 0

        # Répertoire en mémoire : list[(start, count)]  (indexé par dir_index)
        self.global_depth: int = 1
        self._dir_map: list[tuple[int, int]] = []

        # Structure mémoire temporaire pour la construction
        self._mem_directory: list[_Bucket] = []

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """Parcourt *table*, indexe la colonne *col*, écrit sur disque."""
        self._col = col

        # 1. Construction en mémoire (hachage extensible classique)
        self.global_depth = 1
        b0 = _Bucket(local_depth=1, capacity=self.bucket_capacity)
        b1 = _Bucket(local_depth=1, capacity=self.bucket_capacity)
        self._mem_directory = [b0, b1]
        self._size = 0

        for idx, t in enumerate(self._iter_table(table)):
            self._mem_insert(t.val[col], idx)

        # 2. Sérialiser sur disque
        self._flush_to_disk()

        print(
            f"[DynamicHash] index construit : {self._size} entrées | "
            f"profondeur globale={self.global_depth} "
            f"répertoire={len(self._mem_directory)} entrées (col={col}) "
            f"→ {self._data_path}"
        )

    # ── recherche ──────────────────────────────────────────────────────────

    def search(self, value) -> List[int]:
        """Retourne les indices des tuples dont la clé vaut *value*."""
        self._ensure_loaded()
        di              = self._dir_index(value)
        start, count    = self._dir_map[di]

        self._storage.open()
        results = [
            idx
            for key, idx in self._storage.scan_range(start, start + count)
            if key == value
        ]
        self._storage.close()
        return results

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        self._ensure_loaded()
        seen   = set()
        sizes  = []
        for start, count in self._dir_map:
            key = (start, count)
            if key not in seen:
                seen.add(key)
                sizes.append(count)
        avg = sum(sizes) / len(sizes) if sizes else 0
        return (
            f"DynamicHashIndex | profondeur globale={self.global_depth} "
            f"répertoire={len(self._dir_map)} "
            f"seaux distincts={len(sizes)} "
            f"avg/seau={avg:.1f} max/seau={max(sizes, default=0)} "
            f"entrées={self._size}"
        )

    # ── helpers construction mémoire ───────────────────────────────────────

    def _mem_insert(self, key, tuple_idx: int) -> None:
        di     = self._dir_index_mem(key)
        bucket = self._mem_directory[di]
        bucket.entries.append((key, tuple_idx))
        self._size += 1
        if len(bucket.entries) > bucket.capacity:
            self._split(bucket)

    def _dir_index_mem(self, key) -> int:
        return self._hash(key) & ((1 << self.global_depth) - 1)

    def _split(self, bucket: _Bucket) -> None:
        old_depth = bucket.local_depth
        if old_depth >= _MAX_DEPTH:
            return

        if old_depth == self.global_depth:
            self._mem_directory = self._mem_directory * 2
            self.global_depth += 1

        new_depth = old_depth + 1
        b0 = _Bucket(new_depth, [], self.bucket_capacity)
        b1 = _Bucket(new_depth, [], self.bucket_capacity)

        for key, idx in bucket.entries:
            if (self._hash(key) >> old_depth) & 1:
                b1.entries.append((key, idx))
            else:
                b0.entries.append((key, idx))

        # Garde-fou : scission non-redistributive
        if (
            len(b0.entries) == len(bucket.entries)
            or len(b1.entries) == len(bucket.entries)
        ):
            bucket.local_depth = old_depth
            for i in range(len(self._mem_directory)):
                if self._mem_directory[i] is b0 or self._mem_directory[i] is b1:
                    self._mem_directory[i] = bucket
            return

        for i in range(len(self._mem_directory)):
            if self._mem_directory[i] is bucket:
                self._mem_directory[i] = b1 if (i >> old_depth) & 1 else b0

        if len(b0.entries) > self.bucket_capacity:
            self._split(b0)
        if len(b1.entries) > self.bucket_capacity:
            self._split(b1)

    # ── sérialisation / désérialisation ───────────────────────────────────

    def _flush_to_disk(self) -> None:
        """Écrit les seaux sur disque et sauvegarde le répertoire."""
        # Dédupliquer les seaux (plusieurs entrées dir → même seau)
        bucket_id: dict[int, int] = {}   # id(bucket) → index dans flat
        flat: list[tuple] = []
        bucket_ranges: dict[int, tuple[int, int]] = {}  # id(b) → (start, count)

        for bucket in self._mem_directory:
            bid = id(bucket)
            if bid not in bucket_ranges:
                start = len(flat)
                flat.extend(bucket.entries)
                bucket_ranges[bid] = (start, len(bucket.entries))

        # Écrire les entrées
        self._storage.write_entries(flat)

        # Construire le dir_map
        self._dir_map = [
            bucket_ranges[id(bucket)]
            for bucket in self._mem_directory
        ]

        # Sauvegarder le répertoire
        self._save_dir()

    def _save_dir(self) -> None:
        with open(self._dir_path, "wb") as f:
            f.write(struct.pack("II", self.global_depth, len(self._dir_map)))
            for start, count in self._dir_map:
                f.write(struct.pack("II", start, count))

    def _load_dir(self) -> None:
        with open(self._dir_path, "rb") as f:
            self.global_depth, dir_size = struct.unpack("II", f.read(8))
            self._dir_map = []
            for _ in range(dir_size):
                start, count = struct.unpack("II", f.read(8))
                self._dir_map.append((start, count))
        self._storage.open()
        self._size = self._storage.table_size
        self._storage.close()

    def _ensure_loaded(self) -> None:
        if not self._dir_map:
            if os.path.exists(self._dir_path):
                self._load_dir()
            else:
                raise RuntimeError(
                    "DynamicHashIndex : index non construit (appeler build() d'abord)."
                )

    # ── helpers hachage ────────────────────────────────────────────────────

    def _hash(self, key) -> int:
        return hash(key) & 0x7FFF_FFFF

    def _dir_index(self, key) -> int:
        return self._hash(key) & ((1 << self.global_depth) - 1)

    # ── itération table ────────────────────────────────────────────────────

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
