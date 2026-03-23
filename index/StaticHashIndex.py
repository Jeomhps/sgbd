"""
Index par hachage statique — stockage sur disque (IndexDisque).

Structure
---------
• nb_buckets seaux fixes, non redimensionnables.
• Fonction de hachage : hash(key) % nb_buckets
• Les paires (clé, indice_tuple) sont stockées dans un IndexDisque :
    entrées du seau 0 | entrées du seau 1 | … | entrées du seau N-1
• Le répertoire (start, count) par seau est sérialisé dans un fichier
  compagnon .dir.

Format du fichier .dir
-----------------------
  nb_buckets : 4B (unsigned int)
  Pour chaque seau i :
    start : 4B   — offset dans le fichier de données
    count : 4B   — nombre d'entrées

Complexité
----------
• Construction : O(n)
• Recherche    : O(1) en moyenne (1 lecture de bloc en cache)
"""

from __future__ import annotations

import os
import struct
import tempfile
from typing import List

from core.IndexDisque import IndexDisque


_DEFAULT_DIR = tempfile.gettempdir()


class StaticHashIndex:
    """
    Index par hachage statique à nb_buckets seaux — données sur disque.

    Paramètres
    ----------
    nb_buckets : int
        Nombre de seaux. Choisir proche du nombre de valeurs distinctes.
    file_path  : str | None
        Chemin du fichier de données (.dat). Si None, fichier temporaire.
    """

    def __init__(
        self,
        nb_buckets: int = 10,
        file_path: str | None = None,
    ) -> None:
        self.nb_buckets = nb_buckets

        if file_path is None:
            file_path = os.path.join(_DEFAULT_DIR, "static_hash_index.dat")
        self._data_path = file_path
        self._dir_path  = file_path + ".dir"

        self._storage = IndexDisque(self._data_path)
        self._col: int = -1
        self._size: int = 0

        # Répertoire en mémoire après build/chargement : list[(start, count)]
        self._directory: list[tuple[int, int]] = []

    # ── construction ───────────────────────────────────────────────────────

    def build(self, table, col: int) -> None:
        """Parcourt *table*, indexe la colonne *col*, écrit sur disque."""
        self._col  = col
        self._size = 0
        block_size = self._get_block_size(table)

        # 1. Accumuler les entrées dans des seaux mémoire temporaires
        # Chaque entrée stocke (clé, n° de bloc) au lieu de (clé, indice tuple).
        buckets: list[list[tuple]] = [[] for _ in range(self.nb_buckets)]
        for idx, t in enumerate(self._iter_table(table)):
            key      = t.val[col]
            b        = self._bucket_id(key)
            block_no = idx // block_size
            buckets[b].append((key, block_no))
            self._size += 1

        # 2. Construire le répertoire et la liste plate des entrées
        directory: list[tuple[int, int]] = []
        flat: list[tuple] = []
        for bucket in buckets:
            start = len(flat)
            flat.extend(bucket)
            directory.append((start, len(bucket)))

        # 3. Écrire les entrées sur disque via IndexDisque
        self._storage.write_entries(flat)

        # 4. Sauvegarder le répertoire dans le fichier .dir
        self._directory = directory
        self._save_dir()

        # index construit

    # ── recherche ──────────────────────────────────────────────────────────

    def search(self, value) -> List[int]:
        """
        Retourne les numéros de blocs contenant des tuples dont la clé vaut *value*.

        Les doublons sont supprimés (plusieurs tuples dans le même bloc → un seul
        numéro de bloc retourné) tout en préservant l'ordre de découverte.
        """
        self._ensure_dir_loaded()
        b = self._bucket_id(value)
        start, count = self._directory[b]

        self._storage.open()
        seen: dict[int, None] = {}
        for key, block_no in self._storage.scan_range(start, start + count):
            if key == value and block_no not in seen:
                seen[block_no] = None
        self._storage.close()
        return list(seen)

    # ── statistiques ───────────────────────────────────────────────────────

    def stats(self) -> str:
        self._ensure_dir_loaded()
        sizes     = [cnt for _, cnt in self._directory]
        non_empty = sum(1 for s in sizes if s > 0)
        avg       = self._size / self.nb_buckets if self.nb_buckets else 0
        mx        = max(sizes) if sizes else 0
        return (
            f"StaticHashIndex | seaux={self.nb_buckets} "
            f"non-vides={non_empty} | "
            f"avg/seau={avg:.1f} max/seau={mx} "
            f"entrées={self._size}"
        )

    # ── helpers internes ───────────────────────────────────────────────────

    def _bucket_id(self, key) -> int:
        return hash(key) % self.nb_buckets

    def _save_dir(self) -> None:
        with open(self._dir_path, "wb") as f:
            f.write(struct.pack("I", self.nb_buckets))
            for start, count in self._directory:
                f.write(struct.pack("II", start, count))

    def _load_dir(self) -> None:
        with open(self._dir_path, "rb") as f:
            (nb,) = struct.unpack("I", f.read(4))
            self.nb_buckets = nb
            self._directory = []
            for _ in range(nb):
                start, count = struct.unpack("II", f.read(8))
                self._directory.append((start, count))
        # Reconstruire _size
        self._storage.open()
        self._size = self._storage.table_size
        self._storage.close()

    def _ensure_dir_loaded(self) -> None:
        if not self._directory:
            if os.path.exists(self._dir_path):
                self._load_dir()
            else:
                raise RuntimeError(
                    "StaticHashIndex : index non construit (appeler build() d'abord)."
                )

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
