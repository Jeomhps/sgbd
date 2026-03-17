"""
Stockage sur disque des entrées d'index.

Un IndexDisque est un TableDisque spécialisé pour stocker des paires
(clé, indice_tuple), avec tuple_size=2.  Il hérite du mécanisme de
blocs + cache LRU de TableDisque et ajoute :

  • write_entries(entries)   — écrit une liste de (key, idx) sur disque
  • get_entry(pos)           — retourne (key, idx) à la position pos
  • scan_range(start, stop)  — itère les entrées [start, stop[

Format fichier
--------------
  Header  : struct.pack('II', nb_entries, 2)   — 8 octets
  Données : nb_entries × struct.pack('ii', key, idx)  — 8 octets / entrée
"""

from __future__ import annotations

import struct

from core.TableDisque import TableDisque
from core.Tuple import Tuple


class IndexDisque(TableDisque):
    """
    Stockage bloc + LRU pour entrées d'index (key, tuple_idx).

    Paramètres
    ----------
    file_path      : chemin du fichier d'index
    block_size     : entrées par bloc (défaut 16)
    memory_blocks  : taille du cache LRU en blocs (défaut 4)
    """

    ENTRY_SIZE = 2          # tuple_size fixé à 2 : (key, tuple_idx)

    def __init__(
        self,
        file_path:     str = "index.dat",
        block_size:    int = 16,
        memory_blocks: int = 4,
    ) -> None:
        super().__init__(file_path, block_size, memory_blocks)
        self.tuple_size = self.ENTRY_SIZE

    # ── écriture ───────────────────────────────────────────────────────────

    def write_entries(self, entries: list) -> None:
        """
        Écrit *entries* (liste de (key, idx)) sur disque.

        Les clés sont converties en int avant écriture.
        Après l'appel, le fichier est fermé ; appeler open() avant toute lecture.
        """
        n = len(entries)
        with open(self.file_path, "wb") as f:
            f.write(struct.pack("II", n, self.ENTRY_SIZE))
            for key, idx in entries:
                f.write(struct.pack("ii", int(key), int(idx)))
        self.table_size = n
        self.tuple_size = self.ENTRY_SIZE
        # Vider le cache (nouveau contenu)
        self.cache       = {}
        self.cache_order = []

    # ── lecture unitaire ───────────────────────────────────────────────────

    def get_entry(self, pos: int):
        """Retourne (key, tuple_idx) à la position *pos*, ou None."""
        t = self.get_tuple(pos)
        if t is None:
            return None
        return t.val[0], t.val[1]

    # ── parcours partiel ───────────────────────────────────────────────────

    def scan_range(self, start: int, stop: int):
        """Itère les entrées (key, idx) dans [start, stop[ ."""
        for i in range(start, min(stop, self.table_size)):
            e = self.get_entry(i)
            if e is not None:
                yield e
