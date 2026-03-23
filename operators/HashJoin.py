from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional

class HashJoin(Instrumentation, Operateur):
    """
    Jointure par hachage (Hash Join).

    Phase 1 (Build)  : construire une table de hachage sur la table droite
    Phase 2 (Probe)  : pour chaque tuple gauche, lookup O(1) dans la hash table

    Complexite : O(n + m)
    """

    def __init__(self, _left, _right, _left_col, _right_col):
        super().__init__("HashJoin" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col

        self.hash_table = {}          # cle_jointure -> [tuples droits]
        self.current_left = None
        self.current_matches = []
        self.match_index = 0

    def open(self):
        self.start()
        self.left.open()
        self.right.open()

        # Phase Build : table de hachage sur la table droite
        self.hash_table = {}
        while True:
            t = self.right.next()
            if t is None:
                break
            key = t.val[self.right_col]
            if key not in self.hash_table:
                self.hash_table[key] = []
            self.hash_table[key].append(t)

        self.right.close()
        self.right.open()

        self.current_left = None
        self.current_matches = []
        self.match_index = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        # Obtenir le prochain tuple gauche si necessaire
        if self.current_left is None:
            self.current_left = self.left.next()
            if self.current_left is None:
                self.stop()
                return None
            # Phase Probe : lookup dans la hash table
            key = self.current_left.val[self.left_col]
            self.current_matches = self.hash_table.get(key, [])
            self.match_index = 0

        # Emettre les correspondances
        if self.match_index < len(self.current_matches):
            right = self.current_matches[self.match_index]
            self.match_index += 1
            joined = _concat(self.current_left, right)
            self.produit(joined)
            self.stop()
            return joined

        # Plus de correspondances, passer au tuple gauche suivant
        self.current_left = None
        return self.next()

    def close(self):
        self.left.close()
        self.right.close()
        self.hash_table = {}


def _concat(left, right):
    """Concatene deux tuples en un seul."""
    combined = Tuple(len(left.val) + len(right.val))
    combined.val = left.val + right.val
    return combined
