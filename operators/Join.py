from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Join(Instrumentation, Operateur):
    """
    Jointure par boucle imbriquee (Nested Loop Join).

    1. Charge tous les tuples de la table droite en memoire
    2. Pour chaque tuple gauche, parcourt tous les tuples droits
    3. Concatene les tuples quand la condition est satisfaite

    Complexite : O(n x m)
    """

    def __init__(self, _left, _right, _left_col, _right_col):
        super().__init__("Join" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col
        self.right_tuples = []
        self.right_index = 0
        self.left_tuple = None

    def open(self):
        self.start()
        self.left.open()
        self.right.open()

        # Charger tous les tuples droits en memoire
        self.right_tuples = []
        right_tuple = self.right.next()
        while right_tuple is not None:
            self.right_tuples.append(right_tuple)
            right_tuple = self.right.next()

        self.right.close()
        self.right.open()

        self.left_tuple = None
        self.right_index = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self):
        self.start()

        # Obtenir le prochain tuple gauche si necessaire
        while self.left_tuple is None:
            self.left_tuple = self.left.next()
            if self.left_tuple is None:
                self.stop()
                return None
            self.right_index = 0

        # Chercher une correspondance dans les tuples droits
        while self.right_index < len(self.right_tuples):
            right_tuple = self.right_tuples[self.right_index]
            self.right_index += 1

            if self.left_tuple.val[self.left_col] == right_tuple.val[self.right_col]:
                joined = _concat(self.left_tuple, right_tuple)
                self.produit(joined)
                self.stop()
                return joined

        # Plus de correspondances, passer au tuple gauche suivant
        self.left_tuple = None
        return self.next()

    def close(self):
        self.left.close()
        self.right.close()
        self.right_tuples = []


def _concat(left, right):
    """Concatene deux tuples en un seul."""
    combined = Tuple(len(left.val) + len(right.val))
    combined.val = left.val + right.val
    return combined
