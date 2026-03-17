import random
from core.Tuple import Tuple

class TableMemoire:

    # Pour TableMemoire, chaque tuple est son propre "bloc" (block_size=1).
    block_size = 1

    def __init__(self, nb_att):
        self.nb_att = nb_att
        self.valeurs = []

    def get_block(self, block_no: int):
        """Retourne la liste des tuples du bloc *block_no* (ici, 1 tuple par bloc)."""
        if 0 <= block_no < len(self.valeurs):
            return [self.valeurs[block_no]]
        return []
        
    @staticmethod
    def randomize(tuplesize, val_range, tablesize):
        contenu = TableMemoire(tuplesize)
        for i in range(tablesize):
            t = Tuple(tuplesize)
            for j in range(tuplesize):
                t.val[j] = random.randrange(val_range)
            contenu.valeurs.append(t)
        return contenu
