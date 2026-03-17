import random
from core.Tuple import Tuple

class TableMemoire:
    
    def __init__(self, nb_att):
        self.nb_att = nb_att
        self.valeurs = []
        
    @staticmethod
    def randomize(tuplesize, val_range, tablesize):
        contenu = TableMemoire(tuplesize)
        for i in range(tablesize):
            t = Tuple(tuplesize)
            for j in range(tuplesize):
                t.val[j] = random.randrange(val_range)
            contenu.valeurs.append(t)
        return contenu
