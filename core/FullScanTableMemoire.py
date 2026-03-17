from core.Instrumentation import Instrumentation
from core.Operateur import Operateur

class FullScanTableMemoire(Instrumentation, Operateur):
    
    def __init__(self, tbl):
        super().__init__("FullScan" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.contenu = tbl
        self.taille = len(self.contenu.valeurs)
        self.compteur = 0
        self.total = 0
        self.range = 5

    def open(self):
        self.start()
        self.compteur = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
        
    def next(self):
        self.start()
        if self.compteur < self.taille:
            t = self.contenu.valeurs[self.compteur]
            self.compteur += 1
            self.produit(t)
            self.stop()
            return t
        else:
            self.stop()
            return None

    def close(self):
        self.total += self.tuplesProduits
