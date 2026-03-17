import time
class Instrumentation:

    number = 0

    def __init__(self, op_name=None):
        if op_name is None:
            self.opName = "operateur" + str(Instrumentation.number)
            Instrumentation.number += 1
        else:
            self.opName = op_name
            
        self.tuplesProduits = 0
        self.memoire = 0
        self.memoire = 0
        self.time = 0
        self.startTime = 0
        self.stopTime = 0
    
    def reset(self):
        self.tuplesProduits = 0
        self.memoire = 0
        self.time = 0
    

    def start(self):
        self.startTime = time.time()
    
    def stop(self):
        self.stopTime = time.time()
        self.time += self.stopTime - self.startTime

    def produit(self, tuple):
        self.tuplesProduits += 1
        self.memoire += tuple.size

    def __str__(self):
        return self.opName + " -- tuples produits: " + str(self.tuplesProduits) + " -- mémoire utilisée : " + str(self.memoire) + " -- Time: " + str(self.time)

    