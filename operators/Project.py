from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Project(Instrumentation, Operateur):
    
    def __init__(self, _in, _cols):
        super().__init__("Project" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.cols = _cols

    def open(self):
        self.start()
        self.child.open()
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self):
        self.start()
        temp = self.child.next()
        if temp is None:
            self.stop()
            return None
        else:
            new_tuple = Tuple(len(self.cols))
            for i in range(len(self.cols)):
                new_tuple.val[i] = temp.val[self.cols[i]]
            self.produit(new_tuple)
            self.stop()
            return new_tuple

    def close(self):
        self.child.close()
