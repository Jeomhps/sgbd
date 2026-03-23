from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Project(Instrumentation, Operateur):
    """
    Projection (SELECT col1, col2) : ne garde que les colonnes specifiees.
    """

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
        source = self.child.next()
        if source is None:
            self.stop()
            return None

        projected = Tuple(len(self.cols))
        for i, col_index in enumerate(self.cols):
            projected.val[i] = source.val[col_index]

        self.produit(projected)
        self.stop()
        return projected

    def close(self):
        self.child.close()
