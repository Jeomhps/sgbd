from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Restrict(Instrumentation, Operateur):
    """
    Selection (WHERE) : filtre les tuples selon une condition sur une colonne.

    Operateurs supportes : ==, !=, >, <, >=, <=
    """

    def __init__(self, _in, _col, _val, _op="=="):
        super().__init__("Restrict" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.col = _col
        self.val = _val
        self.op = _op

    def open(self):
        self.start()
        self.child.open()
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self):
        self.start()
        while True:
            t = self.child.next()
            if t is None:
                self.stop()
                return None
            if self._check(t):
                self.produit(t)
                self.stop()
                return t

    def _check(self, t):
        """Verifie si le tuple satisfait la condition."""
        v = t.val[self.col]
        if self.op == "==": return v == self.val
        if self.op == "!=": return v != self.val
        if self.op == ">":  return v > self.val
        if self.op == "<":  return v < self.val
        if self.op == ">=": return v >= self.val
        if self.op == "<=": return v <= self.val
        return False

    def close(self):
        self.child.close()
