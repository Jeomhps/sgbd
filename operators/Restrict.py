from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Restrict(Instrumentation, Operateur):
    
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
            temp = self.child.next()
            if temp is None:
                self.stop()
                return None
            
            # Apply the restriction condition
            condition_met = False
            if self.op == "==":
                condition_met = (temp.val[self.col] == self.val)
            elif self.op == "!=":
                condition_met = (temp.val[self.col] != self.val)
            elif self.op == ">":
                condition_met = (temp.val[self.col] > self.val)
            elif self.op == "<":
                condition_met = (temp.val[self.col] < self.val)
            elif self.op == ">=":
                condition_met = (temp.val[self.col] >= self.val)
            elif self.op == "<=":
                condition_met = (temp.val[self.col] <= self.val)
            
            if condition_met:
                self.produit(temp)
                self.stop()
                return temp
            # If condition not met, continue to next tuple
    
    def close(self):
        self.child.close()
