from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Restrict(Instrumentation, Operateur):
    """
    Simple Restrict (Filter) operator.
    
    Filters tuples based on a condition, only returning tuples that satisfy the condition.
    This is equivalent to SQL's WHERE clause.
    
    Args:
        _in: Input operator providing tuples
        _col: Column index to compare
        _val: Value to compare against
        _op: Comparison operator ("==", "!=", ">", "<", ">=", "<=")
    """
    
    def __init__(self, _in, _col, _val, _op="=="):
        super().__init__("Restrict" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.col = _col      # Column index to compare
        self.val = _val      # Value to compare against
        self.op = _op        # Comparison operator

    def open(self):
        """Initialize the restriction operation."""
        self.start()
        self.child.open()
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self):
        """
        Get next tuple that satisfies the restriction condition.
        
        Skips tuples that don't meet the condition, returning only matching tuples.
        """
        self.start()
        
        while True:
            tuple_data = self.child.next()
            if tuple_data is None:
                # No more tuples
                self.stop()
                return None
            
            # Check if tuple satisfies the condition
            if self._satisfies_condition(tuple_data):
                self.produit(tuple_data)
                self.stop()
                return tuple_data
            
            # Condition not met, continue to next tuple
    
    def _satisfies_condition(self, tuple_data: Tuple) -> bool:
        """Check if a tuple satisfies the restriction condition."""
        column_value = tuple_data.val[self.col]
        
        if self.op == "==":
            return column_value == self.val
        elif self.op == "!=":
            return column_value != self.val
        elif self.op == ">":
            return column_value > self.val
        elif self.op == "<":
            return column_value < self.val
        elif self.op == ">=":
            return column_value >= self.val
        elif self.op == "<=":
            return column_value <= self.val
        else:
            return False  # Unknown operator
    
    def close(self):
        """Clean up resources."""
        self.child.close()
