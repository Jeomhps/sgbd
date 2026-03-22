from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Project(Instrumentation, Operateur):
    """
    Simple Project operator.
    
    Selects specific columns from input tuples, creating new tuples with only the requested columns.
    This is equivalent to SQL's SELECT col1, col2, ... FROM table.
    
    Args:
        _in: Input operator providing tuples
        _cols: List of column indices to project (keep)
    """
    
    def __init__(self, _in, _cols):
        super().__init__("Project" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.cols = _cols  # List of column indices to project

    def open(self):
        """Initialize the projection operation."""
        self.start()
        self.child.open()
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self):
        """
        Get next projected tuple.
        
        For each input tuple, creates a new tuple containing only the specified columns.
        """
        self.start()
        
        # Get next tuple from child operator
        source_tuple = self.child.next()
        if source_tuple is None:
            # No more tuples
            self.stop()
            return None
        
        # Create new tuple with only the projected columns
        projected_tuple = Tuple(len(self.cols))
        for i, col_index in enumerate(self.cols):
            projected_tuple.val[i] = source_tuple.val[col_index]
        
        self.produit(projected_tuple)
        self.stop()
        return projected_tuple

    def close(self):
        """Clean up resources."""
        self.child.close()
