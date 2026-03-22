from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Join(Instrumentation, Operateur):
    """
    Nested loop join operator.
    
    Simple join algorithm that works by:
    1. Loading all tuples from the right table into memory
    2. For each tuple in the left table, scanning through all right tuples to find matches
    3. Returning concatenated tuples where the join condition is satisfied
    
    Args:
        _left: Left input operator (outer table in nested loop)
        _right: Right input operator (inner table in nested loop)
        _left_col: Column index from left table to join on
        _right_col: Column index from right table to join on
    """
    
    def __init__(self, _left, _right, _left_col, _right_col):
        super().__init__("Join" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col
        self.right_tuples = []  # Cache for right table tuples (inner table)
        self.right_index = 0     # Current position in right_tuples
        self.left_tuple = None   # Current left tuple being processed
    
    def open(self):
        """Initialize the join operation by loading right table into memory."""
        self.start()
        self.left.open()
        self.right.open()
        
        # Build phase: Load all right tuples into memory
        # This is the "inner" table that we'll scan for each left tuple
        self.right_tuples = []
        right_tuple = self.right.next()
        while right_tuple is not None:
            self.right_tuples.append(right_tuple)
            right_tuple = self.right.next()
        
        # Reset right operator for potential reuse
        self.right.close()
        self.right.open()
        
        # Initialize state for probe phase
        self.left_tuple = None
        self.right_index = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self):
        """
        Get next joined tuple using nested loop algorithm.
        
        For each left tuple, scan through all right tuples to find matches.
        When matches are found, concatenate the tuples.
        """
        self.start()
        
        # Get next left tuple if needed
        while self.left_tuple is None:
            self.left_tuple = self.left.next()
            if self.left_tuple is None:
                self.stop()
                return None
            self.right_index = 0  # Reset right index for new left tuple
        
        # Try to find matching right tuples
        while self.right_index < len(self.right_tuples):
            right_tuple = self.right_tuples[self.right_index]
            self.right_index += 1
            
            # Check join condition: left_tuple[left_col] == right_tuple[right_col]
            if self.left_tuple.val[self.left_col] == right_tuple.val[self.right_col]:
                # Create joined tuple (concatenate left + right)
                joined_size = len(self.left_tuple.val) + len(right_tuple.val)
                joined_tuple = Tuple(joined_size)
                
                # Copy left values
                for i in range(len(self.left_tuple.val)):
                    joined_tuple.val[i] = self.left_tuple.val[i]
                
                # Copy right values
                for i in range(len(right_tuple.val)):
                    joined_tuple.val[len(self.left_tuple.val) + i] = right_tuple.val[i]
                
                self.produit(joined_tuple)
                self.stop()
                return joined_tuple
        
        # No more matches for current left tuple, get next left tuple
        self.left_tuple = None
        return self.next()  # Recursive call to get next left tuple and try again
    
    def _create_joined_tuple(self, left_tuple, right_tuple):
        """Helper method to concatenate two tuples into one."""
        joined_size = len(left_tuple.val) + len(right_tuple.val)
        joined_tuple = Tuple(joined_size)
        
        # Copy left tuple values
        for i, value in enumerate(left_tuple.val):
            joined_tuple.val[i] = value
        
        # Copy right tuple values (after left values)
        for i, value in enumerate(right_tuple.val):
            joined_tuple.val[len(left_tuple.val) + i] = value
        
        return joined_tuple
    
    def close(self):
        """Clean up resources."""
        self.left.close()
        self.right.close()
        self.right_tuples = []  # Clear the cache
