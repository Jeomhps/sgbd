from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple

class Join(Instrumentation, Operateur):
    
    def __init__(self, _left, _right, _left_col, _right_col):
        """
        Join operator using nested loop join algorithm
        
        Args:
            _left: Left input operator
            _right: Right input operator  
            _left_col: Column index from left table to join on
            _right_col: Column index from right table to join on
        """
        super().__init__("Join" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col
        self.right_tuples = []  # Cache for right table tuples
        self.right_index = 0
        self.left_tuple = None
    
    def open(self):
        self.start()
        self.left.open()
        self.right.open()
        
        # Cache all right tuples in memory (for nested loop join)
        self.right_tuples = []
        while True:
            right_tuple = self.right.next()
            if right_tuple is None:
                break
            self.right_tuples.append(right_tuple)
        
        # Reset right operator for potential reuse
        self.right.close()
        self.right.open()
        
        self.right_index = 0
        self.left_tuple = None
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self):
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
    
    def close(self):
        self.left.close()
        self.right.close()
        self.right_tuples = []  # Clear cache