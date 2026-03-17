from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional, Dict, List

class HashJoin(Instrumentation, Operateur):
    """
    Hash Join operator - more efficient than nested loop join for large datasets.
    
    Uses a hash table to build an index on the right table, then probes with left tuples.
    
    Args:
        _left: Left input operator
        _right: Right input operator
        _left_col: Column index from left table to join on
        _right_col: Column index from right table to join on
    """
    
    def __init__(self, _left: Operateur, _right: Operateur, _left_col: int, _right_col: int):
        super().__init__("HashJoin" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col
        
        # Hash table: key -> list of right tuples
        self.hash_table: Dict[int, List[Tuple]] = {}
        self.left_tuple: Optional[Tuple] = None
        self.match_index: int = 0
        self.matches: List[Tuple] = []
        self.built_hash_table: bool = False
    
    def open(self) -> None:
        self.start()
        self.left.open()
        self.right.open()
        
        # Build phase: Create hash table from right relation
        self._build_hash_table()
        
        self.left_tuple = None
        self.match_index = 0
        self.matches = []
        self.built_hash_table = True
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def _build_hash_table(self) -> None:
        """Build hash table from right relation"""
        self.hash_table = {}
        
        # Read all tuples from right operator
        while True:
            right_tuple = self.right.next()
            if right_tuple is None:
                break
            
            # Use the join column value as hash key
            join_key = right_tuple.val[self.right_col]
            
            if join_key not in self.hash_table:
                self.hash_table[join_key] = []
            
            self.hash_table[join_key].append(right_tuple)
        
        # Reset right operator for potential reuse
        self.right.close()
        self.right.open()
    
    def next(self) -> Optional[Tuple]:
        self.start()
        
        # Get next left tuple if needed
        while self.left_tuple is None:
            self.left_tuple = self.left.next()
            if self.left_tuple is None:
                self.stop()
                return None
            
            # Probe phase: Find matches in hash table
            join_key = self.left_tuple.val[self.left_col]
            self.matches = self.hash_table.get(join_key, [])
            self.match_index = 0
        
        # Return joined tuples
        if self.match_index < len(self.matches):
            right_tuple = self.matches[self.match_index]
            self.match_index += 1
            
            # Create joined tuple (left + right)
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
        else:
            # No more matches for current left tuple, get next left tuple
            self.left_tuple = None
            return self.next()  # Recursive call
    
    def close(self) -> None:
        self.left.close()
        self.right.close()
        self.hash_table = {}
        self.built_hash_table = False
