from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional, Dict, List

class HashJoin(Instrumentation, Operateur):
    """
    Simple Hash Join operator.
    
    More efficient than nested loop join for large datasets.
    Works in two phases:
    1. Build: Create hash table from right table (inner table)
    2. Probe: For each left tuple (outer table), find matches in hash table
    
    Args:
        left: Left input operator (outer table for probing)
        right: Right input operator (inner table for building hash)
        left_col: Column index from left table to join on
        right_col: Column index from right table to join on
    """
    
    def __init__(self, _left: Operateur, _right: Operateur, _left_col: int, _right_col: int):
        super().__init__("HashJoin" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.left = _left
        self.right = _right
        self.left_col = _left_col
        self.right_col = _right_col
        
        # Hash table: join_key -> list of matching right tuples
        self.hash_table: Dict[int, List[Tuple]] = {}
        self.current_left_tuple: Optional[Tuple] = None
        self.current_match_index: int = 0
        self.current_matches: List[Tuple] = []
    
    def open(self) -> None:
        self.start()
        self.left.open()
        self.right.open()
        
        # Build phase: Create hash table from right relation
        self._build_hash_table()
        
        self.current_left_tuple = None
        self.current_match_index = 0
        self.current_matches = []
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
        """
        Get next joined tuple using hash join algorithm.
        
        For each left tuple, use hash table to quickly find matching right tuples.
        """
        self.start()
        
        # Get next left tuple if we don't have one
        if self.current_left_tuple is None:
            self.current_left_tuple = self.left.next()
            if self.current_left_tuple is None:
                # No more left tuples, we're done
                self.stop()
                return None
            
            # Probe phase: Find matches in hash table for this left tuple
            join_key = self.current_left_tuple.val[self.left_col]
            self.current_matches = self.hash_table.get(join_key, [])
            self.current_match_index = 0
        
        # Return joined tuples for current left tuple
        if self.current_match_index < len(self.current_matches):
            right_tuple = self.current_matches[self.current_match_index]
            self.current_match_index += 1
            
            # Create joined tuple by concatenating left + right
            joined_tuple = self._create_joined_tuple(self.current_left_tuple, right_tuple)
            self.produit(joined_tuple)
            self.stop()
            return joined_tuple
        else:
            # No more matches for current left tuple, move to next left tuple
            self.current_left_tuple = None
            return self.next()
    
    def _create_joined_tuple(self, left_tuple: Tuple, right_tuple: Tuple) -> Tuple:
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
    
    def close(self) -> None:
        """Clean up resources."""
        self.left.close()
        self.right.close()
        self.hash_table = {}
