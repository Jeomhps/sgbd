from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional, Union

class Aggregate(Instrumentation, Operateur):
    """
    Simple Aggregate operator supporting AVG, SUM, MIN, MAX, COUNT operations.
    
    Computes a single aggregation result over all input tuples.
    For example: SUM(salary), AVG(age), COUNT(*), etc.
    
    Args:
        _in: Input operator providing tuples to aggregate
        _agg_col: Column index to aggregate on
        _agg_func: Aggregation function ('AVG', 'SUM', 'MIN', 'MAX', 'COUNT')
    """
    
    def __init__(self, _in: Operateur, _agg_col: int, _agg_func: str):
        super().__init__("Aggregate" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.agg_col = _agg_col
        self.agg_func = _agg_func.upper()
        
        # Validate aggregation function
        valid_funcs = ['AVG', 'SUM', 'MIN', 'MAX', 'COUNT']
        if self.agg_func not in valid_funcs:
            raise ValueError(f"Invalid aggregation function: {self.agg_func}. Must be one of: {valid_funcs}")
        
        # Simple data structures for aggregation
        self.all_values = []  # Store all values to aggregate
        self.processed = False  # Track if we've processed all tuples
        self.result_returned = False  # Track if we've returned the result
    
    def open(self) -> None:
        """Initialize the aggregation operation."""
        self.start()
        self.child.open()
        self.all_values = []
        self.processed = False
        self.result_returned = False
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self) -> Optional[Tuple]:
        """
        Compute and return the aggregation result.
        
        Returns a single tuple with the aggregation result, or None if already returned.
        """
        self.start()
        
        # Phase 1: Collect all data if not already processed
        if not self.processed:
            self._collect_all_values()
            self.processed = True
        
        # Phase 2: Return the single aggregation result
        if self.result_returned:
            # Already returned the result, we're done
            self.stop()
            return None
        
        self.result_returned = True
        
        # Compute the aggregation based on the function
        result_value = self._compute_aggregation()
        
        # Create result tuple with single value
        result_tuple = Tuple(1)
        result_tuple.val[0] = result_value
        
        self.produit(result_tuple)
        self.stop()
        return result_tuple
    
    def _collect_all_values(self) -> None:
        """Collect all values from input tuples for aggregation."""
        while True:
            tuple_data = self.child.next()
            if tuple_data is None:
                break
            
            # Extract the value to aggregate from the specified column
            agg_value = tuple_data.val[self.agg_col]
            self.all_values.append(agg_value)
    
    def _compute_aggregation(self) -> Union[int, float]:
        """Compute the aggregation result based on the function."""
        if not self.all_values:
            return 0  # Default for empty input
        
        if self.agg_func == 'COUNT':
            return len(self.all_values)
        elif self.agg_func == 'SUM':
            return sum(self.all_values)
        elif self.agg_func == 'AVG':
            return sum(self.all_values) / len(self.all_values)
        elif self.agg_func == 'MIN':
            return min(self.all_values)
        elif self.agg_func == 'MAX':
            return max(self.all_values)
        else:
            return 0
    
    def close(self) -> None:
        """Clean up resources."""
        self.child.close()
        self.all_values = []
        self.processed = False
        self.result_returned = False