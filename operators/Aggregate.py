from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional, List, Union
import math

class Aggregate(Instrumentation, Operateur):
    """
    Aggregate operator supporting AVG, SUM, MIN, MAX, COUNT operations
    
    Args:
        _in: Input operator
        _agg_col: Column index to aggregate on
        _agg_func: Aggregation function ('AVG', 'SUM', 'MIN', 'MAX', 'COUNT')
        _group_by_cols: Optional list of column indices to group by (None for no grouping)
    """
    
    def __init__(self, _in: Operateur, _agg_col: int, _agg_func: str, _group_by_cols: Optional[List[int]] = None):
        super().__init__("Aggregate" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.agg_col = _agg_col
        self.agg_func = _agg_func.upper()
        self.group_by_cols = _group_by_cols or []
        
        # Validate aggregation function
        valid_funcs = ['AVG', 'SUM', 'MIN', 'MAX', 'COUNT']
        if self.agg_func not in valid_funcs:
            raise ValueError(f"Invalid aggregation function: {self.agg_func}. Must be one of: {valid_funcs}")
        
        # Data structures for aggregation
        self.groups = {}  # key: group_values_tuple, value: aggregation data
        self.current_group_key = None
        self.current_group_values = []
        self.group_index = 0
        self.all_values = []  # For non-grouped aggregation
        self.processed = False
    
    def open(self) -> None:
        self.start()
        self.child.open()
        self.groups = {}
        self.current_group_key = None
        self.current_group_values = []
        self.group_index = 0
        self.all_values = []
        self.processed = False
        self.result_returned = False  # flag pour l'agrégation sans GROUP BY
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
    
    def next(self) -> Optional[Tuple]:
        self.start()
        
        # Phase 1: Collect all data and compute aggregations
        if not self.processed:
            self._process_all_tuples()
            self.processed = True
        
        # Phase 2: Return aggregation results
        if self.group_by_cols:
            # Grouped aggregation
            if self.group_index < len(self.groups):
                group_key = list(self.groups.keys())[self.group_index]
                values = self.groups[group_key]
                agg_result = self._compute_aggregation(values)
                self.group_index += 1
                
                # Create result tuple: group_by_cols + aggregation result
                result_size = len(self.group_by_cols) + 1
                result_tuple = Tuple(result_size)
                
                # Add group by values
                for i, col_idx in enumerate(self.group_by_cols):
                    result_tuple.val[i] = group_key[i]
                
                # Add aggregation result
                result_tuple.val[len(self.group_by_cols)] = agg_result
                
                self.produit(result_tuple)
                self.stop()
                return result_tuple
            else:
                self.stop()
                return None
        else:
            # Non-grouped aggregation (single result)
            if self.result_returned:
                self.stop()
                return None

            self.result_returned = True

            # Compute final aggregation
            if self.agg_func == 'COUNT':
                result_value = len(self.all_values)
            elif self.all_values:  # Handle empty case
                if self.agg_func == 'SUM':
                    result_value = sum(self.all_values)
                elif self.agg_func == 'AVG':
                    result_value = sum(self.all_values) / len(self.all_values)
                elif self.agg_func == 'MIN':
                    result_value = min(self.all_values)
                elif self.agg_func == 'MAX':
                    result_value = max(self.all_values)
            else:
                result_value = 0  # Default for empty input

            # Create result tuple with single value
            result_tuple = Tuple(1)
            result_tuple.val[0] = result_value

            self.produit(result_tuple)
            self.stop()
            return result_tuple
    
    def _process_all_tuples(self) -> None:
        """Process all input tuples and compute aggregations"""
        while True:
            tuple_data = self.child.next()
            if tuple_data is None:
                break
            
            # Extract value to aggregate
            agg_value = tuple_data.val[self.agg_col]
            
            if self.group_by_cols:
                # Grouped aggregation
                group_key = tuple(tuple_data.val[col] for col in self.group_by_cols)
                
                if group_key not in self.groups:
                    self.groups[group_key] = []
                
                self.groups[group_key].append(agg_value)
            else:
                # Non-grouped aggregation
                self.all_values.append(agg_value)
    
    def _compute_aggregation(self, values: List[Union[int, float]]) -> Union[int, float]:
        """Compute aggregation for a list of values"""
        if not values:
            return 0
        
        if self.agg_func == 'COUNT':
            return len(values)
        elif self.agg_func == 'SUM':
            return sum(values)
        elif self.agg_func == 'AVG':
            return sum(values) / len(values)
        elif self.agg_func == 'MIN':
            return min(values)
        elif self.agg_func == 'MAX':
            return max(values)
        else:
            return 0
    
    def close(self) -> None:
        self.child.close()
        self.groups = {}
        self.all_values = []
        self.processed = False