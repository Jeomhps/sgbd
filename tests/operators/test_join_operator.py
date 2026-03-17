"""
Unit tests for Join operator using pytest.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Join import Join


class TestJoinOperator:
    """Test Join operator functionality"""
    
    def test_join_with_matches(self):
        """Test join with matching tuples"""
        # Left table
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        t2 = Tuple(2)
        t2.val = [2, 200]
        left_table.valeurs.extend([t1, t2])
        
        # Right table
        right_table = TableMemoire(2)
        t3 = Tuple(2)
        t3.val = [1, 1000]  # Matches left tuple 1
        t4 = Tuple(2)
        t4.val = [3, 3000]  # No match
        right_table.valeurs.extend([t3, t4])
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        join = Join(left_scan, right_scan, 0, 0)  # Join on column 0
        
        join.open()
        results = []
        while True:
            result = join.next()
            if result is None:
                break
            results.append(result)
        join.close()
        
        assert len(results) == 1  # Only one match
        assert results[0].val == [1, 100, 1, 1000]  # Joined tuple
        assert join.tuplesProduits == 1
    
    def test_join_no_matches(self):
        """Test join with no matching tuples"""
        # Create tables with manually set values to ensure no matches
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        t2 = Tuple(2)
        t2.val = [2, 200]
        t3 = Tuple(2)
        t3.val = [3, 300]
        left_table.valeurs.extend([t1, t2, t3])
        
        right_table = TableMemoire(2)
        t4 = Tuple(2)
        t4.val = [10, 1000]  # Different keys - no matches
        t5 = Tuple(2)
        t5.val = [11, 2000]
        t6 = Tuple(2)
        t6.val = [12, 3000]
        right_table.valeurs.extend([t4, t5, t6])
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        join = Join(left_scan, right_scan, 0, 0)
        
        join.open()
        result = join.next()
        join.close()
        
        assert result is None
        assert join.tuplesProduits == 0
    
    def test_join_multiple_matches(self):
        """Test join with multiple matching tuples"""
        # Left table
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        t2 = Tuple(2)
        t2.val = [1, 200]  # Same key as t1
        left_table.valeurs.extend([t1, t2])
        
        # Right table
        right_table = TableMemoire(2)
        t3 = Tuple(2)
        t3.val = [1, 1000]  # Matches both left tuples
        t4 = Tuple(2)
        t4.val = [1, 2000]  # Also matches both left tuples
        right_table.valeurs.extend([t3, t4])
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        join = Join(left_scan, right_scan, 0, 0)  # Join on column 0
        
        join.open()
        results = []
        while True:
            result = join.next()
            if result is None:
                break
            results.append(result)
        join.close()
        
        assert len(results) == 4  # 2 left × 2 right = 4 combinations
        assert join.tuplesProduits == 4
    
    def test_join_empty_left_table(self):
        """Test join with empty left table"""
        left_table = TableMemoire(2)  # Empty
        right_table = TableMemoire.randomize(2, 50, 3)
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        join = Join(left_scan, right_scan, 0, 0)
        
        join.open()
        result = join.next()
        join.close()
        
        assert result is None
        assert join.tuplesProduits == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])