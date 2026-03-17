"""
Unit tests for HashJoin operator using pytest.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.HashJoin import HashJoin


class TestHashJoinOperator:
    """Test HashJoin operator functionality"""
    
    def test_hashjoin_with_matches(self):
        """Test hash join with matching tuples"""
        # Left table (employees: id, dept_id)
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 10]  # id=1, dept_id=10
        t2 = Tuple(2)
        t2.val = [2, 20]  # id=2, dept_id=20
        t3 = Tuple(2)
        t3.val = [3, 10]  # id=3, dept_id=10 (duplicate dept)
        left_table.valeurs.extend([t1, t2, t3])
        
        # Right table (departments: dept_id, name)
        right_table = TableMemoire(2)
        t4 = Tuple(2)
        t4.val = [10, 101]  # dept_id=10, name=101
        t5 = Tuple(2)
        t5.val = [20, 201]  # dept_id=20, name=201
        t6 = Tuple(2)
        t6.val = [30, 301]  # dept_id=30, name=301 (no match)
        right_table.valeurs.extend([t4, t5, t6])
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        hashjoin = HashJoin(left_scan, right_scan, 1, 0)  # left.dept_id = right.dept_id
        
        hashjoin.open()
        results = []
        while True:
            result = hashjoin.next()
            if result is None:
                break
            results.append(result)
        hashjoin.close()
        
        # Should have 4 results:
        # Employee 1 joins with Dept 10
        # Employee 2 joins with Dept 20  
        # Employee 3 joins with Dept 10 (duplicate)
        assert len(results) == 3
        
        # Check that all results have correct structure (4 columns)
        for result in results:
            assert len(result.val) == 4  # left(2) + right(2)
        
        # Check specific joins
        dept_10_results = [r for r in results if r.val[3] == 101]  # name=101 means dept_id=10
        assert len(dept_10_results) == 2  # Two employees in dept 10
        
        dept_20_results = [r for r in results if r.val[3] == 201]  # name=201 means dept_id=20
        assert len(dept_20_results) == 1  # One employee in dept 20
        
        assert hashjoin.tuplesProduits == 3
    
    def test_hashjoin_no_matches(self):
        """Test hash join with no matching tuples"""
        # Left table
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        left_table.valeurs.append(t1)
        
        # Right table (different keys)
        right_table = TableMemoire(2)
        t2 = Tuple(2)
        t2.val = [2, 200]
        right_table.valeurs.append(t2)
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        hashjoin = HashJoin(left_scan, right_scan, 0, 0)  # Join on column 0
        
        hashjoin.open()
        result = hashjoin.next()
        hashjoin.close()
        
        assert result is None
        assert hashjoin.tuplesProduits == 0
    
    def test_hashjoin_empty_tables(self):
        """Test hash join with empty tables"""
        # Empty left table
        left_table = TableMemoire(2)
        right_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        right_table.valeurs.append(t1)
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        hashjoin = HashJoin(left_scan, right_scan, 0, 0)
        
        hashjoin.open()
        result = hashjoin.next()
        hashjoin.close()
        
        assert result is None
        assert hashjoin.tuplesProduits == 0
    
    def test_hashjoin_multiple_matches(self):
        """Test hash join with multiple matches per left tuple"""
        # Left table
        left_table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 10]  # One tuple
        left_table.valeurs.append(t1)
        
        # Right table (multiple tuples with same join key)
        right_table = TableMemoire(2)
        t2 = Tuple(2)
        t2.val = [10, 100]  # Match
        t3 = Tuple(2)
        t3.val = [10, 200]  # Match (same key)
        t4 = Tuple(2)
        t4.val = [10, 300]  # Match (same key)
        right_table.valeurs.extend([t2, t3, t4])
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        hashjoin = HashJoin(left_scan, right_scan, 1, 0)  # left.col1 = right.col0
        
        hashjoin.open()
        results = []
        while True:
            result = hashjoin.next()
            if result is None:
                break
            results.append(result)
        hashjoin.close()
        
        # Should have 3 results (one left tuple × three right tuples)
        assert len(results) == 3
        
        # All results should have the same left part
        for result in results:
            assert result.val[0] == 1  # Left tuple id
            assert result.val[1] == 10  # Left tuple dept_id
        
        # Right parts should be different
        right_values = [r.val[3] for r in results]  # Right tuple second column
        assert set(right_values) == {100, 200, 300}
        
        assert hashjoin.tuplesProduits == 3
    
    def test_hashjoin_performance_comparison(self):
        """Test that hash join builds hash table correctly"""
        # Create larger tables for testing
        left_table = TableMemoire(2)
        right_table = TableMemoire(2)
        
        # Add 100 tuples to each table with some overlaps
        for i in range(100):
            t_left = Tuple(2)
            t_left.val = [i, i % 10]  # 10 different join keys
            left_table.valeurs.append(t_left)
            
            t_right = Tuple(2)
            t_right.val = [i % 10, i * 100]  # Same 10 join keys
            right_table.valeurs.append(t_right)
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        hashjoin = HashJoin(left_scan, right_scan, 1, 0)  # Join on mod 10 values
        
        hashjoin.open()
        result_count = 0
        while hashjoin.next() is not None:
            result_count += 1
        hashjoin.close()
        
        # Each of the 100 left tuples should match 10 right tuples (on average)
        # Total results should be 100 * 10 = 1000
        assert result_count == 1000
        assert hashjoin.tuplesProduits == 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])