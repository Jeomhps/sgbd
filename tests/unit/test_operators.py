"""
Unit tests for database operators using pytest.
These tests are designed for CI/CD and automated testing.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Project import Project
from operators.Restrict import Restrict
from operators.Join import Join
from operators.Aggregate import Aggregate


class TestProjectOperator:
    """Test Project operator functionality"""
    
    def test_project_basic(self):
        """Test basic projection"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [0, 2])  # Keep columns 0 and 2
        
        project.open()
        count = 0
        while project.next() is not None:
            count += 1
        project.close()
        
        assert count == 5  # Should have same number of tuples
        assert project.tuplesProduits == 5
    
    def test_project_empty(self):
        """Test projection on empty table"""
        table = TableMemoire(3)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [0, 1])
        
        project.open()
        result = project.next()
        project.close()
        
        assert result is None  # Should return None for empty table
        assert project.tuplesProduits == 0


class TestRestrictOperator:
    """Test Restrict operator functionality"""
    
    def test_restrict_with_matches(self):
        """Test restriction with matching tuples"""
        table = TableMemoire(2)
        tuple1 = Tuple(2)
        tuple1.val = [10, 100]
        tuple2 = Tuple(2)
        tuple2.val = [20, 200]
        tuple3 = Tuple(2)
        tuple3.val = [10, 300]  # This should match
        table.valeurs.extend([tuple1, tuple2, tuple3])
        
        scan = FullScanTableMemoire(table)
        restrict = Restrict(scan, 0, 10, "==")  # col0 == 10
        
        restrict.open()
        results = []
        while True:
            result = restrict.next()
            if result is None:
                break
            results.append(result)
        restrict.close()
        
        assert len(results) == 2  # Two tuples with col0 == 10
        assert restrict.tuplesProduits == 2
    
    def test_restrict_no_matches(self):
        """Test restriction with no matching tuples"""
        table = TableMemoire.randomize(2, 50, 3)
        scan = FullScanTableMemoire(table)
        restrict = Restrict(scan, 0, 100, ">")  # All values < 50, so no matches
        
        restrict.open()
        result = restrict.next()
        restrict.close()
        
        assert result is None
        assert restrict.tuplesProduits == 0


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
        left_table = TableMemoire.randomize(2, 50, 3)
        right_table = TableMemoire.randomize(2, 100, 3)  # Different range
        
        left_scan = FullScanTableMemoire(left_table)
        right_scan = FullScanTableMemoire(right_table)
        join = Join(left_scan, right_scan, 0, 0)
        
        join.open()
        result = join.next()
        join.close()
        
        assert result is None
        assert join.tuplesProduits == 0


class TestAggregateOperator:
    """Test Aggregate operator functionality"""
    
    def test_sum_aggregation(self):
        """Test SUM aggregation"""
        table = TableMemoire(1)
        t1 = Tuple(1)
        t1.val = [10]
        t2 = Tuple(1)
        t2.val = [20]
        t3 = Tuple(1)
        t3.val = [30]
        table.valeurs.extend([t1, t2, t3])
        
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'SUM')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 60  # 10 + 20 + 30
        assert agg.tuplesProduits == 1
    
    def test_avg_aggregation(self):
        """Test AVG aggregation"""
        table = TableMemoire(1)
        t1 = Tuple(1)
        t1.val = [10.0]
        t2 = Tuple(1)
        t2.val = [20.0]
        table.valeurs.extend([t1, t2])
        
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'AVG')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 15.0  # (10 + 20) / 2
    
    def test_count_aggregation(self):
        """Test COUNT aggregation"""
        table = TableMemoire.randomize(2, 100, 7)
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'COUNT')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 7
    
    def test_group_by_aggregation(self):
        """Test GROUP BY with aggregation"""
        table = TableMemoire(2)
        # Group 1
        t1 = Tuple(2)
        t1.val = [1, 10]
        t2 = Tuple(2)
        t2.val = [1, 20]
        # Group 2
        t3 = Tuple(2)
        t3.val = [2, 30]
        table.valeurs.extend([t1, t2, t3])
        
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 1, 'SUM', [0])  # SUM col1, GROUP BY col0
        
        agg.open()
        results = []
        while True:
            result = agg.next()
            if result is None:
                break
            results.append(result)
        agg.close()
        
        assert len(results) == 2
        # Group 1: sum = 10 + 20 = 30
        # Group 2: sum = 30
        sums = [r.val[1] for r in results]
        assert 30 in sums
        assert 30 in sums
    
    def test_empty_table_aggregation(self):
        """Test aggregation on empty table"""
        table = TableMemoire(1)
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'SUM')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 0  # Default for empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])