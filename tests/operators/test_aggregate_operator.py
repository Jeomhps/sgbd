"""
Unit tests for Aggregate operator using pytest.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Aggregate import Aggregate


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
    
    def test_min_aggregation(self):
        """Test MIN aggregation"""
        table = TableMemoire(1)
        t1 = Tuple(1)
        t1.val = [50]
        t2 = Tuple(1)
        t2.val = [20]
        t3 = Tuple(1)
        t3.val = [80]
        table.valeurs.extend([t1, t2, t3])
        
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'MIN')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 20  # Minimum value
    
    def test_max_aggregation(self):
        """Test MAX aggregation"""
        table = TableMemoire(1)
        t1 = Tuple(1)
        t1.val = [50]
        t2 = Tuple(1)
        t2.val = [20]
        t3 = Tuple(1)
        t3.val = [80]
        table.valeurs.extend([t1, t2, t3])
        
        scan = FullScanTableMemoire(table)
        agg = Aggregate(scan, 0, 'MAX')
        
        agg.open()
        result = agg.next()
        agg.close()
        
        assert result is not None
        assert result.val[0] == 80  # Maximum value


if __name__ == "__main__":
    pytest.main([__file__, "-v"])