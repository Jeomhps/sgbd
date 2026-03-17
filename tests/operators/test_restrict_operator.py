"""
Unit tests for Restrict operator using pytest.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Restrict import Restrict


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
    
    def test_restrict_greater_than(self):
        """Test restriction with greater than operator"""
        table = TableMemoire(2)
        tuple1 = Tuple(2)
        tuple1.val = [50, 100]
        tuple2 = Tuple(2)
        tuple2.val = [30, 200]
        tuple3 = Tuple(2)
        tuple3.val = [70, 300]
        table.valeurs.extend([tuple1, tuple2, tuple3])
        
        scan = FullScanTableMemoire(table)
        restrict = Restrict(scan, 0, 40, ">")  # col0 > 40
        
        restrict.open()
        results = []
        while True:
            result = restrict.next()
            if result is None:
                break
            results.append(result)
        restrict.close()
        
        assert len(results) == 2  # 50 and 70 are > 40
        assert restrict.tuplesProduits == 2
    
    def test_restrict_less_than(self):
        """Test restriction with less than operator"""
        table = TableMemoire(2)
        tuple1 = Tuple(2)
        tuple1.val = [10, 100]
        tuple2 = Tuple(2)
        tuple2.val = [60, 200]
        tuple3 = Tuple(2)
        tuple3.val = [30, 300]
        table.valeurs.extend([tuple1, tuple2, tuple3])
        
        scan = FullScanTableMemoire(table)
        restrict = Restrict(scan, 0, 40, "<")  # col0 < 40
        
        restrict.open()
        results = []
        while True:
            result = restrict.next()
            if result is None:
                break
            results.append(result)
        restrict.close()
        
        assert len(results) == 2  # 10 and 30 are < 40
        assert restrict.tuplesProduits == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])