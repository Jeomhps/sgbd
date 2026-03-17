"""
Unit tests for FullScanTableMemoire class.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple


class TestFullScanTableMemoire:
    """Test FullScanTableMemoire functionality"""
    
    def test_scan_creation(self):
        """Test scan creation"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        
        assert scan.contenu == table
        assert scan.taille == 5
        assert scan.compteur == 0
    
    def test_scan_open_close(self):
        """Test scan open/close cycle"""
        table = TableMemoire.randomize(2, 50, 3)
        scan = FullScanTableMemoire(table)
        
        scan.open()
        assert scan.compteur == 0
        assert scan.tuplesProduits == 0
        
        scan.close()
        assert scan.total == 0  # No tuples produced yet
    
    def test_scan_all_tuples(self):
        """Test scanning all tuples"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        
        scan.open()
        count = 0
        while scan.next() is not None:
            count += 1
        scan.close()
        
        assert count == 5
        assert scan.tuplesProduits == 5
        assert scan.total == 5
    
    def test_scan_empty_table(self):
        """Test scanning empty table"""
        table = TableMemoire(3)
        scan = FullScanTableMemoire(table)
        
        scan.open()
        result = scan.next()
        scan.close()
        
        assert result is None
        assert scan.tuplesProduits == 0
        assert scan.total == 0
    
    def test_scan_with_specific_values(self):
        """Test scanning table with specific values"""
        table = TableMemoire(2)
        t1 = Tuple(2)
        t1.val = [1, 100]
        t2 = Tuple(2)
        t2.val = [2, 200]
        table.valeurs.extend([t1, t2])
        
        scan = FullScanTableMemoire(table)
        scan.open()
        
        result1 = scan.next()
        assert result1 is not None
        assert result1.val == [1, 100]
        
        result2 = scan.next()
        assert result2 is not None
        assert result2.val == [2, 200]
        
        result3 = scan.next()
        assert result3 is None  # End of scan
        
        scan.close()
        assert scan.tuplesProduits == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])