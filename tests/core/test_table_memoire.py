"""
Unit tests for TableMemoire class.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.Tuple import Tuple


class TestTableMemoire:
    """Test TableMemoire functionality"""
    
    def test_table_creation(self):
        """Test basic table creation"""
        table = TableMemoire(3)
        assert table.nb_att == 3
        assert len(table.valeurs) == 0
    
    def test_table_add_tuples(self):
        """Test adding tuples to table"""
        table = TableMemoire(2)
        
        t1 = Tuple(2)
        t1.val = [10, 100]
        t2 = Tuple(2)
        t2.val = [20, 200]
        
        table.valeurs.extend([t1, t2])
        
        assert len(table.valeurs) == 2
        assert table.valeurs[0].val == [10, 100]
        assert table.valeurs[1].val == [20, 200]
    
    def test_table_randomize(self):
        """Test table randomization"""
        table = TableMemoire.randomize(3, 100, 5)  # 3 attributes, values 0-99, 5 tuples
        
        assert table.nb_att == 3
        assert len(table.valeurs) == 5
        
        # Check that all values are within expected range
        for tuple_obj in table.valeurs:
            assert len(tuple_obj.val) == 3
            for val in tuple_obj.val:
                assert 0 <= val < 100
    
    def test_empty_table(self):
        """Test empty table behavior"""
        table = TableMemoire(2)
        assert len(table.valeurs) == 0
        assert table.nb_att == 2
    
    def test_table_with_different_sizes(self):
        """Test tables with different attribute counts"""
        t1 = TableMemoire(1)
        t2 = TableMemoire(5)
        t3 = TableMemoire(10)
        
        assert t1.nb_att == 1
        assert t2.nb_att == 5
        assert t3.nb_att == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])